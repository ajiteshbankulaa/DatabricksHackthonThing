mod csv;
mod dedup;
mod id;
mod links;
mod lookup;
mod parquet_writer;
mod scan;
mod session;
mod timestamp;

use std::env;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Instant;

#[cfg(unix)]
unsafe extern "C" {
    fn _exit(status: i32) -> !;
}

fn run() -> io::Result<()> {
    let timing = env::var("TIMING").ok().as_deref() == Some("1");
    let t0 = if timing { Some(Instant::now()) } else { None };

    let args: Vec<String> = env::args().collect();
    let data_dir = PathBuf::from(if args.len() >= 3 {
        args[args.len() - 2].clone()
    } else {
        env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string())
    });
    let out_dir = PathBuf::from(if args.len() >= 3 {
        args[args.len() - 1].clone()
    } else {
        env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".to_string())
    });
    let ncpu: usize = env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
        });

    let lookups = lookup::Lookups::load(&data_dir)?;
    let t1 = if timing { Some(Instant::now()) } else { None };

    let link_map = links::LinkMap::build(&data_dir, ncpu, timing)?;
    let scan_out = scan::scan_events_and_clicks(&data_dir, &lookups, &link_map, ncpu, timing)?;
    let dedup_out = dedup::dedup_events(
        scan_out.event_shards,
        scan_out.ingest_shards,
        scan_out.num_threads,
        &link_map,
        ncpu,
        timing,
    );
    let link_stats = link_map.stats;
    drop(link_map);

    let sess_out = session::process_sessions(
        dedup_out.event_shards,
        dedup_out.num_threads,
        scan_out.click_shards,
        scan_out.num_threads,
        &lookups,
        ncpu,
        timing,
    );

    let pq_stats = parquet_writer::write_all_outputs(
        &out_dir,
        &sess_out.user_buffers,
        &sess_out.campaign_stats,
        &lookups,
        timing,
    )?;
    let t_end = if timing { Some(Instant::now()) } else { None };

    if let (Some(t0), Some(t1), Some(t_end)) = (t0, t1, t_end) {
        let lookup_ms = (t1 - t0).as_secs_f64() * 1000.0;
        let total_ms = (t_end - t0).as_secs_f64() * 1000.0;
        let ns = |v: u64| v as f64 / 1_000_000.0;
        eprintln!(
            "TIMING ms: lookup={:.2} links_scan={:.2} links_dsu={:.2} scan_events={:.2} scan_clicks={:.2} dedup_hash={:.2} dedup_scatter={:.2} session_copy={:.2} event_sort={:.2} click_sort={:.2} session_core={:.2} campaign_merge={:.2} parquet_user={:.2} parquet_campaign={:.2} total={:.2}",
            lookup_ms,
            ns(link_stats.scan_ns),
            ns(link_stats.dsu_ns),
            ns(scan_out.stats.scan_events_ns),
            ns(scan_out.stats.scan_clicks_ns),
            ns(dedup_out.stats.hash_ns),
            ns(dedup_out.stats.scatter_ns),
            ns(sess_out.stats.copy_ns),
            ns(sess_out.stats.event_sort_ns),
            ns(sess_out.stats.click_sort_ns),
            ns(sess_out.stats.core_ns),
            ns(sess_out.stats.merge_ns),
            ns(pq_stats.user_ns),
            ns(pq_stats.campaign_ns),
            total_ms,
        );
        eprintln!(
            "COUNTERS: input_bytes={} raw_events={} valid_clicks={} unique_valid_events={} intern_fallback={} link_edges={} link_nodes={} max_bucket_events={} max_user_events={} click_window_iters={} user_daily_rows={} campaign_daily_rows={}",
            scan_out.stats.input_bytes,
            scan_out.stats.raw_events,
            scan_out.stats.valid_clicks,
            dedup_out.stats.unique_valid_events,
            id::fallback_calls(),
            link_stats.edges,
            link_stats.nodes,
            sess_out.stats.max_bucket_events,
            sess_out.stats.max_user_events,
            sess_out.stats.click_window_iters,
            pq_stats.user_daily_rows,
            pq_stats.campaign_daily_rows,
        );
    }

    let _ = io::stdout().flush();
    let _ = io::stderr().flush();
    #[cfg(unix)]
    if env::var_os("PGO_TRAIN").is_none() {
        unsafe {
            _exit(0);
        }
    }
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("shopping-sessions: {err}");
        std::process::exit(1);
    }
}
