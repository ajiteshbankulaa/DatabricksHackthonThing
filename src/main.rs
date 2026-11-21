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
    let timing = env::var_os("TIMING").is_some();
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

    let link_map = links::LinkMap::build(&data_dir, ncpu)?;
    let t2 = if timing { Some(Instant::now()) } else { None };

    let scan_out = scan::scan_events_and_clicks(&data_dir, &lookups, &link_map, ncpu)?;
    let t3 = if timing { Some(Instant::now()) } else { None };

    let dedup_out = dedup::dedup_events(
        scan_out.event_shards,
        scan_out.num_threads,
        &link_map,
        ncpu,
    );
    drop(link_map);
    let t4 = if timing { Some(Instant::now()) } else { None };

    let sess_out = session::process_sessions(
        dedup_out.event_shards,
        dedup_out.num_threads,
        scan_out.click_shards,
        scan_out.num_threads,
        &lookups,
        ncpu,
    );
    let t5 = if timing { Some(Instant::now()) } else { None };

    parquet_writer::write_all_outputs(
        &out_dir,
        &sess_out.user_buffers,
        &sess_out.campaign_stats,
        &lookups,
    )?;
    let t6 = if timing { Some(Instant::now()) } else { None };

    if let (Some(t0), Some(t1), Some(t2), Some(t3), Some(t4), Some(t5), Some(t6)) =
        (t0, t1, t2, t3, t4, t5, t6)
    {
        eprintln!(
            "TIMING ms: lookup={:.2} links={:.2} scan={:.2} dedup={:.2} session={:.2} parquet={:.2} total={:.2}",
            (t1 - t0).as_secs_f64() * 1000.0,
            (t2 - t1).as_secs_f64() * 1000.0,
            (t3 - t2).as_secs_f64() * 1000.0,
            (t4 - t3).as_secs_f64() * 1000.0,
            (t5 - t4).as_secs_f64() * 1000.0,
            (t6 - t5).as_secs_f64() * 1000.0,
            (t6 - t0).as_secs_f64() * 1000.0,
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
