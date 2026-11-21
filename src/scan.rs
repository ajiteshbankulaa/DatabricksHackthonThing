use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crate::csv::{find_header_end, list_csv_files, read_exact_at, scan_csv_chunk};
use crate::id::{EMPTY_ID, mix64, pack_id};
use crate::links::LinkMap;
use crate::lookup::{Lookups, NON_PURCHASE_CAT};
use crate::timestamp::{DAY_MS, parse_f1_fast, parse_price_cents, parse_ts_fast};

pub const EVENT_BUCKETS: usize = 64;
pub const USER_BUCKETS: usize = 256;
pub const INVALID_MARKER: u32 = u32::MAX;
const SCAN_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawEvent {
    pub event_id: u64,
    pub user_id: u64,
    pub rel_ts: u32,
    pub price_and_cat: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CleanClick {
    pub canonical_user: u64,
    pub click_id: u64,
    pub rel_ts: u32,
    pub campaign_idx: u16,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskKind {
    Event,
    Click,
}

struct ScanTask {
    kind: TaskKind,
    file_idx: usize,
    start: u64,
    end: u64,
}

enum FileBank {
    Open(Vec<File>),
    Paths(Vec<PathBuf>),
}

impl FileBank {
    fn open_for_paths(paths: Vec<PathBuf>) -> io::Result<(Self, Vec<u64>)> {
        let mut lens = Vec::with_capacity(paths.len());
        if paths.len() <= 384 {
            let mut files = Vec::with_capacity(paths.len());
            for p in paths {
                let f = File::open(&p)?;
                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::io::AsRawFd;
                    unsafe extern "C" {
                        fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
                    }
                    const POSIX_FADV_SEQUENTIAL: i32 = 2;
                    unsafe {
                        posix_fadvise(f.as_raw_fd(), 0, 0, POSIX_FADV_SEQUENTIAL);
                    }
                }
                lens.push(f.metadata()?.len());
                files.push(f);
            }
            Ok((Self::Open(files), lens))
        } else {
            for p in &paths {
                lens.push(std::fs::metadata(p)?.len());
            }
            Ok((Self::Paths(paths), lens))
        }
    }

    fn read_chunk(&self, file_idx: usize, start: u64, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Open(files) => read_exact_at(&files[file_idx], start, buf),
            Self::Paths(paths) => {
                let f = File::open(&paths[file_idx])?;
                read_exact_at(&f, start, buf)
            }
        }
    }

    fn find_header_end(&self, file_idx: usize, file_len: u64) -> io::Result<u64> {
        match self {
            Self::Open(files) => find_header_end(&files[file_idx], file_len),
            Self::Paths(paths) => {
                let f = File::open(&paths[file_idx])?;
                find_header_end(&f, file_len)
            }
        }
    }

    fn find_next_nl(&self, file_idx: usize, mut pos: u64, file_len: u64) -> io::Result<u64> {
        let mut tmp = [0u8; 4096];
        let opened;
        let f_ref = match self {
            Self::Open(files) => &files[file_idx],
            Self::Paths(paths) => {
                opened = File::open(&paths[file_idx])?;
                &opened
            }
        };
        while pos < file_len {
            let to_read = (file_len - pos).min(tmp.len() as u64) as usize;
            let n = read_exact_at(f_ref, pos, &mut tmp[..to_read])?;
            if n == 0 {
                break;
            }
            if let Some(idx) = tmp[..n].iter().position(|&b| b == b'\n') {
                return Ok(pos + idx as u64 + 1);
            }
            pos += n as u64;
        }
        Ok(file_len)
    }
}

fn append_tasks(
    bank: &FileBank,
    lens: &[u64],
    kind: TaskKind,
    tasks: &mut Vec<ScanTask>,
) -> io::Result<()> {
    for (file_idx, &file_len) in lens.iter().enumerate() {
        let data_start = bank.find_header_end(file_idx, file_len)?;
        if data_start >= file_len {
            continue;
        }
        let remaining = file_len - data_start;
        if remaining <= SCAN_CHUNK_SIZE {
            tasks.push(ScanTask {
                kind,
                file_idx,
                start: data_start,
                end: file_len,
            });
            continue;
        }
        let mut start = data_start;
        while start < file_len {
            let target = start.saturating_add(SCAN_CHUNK_SIZE);
            if target >= file_len {
                tasks.push(ScanTask {
                    kind,
                    file_idx,
                    start,
                    end: file_len,
                });
                break;
            }
            let end = bank.find_next_nl(file_idx, target, file_len)?;
            tasks.push(ScanTask {
                kind,
                file_idx,
                start,
                end,
            });
            start = end;
        }
    }
    Ok(())
}

#[derive(Default, Clone, Copy)]
pub struct ScanStats {
    pub scan_events_ns: u64,
    pub scan_clicks_ns: u64,
    pub input_bytes: u64,
    pub raw_events: usize,
    pub valid_clicks: usize,
}

pub struct ScanOutput {
    /// Flat array of `num_threads * EVENT_BUCKETS` vectors.
    pub event_shards: Vec<Vec<RawEvent>>,
    /// Parallel `ingest_sec` (seconds relative to `window_start`) for `event_shards`.
    pub ingest_shards: Vec<Vec<i32>>,
    /// Flat array of `num_threads * USER_BUCKETS` vectors.
    pub click_shards: Vec<Vec<CleanClick>>,
    pub num_threads: usize,
    pub stats: ScanStats,
}

pub fn scan_events_and_clicks(
    data_dir: &Path,
    lookups: &Lookups,
    link_map: &LinkMap,
    ncpu: usize,
    timing: bool,
) -> io::Result<ScanOutput> {
    let wall_t0 = if timing { Some(Instant::now()) } else { None };
    let event_paths = list_csv_files(&data_dir.join("events"))?;
    let click_paths = list_csv_files(&data_dir.join("clicks"))?;

    let (event_bank, event_lens) = FileBank::open_for_paths(event_paths)?;
    let (click_bank, click_lens) = FileBank::open_for_paths(click_paths)?;

    let mut tasks = Vec::new();
    append_tasks(&event_bank, &event_lens, TaskKind::Event, &mut tasks)?;
    append_tasks(&click_bank, &click_lens, TaskKind::Click, &mut tasks)?;

    let input_bytes = event_lens.iter().sum::<u64>() + click_lens.iter().sum::<u64>();
    if tasks.is_empty() {
        return Ok(ScanOutput {
            event_shards: Vec::new(),
            ingest_shards: Vec::new(),
            click_shards: Vec::new(),
            num_threads: 0,
            stats: ScanStats {
                input_bytes,
                ..ScanStats::default()
            },
        });
    }

    tasks.sort_unstable_by(|a, b| (b.end - b.start).cmp(&(a.end - a.start)));
    let num_threads = ncpu.min(tasks.len()).max(1);
    let next_task = AtomicUsize::new(0);
    let window_start_sec = lookups.window_start.div_euclid(1000);

    let total_event_bytes: u64 = event_lens.iter().sum();
    let total_click_bytes: u64 = click_lens.iter().sum();
    let ev_cap = if total_event_bytes > 512 * 1024 {
        (((total_event_bytes / 75) / (num_threads * EVENT_BUCKETS) as u64).max(16) as usize) * 11 / 10
    } else {
        0
    };
    let ck_cap = if total_click_bytes > 512 * 1024 {
        (((total_click_bytes / 55) / (num_threads * USER_BUCKETS) as u64).max(16) as usize) * 11 / 10
    } else {
        0
    };

    let thread_results: Vec<(
        Vec<Vec<RawEvent>>,
        Vec<Vec<i32>>,
        Vec<Vec<CleanClick>>,
        u64,
        u64,
    )> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            handles.push(s.spawn(
                || -> io::Result<(
                    Vec<Vec<RawEvent>>,
                    Vec<Vec<i32>>,
                    Vec<Vec<CleanClick>>,
                    u64,
                    u64,
                )> {
                    let mut buf: Vec<u8> = Vec::new();
                    let mut ev_buckets: Vec<Vec<RawEvent>> = (0..EVENT_BUCKETS)
                        .map(|_| Vec::with_capacity(ev_cap))
                        .collect();
                    let mut ing_buckets: Vec<Vec<i32>> = (0..EVENT_BUCKETS)
                        .map(|_| Vec::with_capacity(ev_cap))
                        .collect();
                    let mut ck_buckets: Vec<Vec<CleanClick>> = (0..USER_BUCKETS)
                        .map(|_| Vec::with_capacity(ck_cap))
                        .collect();
                    let mut ev_cpu_ns = 0u64;
                    let mut ck_cpu_ns = 0u64;

                    loop {
                        let tidx = next_task.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(tidx) else {
                            break;
                        };
                        let len = (task.end - task.start) as usize;
                        if len == 0 {
                            continue;
                        }
                        let c_t0 = if timing { Some(Instant::now()) } else { None };
                        if buf.len() < len + 64 {
                            buf.resize(len + 64, 0);
                        }
                        let bank = match task.kind {
                            TaskKind::Event => &event_bank,
                            TaskKind::Click => &click_bank,
                        };
                        let n = bank.read_chunk(task.file_idx, task.start, &mut buf[..len])?;
                        buf[n..n + 64].fill(0);

                        match task.kind {
                            TaskKind::Event => {
                                scan_csv_chunk::<7, _>(&buf, n, |f| {
                                    if f[0].is_empty() {
                                        return;
                                    }
                                    let Some(ingest_ts) = parse_f1_fast(f[3], &lookups.date_table) else {
                                        return;
                                    };
                                    let event_id = pack_id(f[0]);
                                    let mut valid = !f[1].is_empty();
                                    let mut rel_ts = 0u32;
                                    if valid {
                                        if let Some(ts) = parse_ts_fast(f[2], &lookups.date_table) {
                                            if ts >= lookups.window_start
                                                && ts < lookups.window_end
                                                && ingest_ts - ts <= DAY_MS
                                            {
                                                rel_ts = (ts - lookups.window_start) as u32;
                                            } else {
                                                valid = false;
                                            }
                                        } else {
                                            valid = false;
                                        }
                                    }
                                    let mut price_and_cat = NON_PURCHASE_CAT as u32;
                                    if valid {
                                        if f[5].is_empty() {
                                            valid = false;
                                        } else {
                                            match f[4] {
                                                b"view" | b"cart" => {}
                                                b"purchase" => {
                                                    if let Some(price) = parse_price_cents(f[6]) {
                                                        let cat = lookups.lookup_product(f[5]);
                                                        price_and_cat =
                                                            (price << 8) | (cat as u32);
                                                    } else {
                                                        valid = false;
                                                    }
                                                }
                                                _ => valid = false,
                                            }
                                        }
                                    }
                                    let ingest_sec = (ingest_ts.div_euclid(1000)
                                        - window_start_sec)
                                        .clamp(i32::MIN as i64, i32::MAX as i64)
                                        as i32;
                                    let raw = RawEvent {
                                        event_id,
                                        user_id: if valid {
                                            pack_id(f[1])
                                        } else {
                                            EMPTY_ID
                                        },
                                        rel_ts,
                                        price_and_cat: if valid {
                                            price_and_cat
                                        } else {
                                            INVALID_MARKER
                                        },
                                    };
                                    let b = (mix64(event_id) as usize) & (EVENT_BUCKETS - 1);
                                    unsafe {
                                        ev_buckets.get_unchecked_mut(b).push(raw);
                                        ing_buckets.get_unchecked_mut(b).push(ingest_sec);
                                    }
                                });
                                if let Some(t) = c_t0 {
                                    ev_cpu_ns += t.elapsed().as_nanos() as u64;
                                }
                            }
                            TaskKind::Click => {
                                scan_csv_chunk::<4, _>(&buf, n, |f| {
                                    if f[0].is_empty() || f[1].is_empty() {
                                        return;
                                    }
                                    let campaign_idx = lookups.lookup_campaign(f[2]);
                                    if campaign_idx == 0 {
                                        return;
                                    }
                                    let Some(ts) = parse_ts_fast(f[3], &lookups.date_table) else {
                                        return;
                                    };
                                    if ts < lookups.click_start || ts >= lookups.click_end {
                                        return;
                                    }
                                    let click_id = pack_id(f[0]);
                                    let canonical_user = link_map.resolve(pack_id(f[1]));
                                    let rel_ts = (ts - lookups.click_start) as u32;
                                    let b =
                                        (mix64(canonical_user) as usize) & (USER_BUCKETS - 1);
                                    unsafe {
                                        ck_buckets.get_unchecked_mut(b).push(CleanClick {
                                            canonical_user,
                                            click_id,
                                            rel_ts,
                                            campaign_idx,
                                        });
                                    }
                                });
                                if let Some(t) = c_t0 {
                                    ck_cpu_ns += t.elapsed().as_nanos() as u64;
                                }
                            }
                        }
                    }

                    Ok((ev_buckets, ing_buckets, ck_buckets, ev_cpu_ns, ck_cpu_ns))
                },
            ));
        }

        let mut out = Vec::with_capacity(num_threads);
        for h in handles {
            out.push(h.join().unwrap()?);
        }
        Ok::<_, io::Error>(out)
    })?;

    let mut event_shards = Vec::with_capacity(num_threads * EVENT_BUCKETS);
    let mut ingest_shards = Vec::with_capacity(num_threads * EVENT_BUCKETS);
    let mut click_shards = Vec::with_capacity(num_threads * USER_BUCKETS);
    let mut ev_cpu_sum = 0u64;
    let mut ck_cpu_sum = 0u64;
    let mut raw_events = 0usize;
    let mut valid_clicks = 0usize;
    for (evs, ings, cks, ev_ns, ck_ns) in thread_results {
        ev_cpu_sum += ev_ns;
        ck_cpu_sum += ck_ns;
        if timing {
            raw_events += evs.iter().map(|v| v.len()).sum::<usize>();
            valid_clicks += cks.iter().map(|v| v.len()).sum::<usize>();
        }
        event_shards.extend(evs);
        ingest_shards.extend(ings);
        click_shards.extend(cks);
    }

    let wall_ns = wall_t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
    let cpu_total = (ev_cpu_sum + ck_cpu_sum).max(1);
    let scan_events_ns = (wall_ns as u128 * ev_cpu_sum as u128 / cpu_total as u128) as u64;
    let scan_clicks_ns = wall_ns.saturating_sub(scan_events_ns);

    Ok(ScanOutput {
        event_shards,
        ingest_shards,
        click_shards,
        num_threads,
        stats: ScanStats {
            scan_events_ns,
            scan_clicks_ns,
            input_bytes,
            raw_events,
            valid_clicks,
        },
    })
}
