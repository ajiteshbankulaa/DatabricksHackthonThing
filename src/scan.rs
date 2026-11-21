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

#[cfg(target_os = "linux")]
struct MmapFile {
    ptr: *const u8,
    len: usize,
}

#[cfg(target_os = "linux")]
unsafe impl Send for MmapFile {}
#[cfg(target_os = "linux")]
unsafe impl Sync for MmapFile {}

#[cfg(target_os = "linux")]
impl MmapFile {
    fn from_file(file: &File, len: u64) -> io::Result<Self> {
        if len == 0 {
            return Ok(Self {
                ptr: std::ptr::null(),
                len: 0,
            });
        }
        use std::os::unix::io::AsRawFd;
        unsafe extern "C" {
            fn mmap(
                addr: *mut core::ffi::c_void,
                len: usize,
                prot: i32,
                flags: i32,
                fd: i32,
                offset: i64,
            ) -> *mut core::ffi::c_void;
            fn madvise(addr: *mut core::ffi::c_void, len: usize, advice: i32) -> i32;
        }
        const PROT_READ: i32 = 1;
        const MAP_PRIVATE: i32 = 2;
        const MADV_SEQUENTIAL: i32 = 2;
        const MAP_FAILED: *mut core::ffi::c_void = !0usize as *mut core::ffi::c_void;
        let size = len as usize;
        let ptr = unsafe {
            mmap(
                std::ptr::null_mut(),
                size,
                PROT_READ,
                MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        unsafe {
            madvise(ptr, size, MADV_SEQUENTIAL);
        }
        Ok(Self {
            ptr: ptr as *const u8,
            len: size,
        })
    }

    #[inline(always)]
    fn as_slice(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for MmapFile {
    fn drop(&mut self) {
        if !self.ptr.is_null() && self.len > 0 {
            unsafe extern "C" {
                fn munmap(addr: *mut core::ffi::c_void, len: usize) -> i32;
            }
            unsafe {
                munmap(self.ptr as *mut core::ffi::c_void, self.len);
            }
        }
    }
}

enum FileBank {
    #[cfg(target_os = "linux")]
    Mmap(Vec<MmapFile>),
    #[cfg(not(target_os = "linux"))]
    Open(Vec<File>),
    Paths(Vec<PathBuf>),
}

impl FileBank {
    fn open_for_paths(paths: Vec<PathBuf>) -> io::Result<(Self, Vec<u64>)> {
        let mut lens = Vec::with_capacity(paths.len());
        if paths.len() <= 384 {
            #[cfg(target_os = "linux")]
            {
                let mut mmaps = Vec::with_capacity(paths.len());
                for p in paths {
                    let f = File::open(&p)?;
                    let flen = f.metadata()?.len();
                    mmaps.push(MmapFile::from_file(&f, flen)?);
                    lens.push(flen);
                }
                Ok((Self::Mmap(mmaps), lens))
            }
            #[cfg(not(target_os = "linux"))]
            {
                let mut files = Vec::with_capacity(paths.len());
                for p in paths {
                    let f = File::open(&p)?;
                    lens.push(f.metadata()?.len());
                    files.push(f);
                }
                Ok((Self::Open(files), lens))
            }
        } else {
            for p in &paths {
                lens.push(std::fs::metadata(p)?.len());
            }
            Ok((Self::Paths(paths), lens))
        }
    }

    #[inline(always)]
    fn is_mmap(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            matches!(self, Self::Mmap(_))
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    #[inline(always)]
    fn mmap_slice(&self, file_idx: usize) -> Option<&[u8]> {
        #[cfg(target_os = "linux")]
        if let Self::Mmap(mmaps) = self {
            return Some(mmaps[file_idx].as_slice());
        }
        let _ = file_idx;
        None
    }

    fn read_chunk(&self, file_idx: usize, start: u64, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Mmap(mmaps) => {
                let sl = mmaps[file_idx].as_slice();
                let s = (start as usize).min(sl.len());
                let n = buf.len().min(sl.len() - s);
                buf[..n].copy_from_slice(&sl[s..s + n]);
                Ok(n)
            }
            #[cfg(not(target_os = "linux"))]
            Self::Open(files) => read_exact_at(&files[file_idx], start, buf),
            Self::Paths(paths) => {
                let f = File::open(&paths[file_idx])?;
                read_exact_at(&f, start, buf)
            }
        }
    }

    fn find_header_end(&self, file_idx: usize, file_len: u64) -> io::Result<u64> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Mmap(mmaps) => {
                let sl = mmaps[file_idx].as_slice();
                Ok(match sl.iter().position(|&b| b == b'\n') {
                    Some(i) => i as u64 + 1,
                    None => file_len,
                })
            }
            #[cfg(not(target_os = "linux"))]
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
            #[cfg(target_os = "linux")]
            Self::Mmap(mmaps) => {
                let sl = mmaps[file_idx].as_slice();
                let s = (pos as usize).min(sl.len());
                return Ok(match sl[s..].iter().position(|&b| b == b'\n') {
                    Some(i) => (s + i + 1) as u64,
                    None => file_len,
                });
            }
            #[cfg(not(target_os = "linux"))]
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

#[inline]
fn snap_mmap_range(data: &[u8], task_start: u64, task_end: u64) -> Option<(usize, usize)> {
    let file_len = data.len();
    let s = task_start as usize;
    let e = (task_end as usize).min(file_len);
    if s >= e || file_len == 0 {
        return None;
    }

    let actual_start = if s == 0 {
        let hdr_nl = data[..e].iter().position(|&b| b == b'\n')?;
        let st = hdr_nl + 1;
        if st >= e {
            return None;
        }
        st
    } else {
        let search_end = e - 1;
        let rel_nl = data[s - 1..search_end].iter().position(|&b| b == b'\n')?;
        (s - 1) + rel_nl + 1
    };

    let actual_end = if e >= file_len {
        file_len
    } else {
        match data[e - 1..].iter().position(|&b| b == b'\n') {
            Some(rel_nl) => (e - 1) + rel_nl + 1,
            None => file_len,
        }
    };

    if actual_start >= actual_end {
        None
    } else {
        Some((actual_start, actual_end))
    }
}

fn append_tasks(
    bank: &FileBank,
    lens: &[u64],
    kind: TaskKind,
    tasks: &mut Vec<ScanTask>,
) -> io::Result<()> {
    if bank.is_mmap() {
        for (file_idx, &file_len) in lens.iter().enumerate() {
            if file_len == 0 {
                continue;
            }
            let mut start = 0u64;
            while start < file_len {
                let end = start.saturating_add(SCAN_CHUNK_SIZE).min(file_len);
                tasks.push(ScanTask {
                    kind,
                    file_idx,
                    start,
                    end,
                });
                start = end;
            }
        }
        return Ok(());
    }

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
                        let bank = match task.kind {
                            TaskKind::Event => &event_bank,
                            TaskKind::Click => &click_bank,
                        };
                        let chunk_slice: &[u8] = if let Some(mm) = bank.mmap_slice(task.file_idx) {
                            let Some((st, en)) = snap_mmap_range(mm, task.start, task.end) else {
                                continue;
                            };
                            &mm[st..en]
                        } else {
                            if buf.len() < len {
                                buf.resize(len, 0);
                            }
                            let n = bank.read_chunk(task.file_idx, task.start, &mut buf[..len])?;
                            &buf[..n]
                        };
                        let n = chunk_slice.len();

                        match task.kind {
                            TaskKind::Event => {
                                scan_csv_chunk::<7, _>(chunk_slice, n, |f| {
                                    if f[0].is_empty() {
                                        return;
                                    }
                                    let Some(ingest_ts) = parse_f1_fast(f[3], &lookups.date_table) else {
                                        return;
                                    };
                                    let event_id = pack_id(f[0]);
                                    let mut valid = !f[1].is_empty()
                                        && !f[5].is_empty()
                                        && matches!(f[4], b"view" | b"cart" | b"purchase");
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
                                    if valid && f[4] == b"purchase" {
                                        if let Some(price) = parse_price_cents(f[6]) {
                                            let cat = lookups.lookup_product(f[5]);
                                            price_and_cat = (price << 8) | (cat as u32);
                                        } else {
                                            valid = false;
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
                                scan_csv_chunk::<4, _>(chunk_slice, n, |f| {
                                    if f[0].is_empty() || f[1].is_empty() || f[2].is_empty() {
                                        return;
                                    }
                                    let Some(ts) = parse_ts_fast(f[3], &lookups.date_table) else {
                                        return;
                                    };
                                    if ts < lookups.click_start || ts >= lookups.click_end {
                                        return;
                                    }
                                    let campaign_idx = lookups.lookup_campaign(f[2]);
                                    if campaign_idx == 0 {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn pre_snapped_ranges(data: &[u8], chunk_size: usize) -> Vec<(usize, usize)> {
        let Some(hdr_nl) = data.iter().position(|&b| b == b'\n') else {
            return Vec::new();
        };
        let data_start = hdr_nl + 1;
        if data_start >= data.len() {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut s = data_start;
        while s < data.len() {
            let target = (s + chunk_size).min(data.len());
            let end = if target >= data.len() {
                data.len()
            } else {
                match data[target..].iter().position(|&b| b == b'\n') {
                    Some(i) => target + i + 1,
                    None => data.len(),
                }
            };
            out.push((s, end));
            s = end;
        }
        out
    }

    #[test]
    fn test_worker_snapped_chunk_parity() {
        let cases: &[&[u8]] = &[
            b"h1,h2\nr1,a\nr2,bb\nr3,ccc\n\nr4,dddd\nr5,no_nl",
            b"only_header\n",
            b"no_newline_at_all",
            b"a,b\nshort\nvery_long_line_spanning_many_small_chunks_0123456789ABCDEF\nx,y\n",
            b"very_long_header_0123456789ABCDEF0123456789ABCDEF\nrow1,1\nrow2,2\n",
        ];

        for data in cases {
            let exp_bytes: Vec<u8> = pre_snapped_ranges(data, 7)
                .into_iter()
                .flat_map(|(s, e)| data[s..e].iter().copied())
                .collect();

            for &chunk_size in &[1usize, 2, 3, 7, 13, 64, 4096] {
                let mut got = Vec::new();
                let mut start = 0usize;
                while start < data.len() {
                    let end = (start + chunk_size).min(data.len());
                    if let Some((st, en)) = snap_mmap_range(data, start as u64, end as u64) {
                        got.extend_from_slice(&data[st..en]);
                    }
                    start = end;
                }
                assert_eq!(got, exp_bytes, "mismatch for chunk_size={chunk_size}");
            }
        }
    }
}
