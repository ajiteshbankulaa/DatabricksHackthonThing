use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crate::id::{EMPTY_ID, mix64};
use crate::links::LinkMap;
use crate::scan::{EVENT_BUCKETS, INVALID_MARKER, RawEvent, USER_BUCKETS};

const SUB_PARTITIONS: usize = 16;
const PREFETCH_DIST: usize = 16;

#[derive(Default, Clone, Copy)]
pub struct DedupStats {
    pub hash_ns: u64,
    pub scatter_ns: u64,
    pub unique_valid_events: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CleanEvent {
    pub canonical_user: u64,
    pub rel_ts: u32,
    pub price_and_cat: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DedupSlot {
    key: u64,
    min_ingest: i32,
    row_idx: u32,
}

const EMPTY_SLOT: DedupSlot = DedupSlot {
    key: EMPTY_ID,
    min_ingest: 0,
    row_idx: 0,
};

pub struct DedupOutput {
    /// Flat array of `num_threads * USER_BUCKETS` vectors of `CleanEvent`.
    pub event_shards: Vec<Vec<CleanEvent>>,
    pub num_threads: usize,
    pub stats: DedupStats,
}

#[inline(always)]
fn prefetch_slot(slots: &[DedupSlot], idx: usize) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        let ptr = slots.as_ptr().add(idx) as *const i8;
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(ptr);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = (slots, idx);
}

fn dedup_slice_into_buckets(
    raw: &[RawEvent],
    ing: &[i32],
    slots: &mut Vec<DedupSlot>,
    occupied: &mut Vec<u32>,
    link_map: &LinkMap,
    user_buckets: &mut [Vec<CleanEvent>],
    pending: &mut Vec<RawEvent>,
    timing: bool,
    hash_cpu_ns: &mut u64,
    scatter_cpu_ns: &mut u64,
    unique_valid: &mut usize,
) {
    let n = raw.len();
    if n == 0 {
        return;
    }

    let h_t0 = if timing { Some(Instant::now()) } else { None };
    let needed = (n * 2).next_power_of_two().max(64);
    if slots.len() < needed {
        slots.resize(needed, EMPTY_SLOT);
    }
    let mask = needed - 1;
    occupied.clear();

    for i in 0..n {
        if i + PREFETCH_DIST < n {
            let p_id = unsafe { raw.get_unchecked(i + PREFETCH_DIST).event_id };
            let p_idx = ((mix64(p_id) >> 16) as usize) & mask;
            prefetch_slot(slots, p_idx);
        }
        let ev = unsafe { *raw.get_unchecked(i) };
        let ev_ing = unsafe { *ing.get_unchecked(i) };
        let mut idx = ((mix64(ev.event_id) >> 16) as usize) & mask;
        loop {
            let slot = unsafe { *slots.get_unchecked(idx) };
            if slot.key == EMPTY_ID {
                unsafe {
                    *slots.get_unchecked_mut(idx) = DedupSlot {
                        key: ev.event_id,
                        min_ingest: ev_ing,
                        row_idx: i as u32,
                    };
                }
                occupied.push(idx as u32);
                break;
            }
            if slot.key == ev.event_id {
                if ev_ing < slot.min_ingest {
                    unsafe {
                        let s_mut = slots.get_unchecked_mut(idx);
                        s_mut.min_ingest = ev_ing;
                        s_mut.row_idx = i as u32;
                    }
                }
                break;
            }
            idx = (idx + 1) & mask;
        }
    }
    if let Some(t) = h_t0 {
        *hash_cpu_ns += t.elapsed().as_nanos() as u64;
    }

    let s_t0 = if timing { Some(Instant::now()) } else { None };
    pending.clear();
    for &slot_idx in occupied.iter() {
        let slot = unsafe { *slots.get_unchecked(slot_idx as usize) };
        let ev = unsafe { *raw.get_unchecked(slot.row_idx as usize) };
        unsafe {
            slots.get_unchecked_mut(slot_idx as usize).key = EMPTY_ID;
        }
        if ev.price_and_cat != INVALID_MARKER {
            pending.push(ev);
        }
    }

    let plen = pending.len();
    for j in 0..plen {
        if j + PREFETCH_DIST < plen {
            link_map.prefetch(unsafe { pending.get_unchecked(j + PREFETCH_DIST).user_id });
        }
        let ev = unsafe { *pending.get_unchecked(j) };
        let canonical_user = link_map.resolve(ev.user_id);
        let ub = (mix64(canonical_user) as usize) & (USER_BUCKETS - 1);
        unsafe {
            user_buckets.get_unchecked_mut(ub).push(CleanEvent {
                canonical_user,
                rel_ts: ev.rel_ts,
                price_and_cat: ev.price_and_cat,
            });
        }
    }
    if timing {
        *unique_valid += plen;
        if let Some(t) = s_t0 {
            *scatter_cpu_ns += t.elapsed().as_nanos() as u64;
        }
    }
}

pub fn dedup_events(
    raw_shards: Vec<Vec<RawEvent>>,
    ingest_shards: Vec<Vec<i32>>,
    scan_threads: usize,
    link_map: &LinkMap,
    ncpu: usize,
    timing: bool,
) -> DedupOutput {
    let wall_t0 = if timing { Some(Instant::now()) } else { None };
    if scan_threads == 0 || raw_shards.is_empty() {
        return DedupOutput {
            event_shards: Vec::new(),
            num_threads: 0,
            stats: DedupStats::default(),
        };
    }

    let num_threads = ncpu.min(EVENT_BUCKETS).max(1);
    let next_bucket = AtomicUsize::new(0);
    let total_raw_events: usize = raw_shards.iter().map(|v| v.len()).sum();
    let user_cap = if total_raw_events > 10_000 {
        (total_raw_events / (num_threads * USER_BUCKETS)).max(16) * 11 / 10
    } else {
        0
    };

    let per_thread_out: Vec<(Vec<Vec<CleanEvent>>, u64, u64, usize)> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            handles.push(s.spawn(|| {
                let mut slots: Vec<DedupSlot> = Vec::new();
                let mut occupied: Vec<u32> = Vec::new();
                let mut pending: Vec<RawEvent> = Vec::new();
                let mut flat_raw: Vec<RawEvent> = Vec::new();
                let mut flat_ing: Vec<i32> = Vec::new();
                let mut sub_raw: Vec<RawEvent> = Vec::new();
                let mut sub_ing: Vec<i32> = Vec::new();
                let mut user_buckets: Vec<Vec<CleanEvent>> = (0..USER_BUCKETS)
                    .map(|_| Vec::with_capacity(user_cap))
                    .collect();
                let mut hash_cpu_ns = 0u64;
                let mut scatter_cpu_ns = 0u64;
                let mut unique_valid = 0usize;

                loop {
                    let b = next_bucket.fetch_add(1, Ordering::Relaxed);
                    if b >= EVENT_BUCKETS {
                        break;
                    }

                    let mut total_rows = 0usize;
                    for t in 0..scan_threads {
                        total_rows += raw_shards[t * EVENT_BUCKETS + b].len();
                    }
                    if total_rows == 0 {
                        continue;
                    }

                    if total_rows <= 4096 {
                        flat_raw.clear();
                        flat_ing.clear();
                        flat_raw.reserve(total_rows);
                        flat_ing.reserve(total_rows);
                        for t in 0..scan_threads {
                            let idx = t * EVENT_BUCKETS + b;
                            flat_raw.extend_from_slice(&raw_shards[idx]);
                            flat_ing.extend_from_slice(&ingest_shards[idx]);
                        }
                        dedup_slice_into_buckets(
                            &flat_raw,
                            &flat_ing,
                            &mut slots,
                            &mut occupied,
                            link_map,
                            &mut user_buckets,
                            &mut pending,
                            timing,
                            &mut hash_cpu_ns,
                            &mut scatter_cpu_ns,
                            &mut unique_valid,
                        );
                    } else {
                        let h_t0 = if timing { Some(Instant::now()) } else { None };
                        let mut counts = [0usize; SUB_PARTITIONS];
                        for t in 0..scan_threads {
                            for ev in &raw_shards[t * EVENT_BUCKETS + b] {
                                let sp = ((mix64(ev.event_id) >> 8) as usize) & (SUB_PARTITIONS - 1);
                                counts[sp] += 1;
                            }
                        }
                        let mut offsets = [0usize; SUB_PARTITIONS + 1];
                        let mut sum = 0usize;
                        for i in 0..SUB_PARTITIONS {
                            offsets[i] = sum;
                            sum += counts[i];
                        }
                        offsets[SUB_PARTITIONS] = sum;

                        sub_raw.clear();
                        sub_raw.reserve(total_rows);
                        sub_ing.clear();
                        sub_ing.reserve(total_rows);
                        unsafe {
                            sub_raw.set_len(total_rows);
                            sub_ing.set_len(total_rows);
                        }
                        let mut cursors = [0usize; SUB_PARTITIONS];
                        cursors.copy_from_slice(&offsets[..SUB_PARTITIONS]);

                        for t in 0..scan_threads {
                            let idx = t * EVENT_BUCKETS + b;
                            let r_slice = &raw_shards[idx];
                            let i_slice = &ingest_shards[idx];
                            for j in 0..r_slice.len() {
                                let ev = unsafe { *r_slice.get_unchecked(j) };
                                let ing = unsafe { *i_slice.get_unchecked(j) };
                                let sp = ((mix64(ev.event_id) >> 8) as usize) & (SUB_PARTITIONS - 1);
                                let pos = cursors[sp];
                                cursors[sp] = pos + 1;
                                unsafe {
                                    *sub_raw.get_unchecked_mut(pos) = ev;
                                    *sub_ing.get_unchecked_mut(pos) = ing;
                                }
                            }
                        }
                        if let Some(t) = h_t0 {
                            hash_cpu_ns += t.elapsed().as_nanos() as u64;
                        }

                        for sp in 0..SUB_PARTITIONS {
                            let st = offsets[sp];
                            let en = offsets[sp + 1];
                            if st < en {
                                dedup_slice_into_buckets(
                                    &sub_raw[st..en],
                                    &sub_ing[st..en],
                                    &mut slots,
                                    &mut occupied,
                                    link_map,
                                    &mut user_buckets,
                                    &mut pending,
                                    timing,
                                    &mut hash_cpu_ns,
                                    &mut scatter_cpu_ns,
                                    &mut unique_valid,
                                );
                            }
                        }
                    }
                }

                (user_buckets, hash_cpu_ns, scatter_cpu_ns, unique_valid)
            }));
        }

        let mut out = Vec::with_capacity(num_threads);
        for h in handles {
            out.push(h.join().unwrap());
        }
        out
    });

    drop(raw_shards);
    drop(ingest_shards);

    let mut event_shards = Vec::with_capacity(num_threads * USER_BUCKETS);
    let mut h_sum = 0u64;
    let mut s_sum = 0u64;
    let mut unique_valid_events = 0usize;
    for (t_buckets, h_ns, s_ns, uv) in per_thread_out {
        h_sum += h_ns;
        s_sum += s_ns;
        unique_valid_events += uv;
        event_shards.extend(t_buckets);
    }

    let wall_ns = wall_t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
    let total_cpu = (h_sum + s_sum).max(1);
    let hash_ns = (wall_ns as u128 * h_sum as u128 / total_cpu as u128) as u64;
    let scatter_ns = wall_ns.saturating_sub(hash_ns);

    DedupOutput {
        event_shards,
        num_threads,
        stats: DedupStats {
            hash_ns,
            scatter_ns,
            unique_valid_events,
        },
    }
}
