use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crate::id::{EMPTY_ID, mix64};
use crate::links::LinkMap;
use crate::scan::{EVENT_BUCKETS, INVALID_MARKER, RawEvent, USER_BUCKETS};

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
    shard: u32,
    row: u32,
}

const EMPTY_SLOT: DedupSlot = DedupSlot {
    key: EMPTY_ID,
    shard: 0,
    row: 0,
};

pub struct DedupOutput {
    /// Flat array of `num_threads * USER_BUCKETS` vectors of `CleanEvent`.
    pub event_shards: Vec<Vec<CleanEvent>>,
    pub num_threads: usize,
    pub stats: DedupStats,
}

pub fn dedup_events(
    raw_shards: Vec<Vec<RawEvent>>,
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

                    let h_t0 = if timing { Some(Instant::now()) } else { None };
                    let needed = (total_rows * 2).next_power_of_two().max(64);
                    if slots.len() < needed {
                        slots.resize(needed, EMPTY_SLOT);
                    }
                    let mask = needed - 1;
                    occupied.clear();

                    for t in 0..scan_threads {
                        let slice = &raw_shards[t * EVENT_BUCKETS + b];
                        for (r_idx, ev) in slice.iter().enumerate() {
                            let mut idx = (mix64(ev.event_id) as usize) & mask;
                            loop {
                                let slot = unsafe { *slots.get_unchecked(idx) };
                                if slot.key == EMPTY_ID {
                                    unsafe {
                                        *slots.get_unchecked_mut(idx) = DedupSlot {
                                            key: ev.event_id,
                                            shard: t as u32,
                                            row: r_idx as u32,
                                        };
                                    }
                                    occupied.push(idx as u32);
                                    break;
                                }
                                if slot.key == ev.event_id {
                                    let prev = unsafe {
                                        raw_shards
                                            .get_unchecked(slot.shard as usize * EVENT_BUCKETS + b)
                                            .get_unchecked(slot.row as usize)
                                    };
                                    if ev.ingest_ts < prev.ingest_ts {
                                        unsafe {
                                            let s_mut = slots.get_unchecked_mut(idx);
                                            s_mut.shard = t as u32;
                                            s_mut.row = r_idx as u32;
                                        }
                                    }
                                    break;
                                }
                                idx = (idx + 1) & mask;
                            }
                        }
                    }
                    if let Some(t) = h_t0 {
                        hash_cpu_ns += t.elapsed().as_nanos() as u64;
                    }

                    let s_t0 = if timing { Some(Instant::now()) } else { None };
                    for &slot_idx in &occupied {
                        let slot = unsafe { *slots.get_unchecked(slot_idx as usize) };
                        let ev = unsafe {
                            raw_shards
                                .get_unchecked(slot.shard as usize * EVENT_BUCKETS + b)
                                .get_unchecked(slot.row as usize)
                        };
                        if ev.price_and_cat != INVALID_MARKER {
                            let canonical_user = link_map.resolve(ev.user_id);
                            let ub = (mix64(canonical_user) as usize) & (USER_BUCKETS - 1);
                            unsafe {
                                user_buckets.get_unchecked_mut(ub).push(CleanEvent {
                                    canonical_user,
                                    rel_ts: ev.rel_ts,
                                    price_and_cat: ev.price_and_cat,
                                });
                            }
                            if timing {
                                unique_valid += 1;
                            }
                        }
                        unsafe {
                            slots.get_unchecked_mut(slot_idx as usize).key = EMPTY_ID;
                        }
                    }
                    if let Some(t) = s_t0 {
                        scatter_cpu_ns += t.elapsed().as_nanos() as u64;
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
