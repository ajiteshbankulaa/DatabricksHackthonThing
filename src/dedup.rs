use std::sync::atomic::{AtomicUsize, Ordering};

use crate::id::{EMPTY_ID, mix64};
use crate::links::LinkMap;
use crate::scan::{EVENT_BUCKETS, INVALID_MARKER, RawEvent, USER_BUCKETS};

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
}

pub fn dedup_events(
    raw_shards: Vec<Vec<RawEvent>>,
    scan_threads: usize,
    link_map: &LinkMap,
    ncpu: usize,
) -> DedupOutput {
    if scan_threads == 0 || raw_shards.is_empty() {
        return DedupOutput {
            event_shards: Vec::new(),
            num_threads: 0,
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

    let per_thread_out: Vec<Vec<Vec<CleanEvent>>> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            handles.push(s.spawn(|| {
                let mut slots: Vec<DedupSlot> = Vec::new();
                let mut occupied: Vec<u32> = Vec::new();
                let mut user_buckets: Vec<Vec<CleanEvent>> = (0..USER_BUCKETS)
                    .map(|_| Vec::with_capacity(user_cap))
                    .collect();

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
                        }
                        unsafe {
                            slots.get_unchecked_mut(slot_idx as usize).key = EMPTY_ID;
                        }
                    }
                }

                user_buckets
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
    for t_buckets in per_thread_out {
        event_shards.extend(t_buckets);
    }

    DedupOutput {
        event_shards,
        num_threads,
    }
}
