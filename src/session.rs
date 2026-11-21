use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crate::dedup::CleanEvent;
use crate::id::cmp_id;
use crate::lookup::{Lookups, NON_PURCHASE_CAT};
use crate::scan::{CleanClick, USER_BUCKETS};
use crate::timestamp::{DAY_MS, SEVEN_DAYS_MS};

const SESSION_GAP_MS: u32 = 1_800_000;

#[derive(Default, Clone, Copy)]
pub struct SessionStats {
    pub copy_ns: u64,
    pub event_sort_ns: u64,
    pub click_sort_ns: u64,
    pub core_ns: u64,
    pub merge_ns: u64,
    pub max_bucket_events: usize,
    pub max_user_events: usize,
    pub click_window_iters: u64,
}

#[derive(Clone, Copy, Default)]
pub struct CampaignDayStat {
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue_cents: i64,
    pub assisted_sessions: i64,
}

#[derive(Default)]
pub struct UserDailyBuffer {
    pub user_ids: Vec<u64>,
    pub days: Vec<i32>,
    pub sessions: Vec<i64>,
    pub events: Vec<i64>,
    pub revenue_cents: Vec<i64>,
    /// Index into `Lookups::categories`, or `NON_PURCHASE_CAT` (255) for SQL NULL.
    pub top_category: Vec<u8>,
}

impl UserDailyBuffer {
    #[inline(always)]
    pub fn push(
        &mut self,
        user_id: u64,
        day: i32,
        sessions: i64,
        events: i64,
        revenue_cents: i64,
        top_category: u8,
    ) {
        self.user_ids.push(user_id);
        self.days.push(day);
        self.sessions.push(sessions);
        self.events.push(events);
        self.revenue_cents.push(revenue_cents);
        self.top_category.push(top_category);
    }

    pub fn len(&self) -> usize {
        self.user_ids.len()
    }
}

pub struct SessionOutput {
    pub user_buffers: Vec<UserDailyBuffer>,
    pub campaign_stats: Vec<CampaignDayStat>,
    pub stats: SessionStats,
}

pub fn process_sessions(
    mut event_shards: Vec<Vec<CleanEvent>>,
    dedup_threads: usize,
    mut click_shards: Vec<Vec<CleanClick>>,
    scan_threads: usize,
    lookups: &Lookups,
    ncpu: usize,
    timing: bool,
) -> SessionOutput {
    let wall_t0 = if timing { Some(Instant::now()) } else { None };
    let num_campaigns = lookups.campaigns.len();
    let num_days = lookups.num_days;

    if dedup_threads == 0 || event_shards.is_empty() {
        return SessionOutput {
            user_buffers: Vec::new(),
            campaign_stats: vec![CampaignDayStat::default(); num_campaigns * num_days],
            stats: SessionStats::default(),
        };
    }

    // Order user buckets largest-first so whale/kiosk buckets start immediately.
    let mut bucket_order: Vec<(usize, usize)> = (0..USER_BUCKETS)
        .map(|b| {
            let sz: usize = (0..dedup_threads)
                .map(|t| event_shards[t * USER_BUCKETS + b].len())
                .sum();
            (b, sz)
        })
        .filter(|&(_, sz)| sz > 0)
        .collect();
    bucket_order.sort_unstable_by(|a, b| b.1.cmp(&a.1));

    if bucket_order.is_empty() {
        return SessionOutput {
            user_buffers: Vec::new(),
            campaign_stats: vec![CampaignDayStat::default(); num_campaigns * num_days],
            stats: SessionStats::default(),
        };
    }

    let max_bucket_events = bucket_order[0].1;
    let num_threads = ncpu.min(bucket_order.len()).max(1);
    let next_idx = AtomicUsize::new(0);
    let num_cats = lookups.categories.len();
    let window_start = lookups.window_start;
    let start_epoch_day = lookups.start_epoch_day;

    // Move each bucket's shards into a Vec per bucket first (zero copy of elements!).
    let mut bucket_events_shards: Vec<Vec<Vec<CleanEvent>>> =
        (0..USER_BUCKETS).map(|_| Vec::new()).collect();
    for t in 0..dedup_threads {
        for b in 0..USER_BUCKETS {
            let v = std::mem::take(&mut event_shards[t * USER_BUCKETS + b]);
            if !v.is_empty() {
                bucket_events_shards[b].push(v);
            }
        }
    }
    drop(event_shards);

    let mut bucket_clicks_shards: Vec<Vec<Vec<CleanClick>>> =
        (0..USER_BUCKETS).map(|_| Vec::new()).collect();
    for t in 0..scan_threads {
        for b in 0..USER_BUCKETS {
            let v = std::mem::take(&mut click_shards[t * USER_BUCKETS + b]);
            if !v.is_empty() {
                bucket_clicks_shards[b].push(v);
            }
        }
    }
    drop(click_shards);

    struct WorkerRes {
        user_buf: UserDailyBuffer,
        camp_stats: Vec<CampaignDayStat>,
        copy_ns: u64,
        ev_sort_ns: u64,
        ck_sort_ns: u64,
        core_ns: u64,
        max_user_events: usize,
        click_window_iters: u64,
    }

    let thread_outputs: Vec<WorkerRes> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            handles.push(s.spawn(|| {
                let mut user_buf = UserDailyBuffer::default();
                let mut camp_stats = vec![CampaignDayStat::default(); num_campaigns * num_days];
                let mut events: Vec<CleanEvent> = Vec::new();
                let mut clicks: Vec<CleanClick> = Vec::new();
                let mut cat_rev: Vec<i64> = vec![0i64; num_cats];
                let mut last_seen_gen: Vec<u32> = vec![0u32; num_campaigns];
                let mut session_gen: u32 = 0;

                let mut copy_ns = 0u64;
                let mut ev_sort_ns = 0u64;
                let mut ck_sort_ns = 0u64;
                let mut core_ns = 0u64;
                let mut max_user_events = 0usize;
                let mut click_window_iters = 0u64;

                loop {
                    let order_pos = next_idx.fetch_add(1, Ordering::Relaxed);
                    let Some(&(b, total_ev)) = bucket_order.get(order_pos) else {
                        break;
                    };

                    let t_cp = if timing { Some(Instant::now()) } else { None };
                    events.clear();
                    events.reserve(total_ev);
                    for slice in &bucket_events_shards[b] {
                        events.extend_from_slice(slice);
                    }

                    let total_ck: usize = bucket_clicks_shards[b].iter().map(|v| v.len()).sum();
                    clicks.clear();
                    clicks.reserve(total_ck);
                    for slice in &bucket_clicks_shards[b] {
                        clicks.extend_from_slice(slice);
                    }
                    if let Some(t) = t_cp {
                        copy_ns += t.elapsed().as_nanos() as u64;
                    }

                    let t_es = if timing { Some(Instant::now()) } else { None };
                    events.sort_unstable_by_key(|a| {
                        ((a.canonical_user as u128) << 32) | (a.rel_ts as u128)
                    });
                    if let Some(t) = t_es {
                        ev_sort_ns += t.elapsed().as_nanos() as u64;
                    }

                    let t_cs = if timing { Some(Instant::now()) } else { None };
                    if !clicks.is_empty() {
                        clicks.sort_unstable_by(|a, b| {
                            a.canonical_user
                                .cmp(&b.canonical_user)
                                .then_with(|| a.rel_ts.cmp(&b.rel_ts))
                                .then_with(|| cmp_id(b.click_id, a.click_id))
                        });
                        clicks.dedup_by(|a, b| {
                            a.canonical_user == b.canonical_user
                                && a.rel_ts == b.rel_ts
                                && a.click_id == b.click_id
                        });
                    }
                    if let Some(t) = t_cs {
                        ck_sort_ns += t.elapsed().as_nanos() as u64;
                    }

                    let t_co = if timing { Some(Instant::now()) } else { None };
                    let mut ev_idx = 0usize;
                    let mut ck_idx = 0usize;
                    let ev_len = events.len();
                    let ck_len = clicks.len();

                    while ev_idx < ev_len {
                        let user_id = events[ev_idx].canonical_user;
                        let ev_start = ev_idx;
                        while ev_idx < ev_len && events[ev_idx].canonical_user == user_id {
                            ev_idx += 1;
                        }
                        let u_events = &events[ev_start..ev_idx];
                        if timing && u_events.len() > max_user_events {
                            max_user_events = u_events.len();
                        }

                        while ck_idx < ck_len && clicks[ck_idx].canonical_user < user_id {
                            ck_idx += 1;
                        }
                        let ck_start = ck_idx;
                        while ck_idx < ck_len && clicks[ck_idx].canonical_user == user_id {
                            ck_idx += 1;
                        }
                        let u_clicks = &clicks[ck_start..ck_idx];

                        let iters = process_user(
                            user_id,
                            u_events,
                            u_clicks,
                            window_start,
                            start_epoch_day,
                            num_days,
                            &mut cat_rev,
                            &mut last_seen_gen,
                            &mut session_gen,
                            &mut user_buf,
                            &mut camp_stats,
                        );
                        if timing {
                            click_window_iters += iters;
                        }
                    }
                    if let Some(t) = t_co {
                        core_ns += t.elapsed().as_nanos() as u64;
                    }
                }

                WorkerRes {
                    user_buf,
                    camp_stats,
                    copy_ns,
                    ev_sort_ns,
                    ck_sort_ns,
                    core_ns,
                    max_user_events,
                    click_window_iters,
                }
            }));
        }

        let mut out = Vec::with_capacity(num_threads);
        for h in handles {
            out.push(h.join().unwrap());
        }
        out
    });

    let t_merge = if timing { Some(Instant::now()) } else { None };
    let mut user_buffers = Vec::with_capacity(num_threads);
    let mut merged_camp = vec![CampaignDayStat::default(); num_campaigns * num_days];
    let mut cp_sum = 0u64;
    let mut es_sum = 0u64;
    let mut cs_sum = 0u64;
    let mut co_sum = 0u64;
    let mut max_user_events = 0usize;
    let mut click_window_iters = 0u64;

    for r in thread_outputs {
        if r.user_buf.len() > 0 {
            user_buffers.push(r.user_buf);
        }
        for (dst, src) in merged_camp.iter_mut().zip(r.camp_stats.iter()) {
            dst.sessions += src.sessions;
            dst.purchasing_sessions += src.purchasing_sessions;
            dst.revenue_cents += src.revenue_cents;
            dst.assisted_sessions += src.assisted_sessions;
        }
        cp_sum += r.copy_ns;
        es_sum += r.ev_sort_ns;
        cs_sum += r.ck_sort_ns;
        co_sum += r.core_ns;
        max_user_events = max_user_events.max(r.max_user_events);
        click_window_iters += r.click_window_iters;
    }

    let merge_ns = t_merge.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
    let worker_wall_ns = wall_t0
        .map(|t| t.elapsed().as_nanos() as u64)
        .unwrap_or(0)
        .saturating_sub(merge_ns);
    let cpu_total = (cp_sum + es_sum + cs_sum + co_sum).max(1);
    let copy_ns = (worker_wall_ns as u128 * cp_sum as u128 / cpu_total as u128) as u64;
    let event_sort_ns = (worker_wall_ns as u128 * es_sum as u128 / cpu_total as u128) as u64;
    let click_sort_ns = (worker_wall_ns as u128 * cs_sum as u128 / cpu_total as u128) as u64;
    let core_ns = worker_wall_ns.saturating_sub(copy_ns + event_sort_ns + click_sort_ns);

    SessionOutput {
        user_buffers,
        campaign_stats: merged_camp,
        stats: SessionStats {
            copy_ns,
            event_sort_ns,
            click_sort_ns,
            core_ns,
            merge_ns,
            max_bucket_events,
            max_user_events,
            click_window_iters,
        },
    }
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn process_user(
    user_id: u64,
    u_events: &[CleanEvent],
    u_clicks: &[CleanClick],
    window_start: i64,
    start_epoch_day: i32,
    num_days: usize,
    cat_rev: &mut [i64],
    last_seen_gen: &mut [u32],
    session_gen: &mut u32,
    user_buf: &mut UserDailyBuffer,
    camp_stats: &mut [CampaignDayStat],
) -> u64 {
    let mut c_left = 0usize;
    let mut c_right = 0usize;
    let c_len = u_clicks.len();
    let mut assist_iters = 0u64;

    let mut cur_day_idx = usize::MAX;
    let mut cur_epoch_day = 0i32;
    let mut day_sessions = 0i64;
    let mut day_events = 0i64;
    let mut day_rev = 0i64;

    let mut i = 0usize;
    let n = u_events.len();
    while i < n {
        let session_start_rel = u_events[i].rel_ts;
        let mut prev_rel = session_start_rel;
        let mut sess_events = 1i64;
        let mut sess_rev = 0i64;
        let mut sess_has_purchase = false;

        let sess_epoch_day = (window_start + session_start_rel as i64).div_euclid(DAY_MS) as i32;
        let sess_day_idx = (sess_epoch_day - start_epoch_day) as usize;
        if sess_day_idx != cur_day_idx {
            if day_sessions > 0 {
                let top_cat = pick_top_category(day_rev, cat_rev);
                user_buf.push(
                    user_id,
                    cur_epoch_day,
                    day_sessions,
                    day_events,
                    day_rev,
                    top_cat,
                );
                if day_rev > 0 {
                    cat_rev.fill(0);
                }
            }
            cur_day_idx = sess_day_idx;
            cur_epoch_day = sess_epoch_day;
            day_sessions = 0;
            day_events = 0;
            day_rev = 0;
        }

        let pac0 = u_events[i].price_and_cat;
        let cat0 = (pac0 & 0xFF) as usize;
        if cat0 != NON_PURCHASE_CAT as usize {
            let p = (pac0 >> 8) as i64;
            sess_rev += p;
            sess_has_purchase = true;
            unsafe {
                *cat_rev.get_unchecked_mut(cat0) += p;
            }
        }
        i += 1;

        while i < n {
            let rel = u_events[i].rel_ts;
            if rel - prev_rel > SESSION_GAP_MS {
                break;
            }
            prev_rel = rel;
            sess_events += 1;
            let pac = u_events[i].price_and_cat;
            let cat = (pac & 0xFF) as usize;
            if cat != NON_PURCHASE_CAT as usize {
                let p = (pac >> 8) as i64;
                sess_rev += p;
                sess_has_purchase = true;
                unsafe {
                    *cat_rev.get_unchecked_mut(cat) += p;
                }
            }
            i += 1;
        }

        day_sessions += 1;
        day_events += sess_events;
        day_rev += sess_rev;

        // Credit and assists for this session
        let min_ck_rel = session_start_rel as i64;
        let max_ck_rel = min_ck_rel + SEVEN_DAYS_MS;

        while c_left < c_len && (u_clicks[c_left].rel_ts as i64) < min_ck_rel {
            c_left += 1;
        }
        if c_right < c_left {
            c_right = c_left;
        }
        while c_right < c_len && (u_clicks[c_right].rel_ts as i64) <= max_ck_rel {
            c_right += 1;
        }

        if c_left == c_right {
            let slot = unsafe { camp_stats.get_unchecked_mut(sess_day_idx) };
            slot.sessions += 1;
            if sess_has_purchase {
                slot.purchasing_sessions += 1;
                slot.revenue_cents += sess_rev;
            }
        } else {
            let credit_camp = unsafe { u_clicks.get_unchecked(c_right - 1).campaign_idx as usize };
            let slot = unsafe { camp_stats.get_unchecked_mut(credit_camp * num_days + sess_day_idx) };
            slot.sessions += 1;
            if sess_has_purchase {
                slot.purchasing_sessions += 1;
                slot.revenue_cents += sess_rev;
            }

            *session_gen = session_gen.wrapping_add(1);
            if *session_gen == 0 {
                last_seen_gen.fill(0);
                *session_gen = 1;
            }
            let gen_val = *session_gen;
            assist_iters += (c_right - c_left) as u64;
            for ck in &u_clicks[c_left..c_right] {
                let c = ck.campaign_idx as usize;
                unsafe {
                    if *last_seen_gen.get_unchecked(c) != gen_val {
                        *last_seen_gen.get_unchecked_mut(c) = gen_val;
                        camp_stats
                            .get_unchecked_mut(c * num_days + sess_day_idx)
                            .assisted_sessions += 1;
                    }
                }
            }
        }
    }

    if day_sessions > 0 {
        let top_cat = pick_top_category(day_rev, cat_rev);
        user_buf.push(
            user_id,
            cur_epoch_day,
            day_sessions,
            day_events,
            day_rev,
            top_cat,
        );
        if day_rev > 0 {
            cat_rev.fill(0);
        }
    }
    assist_iters
}

#[inline(always)]
fn pick_top_category(day_rev: i64, cat_rev: &[i64]) -> u8 {
    if day_rev == 0 {
        return NON_PURCHASE_CAT;
    }
    let mut best_idx = 0usize;
    let mut best_val = -1i64;
    for (idx, &val) in cat_rev.iter().enumerate() {
        if val > best_val {
            best_val = val;
            best_idx = idx;
        }
    }
    best_idx as u8
}
