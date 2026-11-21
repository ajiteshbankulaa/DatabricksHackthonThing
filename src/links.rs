use std::cmp::Ordering;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrd};
use std::time::Instant;

use crate::csv::{build_chunks, list_csv_files, read_exact_at, scan_csv_chunk};
use crate::id::{EMPTY_ID, cmp_id, mix64, pack_id};

#[derive(Default, Clone, Copy)]
pub struct LinkStats {
    pub scan_ns: u64,
    pub dsu_ns: u64,
    pub edges: usize,
    pub nodes: usize,
}

#[derive(Clone, Copy)]
struct LinkSlot {
    key: u64,
    val: u64,
}

pub struct LinkMap {
    slots: Vec<LinkSlot>,
    mask: usize,
    pub stats: LinkStats,
}

impl LinkMap {
    pub fn empty() -> Self {
        Self {
            slots: Vec::new(),
            mask: 0,
            stats: LinkStats::default(),
        }
    }

    #[inline(always)]
    pub fn resolve(&self, user_id: u64) -> u64 {
        if self.slots.is_empty() || user_id == EMPTY_ID {
            return user_id;
        }
        let mut idx = (mix64(user_id) as usize) & self.mask;
        loop {
            let slot = unsafe { *self.slots.get_unchecked(idx) };
            if slot.key == user_id {
                return slot.val;
            }
            if slot.key == EMPTY_ID {
                return user_id;
            }
            idx = (idx + 1) & self.mask;
        }
    }

    pub fn build(data_dir: &Path, ncpu: usize, timing: bool) -> io::Result<Self> {
        let t0 = if timing { Some(Instant::now()) } else { None };
        let paths = list_csv_files(&data_dir.join("links"))?;
        if paths.is_empty() {
            return Ok(Self::empty());
        }

        let (files, chunks) = build_chunks(&paths, 2 * 1024 * 1024)?;
        if chunks.is_empty() {
            return Ok(Self::empty());
        }

        let num_threads = ncpu.min(chunks.len()).max(1);
        let next_chunk = AtomicUsize::new(0);

        let edge_vecs: Vec<Vec<(u64, u64)>> = std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(num_threads);
            for _ in 0..num_threads {
                handles.push(s.spawn(|| -> io::Result<Vec<(u64, u64)>> {
                    let mut buf: Vec<u8> = Vec::new();
                    let mut edges: Vec<(u64, u64)> = Vec::new();
                    loop {
                        let cidx = next_chunk.fetch_add(1, AtomicOrd::Relaxed);
                        let Some(chunk) = chunks.get(cidx) else {
                            break;
                        };
                        let len = (chunk.end - chunk.start) as usize;
                        if len == 0 {
                            continue;
                        }
                        if buf.len() < len + 64 {
                            buf.resize(len + 64, 0);
                        }
                        let n = read_exact_at(&files[chunk.file_idx], chunk.start, &mut buf[..len])?;
                        buf[n..n + 64].fill(0);
                        scan_csv_chunk::<2, _>(&buf, n, |f| {
                            if f[0].is_empty() || f[1].is_empty() {
                                return;
                            }
                            let a = pack_id(f[0]);
                            let b = pack_id(f[1]);
                            if a != b {
                                edges.push((a, b));
                            }
                        });
                    }
                    Ok(edges)
                }));
            }
            let mut out = Vec::with_capacity(num_threads);
            for h in handles {
                out.push(h.join().unwrap()?);
            }
            Ok::<_, io::Error>(out)
        })?;

        let scan_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
        let t1 = if timing { Some(Instant::now()) } else { None };
        let mut map = Self::from_edge_vecs(&edge_vecs);
        map.stats.scan_ns = scan_ns;
        map.stats.dsu_ns = t1.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
        Ok(map)
    }

    pub fn from_edge_vecs(edge_vecs: &[Vec<(u64, u64)>]) -> Self {
        let total_edges: usize = edge_vecs.iter().map(|v| v.len()).sum();
        if total_edges == 0 {
            return Self::empty();
        }

        // Upper bound on distinct nodes is 2 * total_edges; cap >= 3 * total_edges keeps load <= 66.7%
        let cap = (total_edges.saturating_mul(3)).next_power_of_two().max(1024);
        let mask = cap - 1;
        let mut slots = vec![
            LinkSlot {
                key: EMPTY_ID,
                val: 0
            };
            cap
        ];
        let est_nodes = total_edges.min(1 << 22);
        let mut parent: Vec<u32> = Vec::with_capacity(est_nodes);
        let mut node_uid: Vec<u64> = Vec::with_capacity(est_nodes);
        let mut node_slot: Vec<u32> = Vec::with_capacity(est_nodes);

        #[inline(always)]
        fn get_or_insert(
            slots: &mut [LinkSlot],
            mask: usize,
            parent: &mut Vec<u32>,
            node_uid: &mut Vec<u64>,
            node_slot: &mut Vec<u32>,
            key: u64,
        ) -> u32 {
            let mut idx = (mix64(key) as usize) & mask;
            loop {
                let s = unsafe { *slots.get_unchecked(idx) };
                if s.key == key {
                    return s.val as u32;
                }
                if s.key == EMPTY_ID {
                    let node_idx = parent.len() as u32;
                    parent.push(node_idx);
                    node_uid.push(key);
                    node_slot.push(idx as u32);
                    unsafe {
                        *slots.get_unchecked_mut(idx) = LinkSlot {
                            key,
                            val: node_idx as u64,
                        };
                    }
                    return node_idx;
                }
                idx = (idx + 1) & mask;
            }
        }

        for vec in edge_vecs {
            for &(a, b) in vec {
                let u = get_or_insert(&mut slots, mask, &mut parent, &mut node_uid, &mut node_slot, a);
                let v = get_or_insert(&mut slots, mask, &mut parent, &mut node_uid, &mut node_slot, b);
                unite_rem(&mut parent, u, v);
            }
        }

        let num_nodes = parent.len();
        let mut min_uid = node_uid;
        for i in 0..num_nodes {
            let p = parent[i] as usize;
            let r = parent[p];
            parent[i] = r;
            let ru = r as usize;
            if ru != i && cmp_id(min_uid[i], min_uid[ru]) == Ordering::Less {
                min_uid[ru] = min_uid[i];
            }
        }

        for (i, &s_idx) in node_slot.iter().enumerate() {
            let r = parent[i] as usize;
            unsafe {
                slots.get_unchecked_mut(s_idx as usize).val = *min_uid.get_unchecked(r);
            }
        }

        Self {
            slots,
            mask,
            stats: LinkStats {
                scan_ns: 0,
                dsu_ns: 0,
                edges: total_edges,
                nodes: num_nodes,
            },
        }
    }
}

#[inline(always)]
fn unite_rem(parent: &mut [u32], mut rx: u32, mut ry: u32) {
    unsafe {
        while *parent.get_unchecked(rx as usize) != *parent.get_unchecked(ry as usize) {
            let px = *parent.get_unchecked(rx as usize);
            let py = *parent.get_unchecked(ry as usize);
            if px < py {
                std::mem::swap(&mut rx, &mut ry);
                // After swap, px was < py, so now parent[rx] > parent[ry]
                if rx == py {
                    *parent.get_unchecked_mut(rx as usize) = px;
                    break;
                }
                *parent.get_unchecked_mut(rx as usize) = px;
                rx = py;
            } else {
                if rx == px {
                    *parent.get_unchecked_mut(rx as usize) = py;
                    break;
                }
                *parent.get_unchecked_mut(rx as usize) = py;
                rx = px;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dsu_chains_and_stars() {
        let edges = vec![vec![
            (pack_id(b"u3"), pack_id(b"u9")),
            (pack_id(b"u9"), pack_id(b"u2")),
            (pack_id(b"U7"), pack_id(b"u9")),
            (pack_id(b"u10"), pack_id(b"u10")),
        ]];
        let map = LinkMap::from_edge_vecs(&edges);
        let expected = pack_id(b"U7");
        assert_eq!(map.resolve(pack_id(b"u3")), expected);
        assert_eq!(map.resolve(pack_id(b"u9")), expected);
        assert_eq!(map.resolve(pack_id(b"u2")), expected);
        assert_eq!(map.resolve(pack_id(b"U7")), expected);
        assert_eq!(map.resolve(pack_id(b"u10")), pack_id(b"u10"));
        assert_eq!(map.resolve(pack_id(b"u_unlinked")), pack_id(b"u_unlinked"));
    }
}
