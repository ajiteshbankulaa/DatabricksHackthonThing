use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrd};
use std::sync::{Mutex, OnceLock};

pub const EMPTY_ID: u64 = 0;
const FALLBACK_BIT: u64 = 1u64 << 63;
const NUM_SHARDS: usize = 64;
static FALLBACK_CALLS: AtomicUsize = AtomicUsize::new(0);

#[inline(always)]
pub fn fallback_calls() -> usize {
    FALLBACK_CALLS.load(AtomicOrd::Relaxed)
}

#[inline(always)]
pub fn is_fallback(id: u64) -> bool {
    (id & FALLBACK_BIT) != 0
}

/// Rapid mix64 for hash tables and partitioning.
#[inline(always)]
pub fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[derive(Default)]
struct InternShard {
    map: HashMap<Box<[u8]>, u32>,
    items: Vec<Box<[u8]>>,
}

pub struct IdInterner {
    shards: Vec<Mutex<InternShard>>,
}

impl IdInterner {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(Mutex::new(InternShard::default()));
        }
        Self { shards }
    }

    pub fn global() -> &'static IdInterner {
        static GLOBAL: OnceLock<IdInterner> = OnceLock::new();
        GLOBAL.get_or_init(IdInterner::new)
    }

    #[cold]
    #[inline(never)]
    pub fn intern_fallback(&self, bytes: &[u8]) -> u64 {
        FALLBACK_CALLS.fetch_add(1, AtomicOrd::Relaxed);
        let h = fnv1a_64(bytes);
        let shard_idx = (h as usize) & (NUM_SHARDS - 1);
        let mut guard = self.shards[shard_idx].lock().unwrap();
        if let Some(&idx) = guard.map.get(bytes) {
            return FALLBACK_BIT | ((shard_idx as u64) << 32) | (idx as u64);
        }
        let idx = guard.items.len() as u32;
        let boxed: Box<[u8]> = bytes.into();
        guard.map.insert(boxed.clone(), idx);
        guard.items.push(boxed);
        FALLBACK_BIT | ((shard_idx as u64) << 32) | (idx as u64)
    }

    #[cold]
    #[inline(never)]
    pub fn write_fallback(&self, id: u64, out: &mut Vec<u8>) {
        let shard_idx = ((id >> 32) as usize) & (NUM_SHARDS - 1);
        let idx = (id & 0xFFFF_FFFF) as usize;
        let guard = self.shards[shard_idx].lock().unwrap();
        out.extend_from_slice(&guard.items[idx]);
    }

    #[cold]
    #[inline(never)]
    pub fn with_bytes<R>(&self, id: u64, f: impl FnOnce(&[u8]) -> R) -> R {
        if id == 0 {
            return f(&[]);
        }
        if !is_fallback(id) {
            let mut buf = [0u8; 9];
            let len = unpack_inline(id, &mut buf);
            return f(&buf[..len]);
        }
        let shard_idx = ((id >> 32) as usize) & (NUM_SHARDS - 1);
        let idx = (id & 0xFFFF_FFFF) as usize;
        let guard = self.shards[shard_idx].lock().unwrap();
        f(&guard.items[idx])
    }
}

#[inline(always)]
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Packs a byte slice into an order-preserving u64 when len <= 9 and all bytes are in 1..=127.
/// Empty slice returns 0 (`EMPTY_ID`). Any slice > 9 bytes or containing 0 or >= 128 uses `IdInterner`.
#[inline(always)]
pub fn pack_id(bytes: &[u8]) -> u64 {
    let len = bytes.len();
    if len == 0 {
        return EMPTY_ID;
    }
    if len <= 9 {
        let mut acc = 0u64;
        let mut i = 0;
        let mut ok = true;
        while i < len {
            let b = bytes[i];
            if b.wrapping_sub(1) >= 127 {
                ok = false;
                break;
            }
            acc |= (b as u64) << (56 - i * 7);
            i += 1;
        }
        if ok {
            return acc;
        }
    }
    IdInterner::global().intern_fallback(bytes)
}

#[inline(always)]
pub fn unpack_inline(id: u64, buf: &mut [u8; 9]) -> usize {
    let mut len = 0;
    let mut i = 0;
    while i < 9 {
        let b = ((id >> (56 - i * 7)) & 0x7F) as u8;
        if b == 0 {
            break;
        }
        buf[i] = b;
        len = i + 1;
        i += 1;
    }
    len
}

#[inline(always)]
pub fn append_id_bytes(id: u64, out: &mut Vec<u8>) {
    if id == 0 {
        return;
    }
    if !is_fallback(id) {
        let mut buf = [0u8; 9];
        let len = unpack_inline(id, &mut buf);
        out.extend_from_slice(&buf[..len]);
    } else {
        IdInterner::global().write_fallback(id, out);
    }
}

/// Byte-wise lexicographical comparison of two packed IDs.
#[inline(always)]
pub fn cmp_id(a: u64, b: u64) -> Ordering {
    if ((a | b) & FALLBACK_BIT) == 0 {
        return a.cmp(&b);
    }
    cmp_id_slow(a, b)
}

#[cold]
#[inline(never)]
fn cmp_id_slow(a: u64, b: u64) -> Ordering {
    if a == b {
        return Ordering::Equal;
    }
    let interner = IdInterner::global();
    let mut va = Vec::new();
    let mut vb = Vec::new();
    interner.with_bytes(a, |ba| va.extend_from_slice(ba));
    interner.with_bytes(b, |bb| vb.extend_from_slice(bb));
    va.cmp(&vb)
}
