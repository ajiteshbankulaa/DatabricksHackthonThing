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

#[allow(dead_code)]
#[inline(always)]
fn pack_8_swar(be0: u64) -> u64 {
    let x = ((be0 & 0x7F00_7F00_7F00_7F00) >> 1) | (be0 & 0x007F_007F_007F_007F);
    let y = ((x & 0x3FFF_0000_3FFF_0000) >> 2) | (x & 0x0000_3FFF_0000_3FFF);
    (((y & 0x0FFF_FFFF_0000_0000) >> 4) | (y & 0x0000_0000_0FFF_FFFF)) << 7
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
fn pack_8_inline(be0: u64) -> u64 {
    unsafe { core::arch::x86_64::_pext_u64(be0, 0x7F7F_7F7F_7F7F_7F7F) << 7 }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
#[inline(always)]
fn pack_8_inline(be0: u64) -> u64 {
    pack_8_swar(be0)
}

#[allow(dead_code)]
#[inline(always)]
fn unpack_8_swar(v: u64) -> u64 {
    let a = ((v & 0x00FF_FFFF_F000_0000) << 4) | (v & 0x0000_0000_0FFF_FFFF);
    let b = ((a & 0x0FFF_C000_0FFF_C000) << 2) | (a & 0x0000_3FFF_0000_3FFF);
    ((b & 0x3F80_3F80_3F80_3F80) << 1) | (b & 0x007F_007F_007F_007F)
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
fn unpack_8_inline(v: u64) -> u64 {
    unsafe { core::arch::x86_64::_pdep_u64(v, 0x7F7F_7F7F_7F7F_7F7F) }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
#[inline(always)]
fn unpack_8_inline(v: u64) -> u64 {
    unpack_8_swar(v)
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
        let ptr = bytes.as_ptr();
        if len >= 8 {
            let raw = unsafe { *(ptr as *const [u8; 8]) };
            let be0 = u64::from_be_bytes(raw);
            let bad = (be0 | be0.wrapping_sub(0x0101_0101_0101_0101)) & 0x8080_8080_8080_8080;
            let b8 = if len == 9 { unsafe { *ptr.add(8) } } else { 0 };
            if bad == 0 && (len == 8 || b8.wrapping_sub(1) < 127) {
                return pack_8_inline(be0) | (b8 as u64);
            }
        } else if len >= 4 {
            let raw_hi = unsafe { *(ptr as *const [u8; 4]) };
            let raw_lo = unsafe { *(ptr.add(len - 4) as *const [u8; 4]) };
            let comb = ((u32::from_ne_bytes(raw_hi) as u64) << 32)
                | (u32::from_ne_bytes(raw_lo) as u64);
            let bad = (comb | comb.wrapping_sub(0x0101_0101_0101_0101)) & 0x8080_8080_8080_8080;
            if bad == 0 {
                let be_hi = u32::from_be_bytes(raw_hi) as u64;
                let be_lo = u32::from_be_bytes(raw_lo) as u64;
                let tail_mask = (1u64 << ((len - 4) * 8)) - 1;
                let tail_shift = (8 - len) * 8;
                let be0 = (be_hi << 32) | ((be_lo & tail_mask) << tail_shift);
                return pack_8_inline(be0);
            }
        } else {
            let b0 = unsafe { *ptr };
            let mut valid = b0.wrapping_sub(1) < 127;
            let mut acc = (b0 as u64) << 56;
            if len >= 2 {
                let b1 = unsafe { *ptr.add(1) };
                valid &= b1.wrapping_sub(1) < 127;
                acc |= (b1 as u64) << 49;
                if len == 3 {
                    let b2 = unsafe { *ptr.add(2) };
                    valid &= b2.wrapping_sub(1) < 127;
                    acc |= (b2 as u64) << 42;
                }
            }
            if valid {
                return acc;
            }
        }
    }
    IdInterner::global().intern_fallback(bytes)
}

#[inline(always)]
pub fn unpack_inline(id: u64, buf: &mut [u8; 9]) -> usize {
    if id == 0 {
        return 0;
    }
    let be0 = unpack_8_inline(id >> 7);
    unsafe {
        *(buf.as_mut_ptr() as *mut [u8; 8]) = be0.to_be_bytes();
        *buf.get_unchecked_mut(8) = (id & 0x7F) as u8;
    }
    9 - (id.trailing_zeros() as usize / 7)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn pack_scalar_ref(bytes: &[u8]) -> Option<u64> {
        if bytes.is_empty() {
            return Some(EMPTY_ID);
        }
        if bytes.len() > 9 {
            return None;
        }
        let mut acc = 0u64;
        for (i, &b) in bytes.iter().enumerate() {
            if b.wrapping_sub(1) >= 127 {
                return None;
            }
            acc |= (b as u64) << (56 - i * 7);
        }
        Some(acc)
    }

    fn unpack_scalar_ref(id: u64) -> Vec<u8> {
        let mut out = Vec::new();
        for i in 0..9 {
            let b = ((id >> (56 - i * 7)) & 0x7F) as u8;
            if b == 0 {
                break;
            }
            out.push(b);
        }
        out
    }

    #[test]
    fn pack_unpack_parity_0_to_12() {
        let sample = b"a1_Z9~@!PqRx";
        for len in 0..=12 {
            let slice = &sample[..len];
            let got = pack_id(slice);
            if let Some(expected) = pack_scalar_ref(slice) {
                assert_eq!(got, expected, "mismatch for len={len}");
                let mut buf = [0u8; 9];
                let ulen = unpack_inline(got, &mut buf);
                assert_eq!(&buf[..ulen], slice);
                assert_eq!(unpack_scalar_ref(got), slice);
            } else {
                assert!(is_fallback(got), "expected fallback for len={len}");
                let mut out = Vec::new();
                append_id_bytes(got, &mut out);
                assert_eq!(out, slice);
            }
        }
    }

    #[test]
    fn pack_invalid_byte_positions_use_fallback() {
        for len in 1..=9 {
            for pos in 0..len {
                for &bad in &[0u8, 128u8, 200u8, 255u8] {
                    let mut buf = vec![b'@'; len];
                    buf[pos] = bad;
                    let id = pack_id(&buf);
                    assert!(is_fallback(id), "len={len} pos={pos} bad={bad} did not fallback");
                    let mut out = Vec::new();
                    append_id_bytes(id, &mut out);
                    assert_eq!(out, buf);
                }
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn bmi2_and_swar_exact_equivalence() {
        #[target_feature(enable = "bmi2")]
        unsafe fn pext_ref(be0: u64) -> u64 {
            core::arch::x86_64::_pext_u64(be0, 0x7F7F_7F7F_7F7F_7F7F) << 7
        }
        #[target_feature(enable = "bmi2")]
        unsafe fn pdep_ref(v: u64) -> u64 {
            core::arch::x86_64::_pdep_u64(v, 0x7F7F_7F7F_7F7F_7F7F)
        }

        if !std::is_x86_feature_detected!("bmi2") {
            return;
        }
        let mut x = 0x1234_5678_9abc_def0u64;
        for _ in 0..10_000 {
            x = mix64(x);
            let be0 = x & 0x7F7F_7F7F_7F7F_7F7F;
            let packed_swar = pack_8_swar(be0);
            let packed_bmi2 = unsafe { pext_ref(be0) };
            assert_eq!(packed_swar, packed_bmi2);
            let unpacked_swar = unpack_8_swar(packed_swar >> 7);
            let unpacked_bmi2 = unsafe { pdep_ref(packed_bmi2 >> 7) };
            assert_eq!(unpacked_swar, be0);
            assert_eq!(unpacked_bmi2, be0);
        }
    }
}
