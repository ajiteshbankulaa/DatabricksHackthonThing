use std::cell::Cell;

pub const SECOND_MS: i64 = 1_000;
pub const MINUTE_MS: i64 = 60 * SECOND_MS;
pub const HOUR_MS: i64 = 60 * MINUTE_MS;
pub const DAY_MS: i64 = 24 * HOUR_MS;
pub const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;

#[derive(Clone, Copy)]
struct DateCache {
    prefix: [u8; 10],
    day_ms: i64,
}

impl Default for DateCache {
    fn default() -> Self {
        Self {
            prefix: [0u8; 10],
            day_ms: 0,
        }
    }
}

thread_local! {
    static DATE_CACHE: Cell<DateCache> = Cell::new(DateCache::default());
}

#[inline(always)]
fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[inline(always)]
fn days_in_month(year: i64, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

#[inline(always)]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline(always)]
fn parse_2_digits(a: u8, b: u8) -> Option<u8> {
    let d0 = a.wrapping_sub(b'0');
    let d1 = b.wrapping_sub(b'0');
    if d0 > 9 || d1 > 9 {
        return None;
    }
    Some(d0 * 10 + d1)
}

#[inline(always)]
fn parse_4_digits_swar(bytes: &[u8]) -> Option<i64> {
    let v = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let d = v.wrapping_sub(0x3030_3030);
    let check = (v.wrapping_add(0x4646_4646) | d) & 0x8080_8080;
    if check != 0 {
        return None;
    }
    let b0 = (d & 0xFF) as i64;
    let b1 = ((d >> 8) & 0xFF) as i64;
    let b2 = ((d >> 16) & 0xFF) as i64;
    let b3 = ((d >> 24) & 0xFF) as i64;
    Some(b0 * 1000 + b1 * 100 + b2 * 10 + b3)
}

/// Parses `YYYY-MM-DDTHH:MM:SS` (19 bytes) into UTC epoch ms (before any timezone offset).
#[inline(always)]
fn parse_datetime_prefix(b: &[u8]) -> Option<i64> {
    if b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let hour = parse_2_digits(b[11], b[12])?;
    let minute = parse_2_digits(b[14], b[15])?;
    let second = parse_2_digits(b[17], b[18])?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let time_ms = (hour as i64 * 3_600 + minute as i64 * 60 + second as i64) * SECOND_MS;

    let cached = DATE_CACHE.with(|c| c.get());
    if cached.prefix == b[..10] {
        return Some(cached.day_ms + time_ms);
    }

    if b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let year = parse_4_digits_swar(&b[0..4])?;
    let month = parse_2_digits(b[5], b[6])?;
    let day = parse_2_digits(b[8], b[9])?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return None;
    }
    let day_ms = days_from_civil(year, month as i64, day as i64) * DAY_MS;
    let mut prefix = [0u8; 10];
    prefix.copy_from_slice(&b[..10]);
    DATE_CACHE.with(|c| c.set(DateCache { prefix, day_ms }));
    Some(day_ms + time_ms)
}

/// Parses Form F1 (`YYYY-MM-DDTHH:MM:SSZ`).
#[inline(always)]
pub fn parse_f1(b: &[u8]) -> Option<i64> {
    if b.len() != 20 || b[19] != b'Z' {
        return None;
    }
    parse_datetime_prefix(b)
}

/// Parses Form F1, F2 (`YYYY-MM-DDTHH:MM:SS[+-]HH:MM`), or F3 (ASCII digits ms).
#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    if b.len() == 20 && b[19] == b'Z' {
        return parse_datetime_prefix(b);
    }
    if b.len() == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let base_ms = parse_datetime_prefix(b)?;
        let off_h = parse_2_digits(b[20], b[21])?;
        let off_m = parse_2_digits(b[23], b[24])?;
        if off_h > 23 || off_m > 59 {
            return None;
        }
        let off_ms = (off_h as i64 * 60 + off_m as i64) * MINUTE_MS;
        return if b[19] == b'+' {
            Some(base_ms - off_ms)
        } else {
            Some(base_ms + off_ms)
        };
    }
    if b.is_empty() {
        return None;
    }
    parse_f3(b)
}

#[inline(always)]
fn parse_f3(b: &[u8]) -> Option<i64> {
    let mut acc: u64 = 0;
    for &ch in b {
        let d = ch.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        acc = acc.saturating_mul(10).saturating_add(d as u64);
    }
    Some(acc.min(i64::MAX as u64) as i64)
}

/// Parses `price_cents` for a purchase: non-empty ASCII digits only, value in 1..=10_000_000.
#[inline(always)]
pub fn parse_price_cents(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut acc: u64 = 0;
    for &ch in b {
        let d = ch.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        acc = acc * 10 + d as u64;
        if acc > 10_000_000 {
            return None;
        }
    }
    if acc == 0 {
        return None;
    }
    Some(acc as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamps_and_swar() {
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), parse_ts(b"2026-10-03T19:58:10-04:00"));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), parse_ts(b"1791071890000"));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), parse_ts(b"0001791071890000"));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), parse_ts(b"00000001791071890000"));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), parse_ts(b"0000000000001791071890000"));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10-00:00"), parse_ts(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts(b"2028-02-29T12:00:00Z").is_some(), true);
        assert_eq!(parse_ts(b"2027-02-29T12:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-02-30T12:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-13-01T12:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T24:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:60Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+24:00"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+05:60"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10z"), None);
        assert_eq!(parse_ts(b"2026-10-03 23:58:10Z"), None);
        assert_eq!(parse_ts(b"1791071890000.0"), None);
        assert_eq!(parse_ts(b"179107189000000000000"), Some(i64::MAX));
        assert_eq!(parse_4_digits_swar(b"2026"), Some(2026));
        assert_eq!(parse_4_digits_swar(b"20:6"), None);
        assert_eq!(parse_4_digits_swar(b"20/6"), None);
    }
}
