pub const SECOND_MS: i64 = 1_000;
pub const MINUTE_MS: i64 = 60 * SECOND_MS;
pub const HOUR_MS: i64 = 60 * MINUTE_MS;
pub const DAY_MS: i64 = 24 * HOUR_MS;
pub const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;

/// Precomputed L1-resident date lookup covering `[base_year - 1, base_year + 1]`.
/// Indexed by `(year_slot << 9) | (month << 5) | day`; invalid dates hold `i64::MIN`.
pub struct DateTable {
    pub year_u32: [u32; 3],
    pub day_ms: [i64; 3 * 512],
}

impl DateTable {
    pub fn for_window_start(window_start_ms: i64) -> Self {
        let base_year = year_from_ms(window_start_ms);
        let mut year_u32 = [0u32; 3];
        let mut day_ms = [i64::MIN; 3 * 512];

        for y_off in 0..3 {
            let year = base_year - 1 + y_off as i64;
            if !(0..=9999).contains(&year) {
                continue;
            }
            let ys = format!("{year:04}");
            year_u32[y_off] = u32::from_le_bytes(ys.as_bytes().try_into().unwrap());
            let base = y_off << 9;
            for month in 1..=12u8 {
                let max_d = days_in_month(year, month);
                for day in 1..=max_d {
                    let idx = base | ((month as usize) << 5) | (day as usize);
                    day_ms[idx] = days_from_civil(year, month as i64, day as i64) * DAY_MS;
                }
            }
        }

        Self { year_u32, day_ms }
    }
}

#[inline(always)]
fn year_from_ms(milliseconds: i64) -> i64 {
    let days = milliseconds.div_euclid(DAY_MS);
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    year_of_era + era * 400 + i64::from(month <= 2)
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

#[inline(always)]
fn parse_hms_swar(b: &[u8]) -> Option<i64> {
    // Expects b[10] == 'T' and b[11..19] == "HH:MM:SS"
    if b[10] != b'T' {
        return None;
    }
    let w = u64::from_le_bytes(b[11..19].try_into().unwrap());
    if (w & 0x0000_FF00_00FF_0000) != 0x0000_3A00_003A_0000 {
        return None;
    }
    let wm = w ^ 0x0000_0A00_000A_0000;
    let d = wm.wrapping_sub(0x3030_3030_3030_3030);
    if ((wm.wrapping_add(0x4646_4646_4646_4646) | d) & 0x8080_8080_8080_8080) != 0 {
        return None;
    }
    let hour = ((d & 0xFF) * 10 + ((d >> 8) & 0xFF)) as i64;
    let minute = (((d >> 24) & 0xFF) * 10 + ((d >> 32) & 0xFF)) as i64;
    let second = (((d >> 48) & 0xFF) * 10 + ((d >> 56) & 0xFF)) as i64;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some((hour * 3_600 + minute * 60 + second) * SECOND_MS)
}

#[cold]
#[inline(never)]
fn parse_date_ms_slow(b: &[u8]) -> Option<i64> {
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
    Some(days_from_civil(year, month as i64, day as i64) * DAY_MS)
}

/// Parses `YYYY-MM-DDTHH:MM:SS` (19 bytes) into UTC epoch ms (before any timezone offset).
#[inline(always)]
fn parse_datetime_prefix(b: &[u8], dt: Option<&DateTable>) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let time_ms = parse_hms_swar(b)?;

    if let Some(table) = dt {
        let y_u32 = u32::from_le_bytes(b[0..4].try_into().unwrap());
        let y_off = if y_u32 == table.year_u32[1] {
            1usize
        } else if y_u32 == table.year_u32[2] {
            2usize
        } else if y_u32 == table.year_u32[0] {
            0usize
        } else {
            return parse_date_ms_slow(b).map(|dms| dms + time_ms);
        };
        let month = parse_2_digits(b[5], b[6])?;
        let day = parse_2_digits(b[8], b[9])?;
        if month > 12 || day > 31 {
            return None;
        }
        let dms = unsafe {
            *table
                .day_ms
                .get_unchecked((y_off << 9) | ((month as usize) << 5) | (day as usize))
        };
        if dms == i64::MIN {
            return None;
        }
        return Some(dms + time_ms);
    }

    parse_date_ms_slow(b).map(|dms| dms + time_ms)
}

/// Parses Form F1 (`YYYY-MM-DDTHH:MM:SSZ`) with a precomputed `DateTable`.
#[inline(always)]
pub fn parse_f1_fast(b: &[u8], dt: &DateTable) -> Option<i64> {
    if b.len() != 20 || b[19] != b'Z' {
        return None;
    }
    parse_datetime_prefix(b, Some(dt))
}

/// Parses Form F1, F2 (`YYYY-MM-DDTHH:MM:SS[+-]HH:MM`), or F3 (ASCII digits ms) with `DateTable`.
#[inline(always)]
pub fn parse_ts_fast(b: &[u8], dt: &DateTable) -> Option<i64> {
    if b.len() == 20 && b[19] == b'Z' {
        return parse_datetime_prefix(b, Some(dt));
    }
    if b.len() == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let base_ms = parse_datetime_prefix(b, Some(dt))?;
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

/// Parses Form F1, F2, or F3 without a `DateTable` (used for manifest and unit tests).
#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    if b.len() == 20 && b[19] == b'Z' {
        return parse_datetime_prefix(b, None);
    }
    if b.len() == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let base_ms = parse_datetime_prefix(b, None)?;
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
fn swar_8_digits(d: u64) -> i64 {
    let pairs = ((d & 0x00FF_00FF_00FF_00FF) * 10) + ((d >> 8) & 0x00FF_00FF_00FF_00FF);
    let quads = ((pairs & 0x0000_FFFF_0000_FFFF) * 100) + ((pairs >> 16) & 0x0000_FFFF_0000_FFFF);
    ((quads & 0xFFFF_FFFF) * 10_000 + (quads >> 32)) as i64
}

#[inline(always)]
fn swar_4_digits(d: u32) -> i64 {
    let pairs = ((d & 0x00FF_00FF) * 10) + ((d >> 8) & 0x00FF_00FF);
    (((pairs & 0xFFFF) * 100) + (pairs >> 16)) as i64
}

#[inline(always)]
fn parse_f3(b: &[u8]) -> Option<i64> {
    if b.len() == 13 {
        let w0 = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let d0 = w0.wrapping_sub(0x3030_3030_3030_3030);
        let c0 = (w0.wrapping_add(0x4646_4646_4646_4646) | d0) & 0x8080_8080_8080_8080;

        let w1 = u32::from_le_bytes(b[8..12].try_into().unwrap());
        let d1 = w1.wrapping_sub(0x3030_3030);
        let c1 = (w1.wrapping_add(0x4646_4646) | d1) & 0x8080_8080;

        let last = b[12].wrapping_sub(b'0');
        if (c0 | (c1 as u64)) == 0 && last <= 9 {
            let hi8 = swar_8_digits(d0);
            let mid4 = swar_4_digits(d1);
            return Some((hi8 * 10_000 + mid4) * 10 + last as i64);
        }
        return None;
    }

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
        let dt = DateTable::for_window_start(parse_ts(b"2026-10-01T00:00:00Z").unwrap());
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), parse_ts(b"2026-10-03T19:58:10-04:00"));
        assert_eq!(parse_ts_fast(b"2026-10-03T23:58:10Z", &dt), parse_ts(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts_fast(b"2026-10-03T19:58:10-04:00", &dt), parse_ts(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts_fast(b"1791071890000", &dt), parse_ts(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts(b"1791071890000"), parse_ts(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts(b"0001791071890000"), parse_ts(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts(b"00000001791071890000"), parse_ts(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts(b"0000000000001791071890000"), parse_ts(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10-00:00"), parse_ts(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts(b"2028-02-29T12:00:00Z").is_some(), true);
        assert_eq!(parse_ts_fast(b"2027-02-29T12:00:00Z", &dt), None);
        assert_eq!(parse_ts_fast(b"2026-02-30T12:00:00Z", &dt), None);
        assert_eq!(parse_ts_fast(b"2026-13-01T12:00:00Z", &dt), None);
        assert_eq!(parse_ts_fast(b"2026-10-03T23;58:10Z", &dt), None);
        assert_eq!(parse_ts(b"2026-10-03T24:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:60Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+24:00"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+05:60"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10z"), None);
        assert_eq!(parse_ts(b"2026-10-03 23:58:10Z"), None);
        assert_eq!(parse_ts(b"1791071890000.0"), None);
        assert_eq!(parse_ts(b"179107189000000000000"), Some(i64::MAX));
        assert_eq!(parse_f3(b"0000000000000"), Some(0));
        assert_eq!(parse_f3(b"9999999999999"), Some(9_999_999_999_999));
        assert_eq!(parse_f3(b"179107189000a"), None);
        assert_eq!(parse_4_digits_swar(b"2026"), Some(2026));
        assert_eq!(parse_4_digits_swar(b"20:6"), None);
        assert_eq!(parse_4_digits_swar(b"20/6"), None);
    }
}
