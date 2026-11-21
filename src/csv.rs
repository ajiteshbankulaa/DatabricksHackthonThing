use std::fs::File;
use std::io;
use std::path::Path;

#[inline(always)]
pub fn unquote_field(s: &[u8]) -> Option<&[u8]> {
    let len = s.len();
    if len == 0 {
        return Some(s);
    }
    if s[0] == b'"' {
        if len < 2 || s[len - 1] != b'"' {
            return None;
        }
        let inner = &s[1..len - 1];
        if inner.contains(&b'"') {
            return None;
        }
        Some(inner)
    } else {
        if s.contains(&b'"') {
            return None;
        }
        Some(s)
    }
}

/// Reads bytes at `offset` until `buf` is filled or EOF.
pub fn read_exact_at(file: &File, mut offset: u64, mut buf: &mut [u8]) -> io::Result<usize> {
    let mut total = 0;
    while !buf.is_empty() {
        #[cfg(unix)]
        let n = std::os::unix::fs::FileExt::read_at(file, buf, offset)?;
        #[cfg(windows)]
        let n = std::os::windows::fs::FileExt::seek_read(file, buf, offset)?;
        if n == 0 {
            break;
        }
        total += n;
        offset += n as u64;
        buf = &mut buf[n..];
    }
    Ok(total)
}

pub struct ChunkTask {
    pub file_idx: usize,
    pub start: u64,
    pub end: u64,
}

/// Finds the byte offset right after the first `\n` (header line end), or `file_len` if none.
pub fn find_header_end(file: &File, file_len: u64) -> io::Result<u64> {
    let mut pos = 0u64;
    let mut buf = [0u8; 4096];
    while pos < file_len {
        let to_read = (file_len - pos).min(buf.len() as u64) as usize;
        let n = read_exact_at(file, pos, &mut buf[..to_read])?;
        if n == 0 {
            break;
        }
        if let Some(idx) = buf[..n].iter().position(|&b| b == b'\n') {
            return Ok(pos + idx as u64 + 1);
        }
        pos += n as u64;
    }
    Ok(file_len)
}

/// Finds the byte offset right after the next `\n` at or after `from`, or `file_len`.
fn find_next_newline_after(file: &File, mut pos: u64, file_len: u64) -> io::Result<u64> {
    let mut buf = [0u8; 4096];
    while pos < file_len {
        let to_read = (file_len - pos).min(buf.len() as u64) as usize;
        let n = read_exact_at(file, pos, &mut buf[..to_read])?;
        if n == 0 {
            break;
        }
        if let Some(idx) = buf[..n].iter().position(|&b| b == b'\n') {
            return Ok(pos + idx as u64 + 1);
        }
        pos += n as u64;
    }
    Ok(file_len)
}

pub fn build_chunks(
    paths: &[std::path::PathBuf],
    chunk_size: u64,
) -> io::Result<(Vec<File>, Vec<ChunkTask>)> {
    let mut files = Vec::with_capacity(paths.len());
    let mut chunks = Vec::new();
    for (file_idx, path) in paths.iter().enumerate() {
        let file = File::open(path)?;
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            unsafe extern "C" {
                fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
            }
            const POSIX_FADV_SEQUENTIAL: i32 = 2;
            unsafe {
                posix_fadvise(file.as_raw_fd(), 0, 0, POSIX_FADV_SEQUENTIAL);
            }
        }
        let file_len = file.metadata()?.len();
        let data_start = find_header_end(&file, file_len)?;
        files.push(file);
        if data_start >= file_len {
            continue;
        }
        let remaining = file_len - data_start;
        if remaining <= chunk_size {
            chunks.push(ChunkTask {
                file_idx,
                start: data_start,
                end: file_len,
            });
            continue;
        }
        let mut start = data_start;
        let file_ref = &files[file_idx];
        while start < file_len {
            let target = start.saturating_add(chunk_size);
            if target >= file_len {
                chunks.push(ChunkTask {
                    file_idx,
                    start,
                    end: file_len,
                });
                break;
            }
            let end = find_next_newline_after(file_ref, target, file_len)?;
            chunks.push(ChunkTask {
                file_idx,
                start,
                end,
            });
            start = end;
        }
    }
    chunks.sort_unstable_by(|a, b| (b.end - b.start).cmp(&(a.end - a.start)));
    Ok((files, chunks))
}

#[inline(always)]
fn dispatch_row<const N: usize, F>(
    buf: &[u8],
    line_start: usize,
    field_ends: &[usize; N],
    line_has_quote: bool,
    on_row: &mut F,
) where
    F: FnMut([&[u8]; N]),
{
    let mut fields: [&[u8]; N] = [&[]; N];
    let mut s = line_start;
    if !line_has_quote {
        let mut i = 0;
        while i < N {
            let e = field_ends[i];
            fields[i] = &buf[s..e];
            s = e + 1;
            i += 1;
        }
    } else {
        let mut i = 0;
        while i < N {
            let e = field_ends[i];
            let raw = &buf[s..e];
            match unquote_field(raw) {
                Some(clean) => fields[i] = clean,
                None => return,
            }
            s = e + 1;
            i += 1;
        }
    }
    on_row(fields);
}

/// Scans `buf[..len]` (`buf.len() >= len + 64` for safe SIMD over-read).
/// Splits strictly on `\n` and `,`, enforcing `N` fields and valid quoting.
#[inline(always)]
pub fn scan_csv_chunk<const N: usize, F>(buf: &[u8], len: usize, mut on_row: F)
where
    F: FnMut([&[u8]; N]),
{
    debug_assert!(buf.len() >= len + 64);
    if len == 0 {
        return;
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("avx512bw") {
            unsafe {
                scan_csv_avx512::<N, F>(buf, len, &mut on_row);
            }
            return;
        }
        if std::is_x86_feature_detected!("avx2") {
            unsafe {
                scan_csv_avx2::<N, F>(buf, len, &mut on_row);
            }
            return;
        }
    }

    scan_csv_swar::<N, F>(buf, len, &mut on_row);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn scan_csv_avx512<const N: usize, F>(buf: &[u8], len: usize, on_row: &mut F)
where
    F: FnMut([&[u8]; N]),
{
    use std::arch::x86_64::*;

    let ptr = buf.as_ptr();
    let v_nl = _mm512_set1_epi8(b'\n' as i8);
    let v_comma = _mm512_set1_epi8(b',' as i8);
    let v_quote = _mm512_set1_epi8(b'"' as i8);

    let mut line_start = 0usize;
    let mut field_idx = 0usize;
    let mut field_ends = [0usize; N];
    let mut line_has_quote = false;

    let mut offset = 0usize;
    while offset < len {
        let (mut nl_mask, mut comma_mask, mut quote_mask) = unsafe {
            let chunk = _mm512_loadu_si512(ptr.add(offset) as *const __m512i);
            (
                _mm512_cmpeq_epi8_mask(chunk, v_nl),
                _mm512_cmpeq_epi8_mask(chunk, v_comma),
                _mm512_cmpeq_epi8_mask(chunk, v_quote),
            )
        };

        let rem = len - offset;
        if rem < 64 {
            let valid = (1u64 << rem) - 1;
            nl_mask &= valid;
            comma_mask &= valid;
            quote_mask &= valid;
        }

        if quote_mask != 0 {
            line_has_quote = true;
        }

        let mut delim_mask = nl_mask | comma_mask;
        while delim_mask != 0 {
            let bit = delim_mask.trailing_zeros() as usize;
            let pos = offset + bit;
            if ((nl_mask >> bit) & 1) != 0 {
                if field_idx == N - 1 {
                    field_ends[N - 1] = pos;
                    dispatch_row::<N, F>(buf, line_start, &field_ends, line_has_quote, on_row);
                }
                line_start = pos + 1;
                field_idx = 0;
                line_has_quote = if bit < 63 {
                    (quote_mask >> (bit + 1)) != 0
                } else {
                    false
                };
            } else {
                if field_idx < N {
                    field_ends[field_idx] = pos;
                }
                field_idx += 1;
            }
            delim_mask &= delim_mask - 1;
        }

        offset += 64;
    }

    if line_start < len && field_idx == N - 1 {
        field_ends[N - 1] = len;
        dispatch_row::<N, F>(buf, line_start, &field_ends, line_has_quote, on_row);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn scan_csv_avx2<const N: usize, F>(buf: &[u8], len: usize, on_row: &mut F)
where
    F: FnMut([&[u8]; N]),
{
    use std::arch::x86_64::*;

    let ptr = buf.as_ptr();
    let v_nl = _mm256_set1_epi8(b'\n' as i8);
    let v_comma = _mm256_set1_epi8(b',' as i8);
    let v_quote = _mm256_set1_epi8(b'"' as i8);

    let mut line_start = 0usize;
    let mut field_idx = 0usize;
    let mut field_ends = [0usize; N];
    let mut line_has_quote = false;

    let mut offset = 0usize;
    while offset < len {
        let (mut nl_mask, mut comma_mask, mut quote_mask) = unsafe {
            let c0 = _mm256_loadu_si256(ptr.add(offset) as *const __m256i);
            let c1 = _mm256_loadu_si256(ptr.add(offset + 32) as *const __m256i);

            let nl0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(c0, v_nl)) as u32 as u64;
            let nl1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(c1, v_nl)) as u32 as u64;

            let cm0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(c0, v_comma)) as u32 as u64;
            let cm1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(c1, v_comma)) as u32 as u64;

            let qt0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(c0, v_quote)) as u32 as u64;
            let qt1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(c1, v_quote)) as u32 as u64;

            (nl0 | (nl1 << 32), cm0 | (cm1 << 32), qt0 | (qt1 << 32))
        };

        let rem = len - offset;
        if rem < 64 {
            let valid = (1u64 << rem) - 1;
            nl_mask &= valid;
            comma_mask &= valid;
            quote_mask &= valid;
        }

        if quote_mask != 0 {
            line_has_quote = true;
        }

        let mut delim_mask = nl_mask | comma_mask;
        while delim_mask != 0 {
            let bit = delim_mask.trailing_zeros() as usize;
            let pos = offset + bit;
            if ((nl_mask >> bit) & 1) != 0 {
                if field_idx == N - 1 {
                    field_ends[N - 1] = pos;
                    dispatch_row::<N, F>(buf, line_start, &field_ends, line_has_quote, on_row);
                }
                line_start = pos + 1;
                field_idx = 0;
                line_has_quote = if bit < 63 {
                    (quote_mask >> (bit + 1)) != 0
                } else {
                    false
                };
            } else {
                if field_idx < N {
                    field_ends[field_idx] = pos;
                }
                field_idx += 1;
            }
            delim_mask &= delim_mask - 1;
        }

        offset += 64;
    }

    if line_start < len && field_idx == N - 1 {
        field_ends[N - 1] = len;
        dispatch_row::<N, F>(buf, line_start, &field_ends, line_has_quote, on_row);
    }
}

#[inline(always)]
fn has_eq_mask(word: u64, byte_splat: u64) -> u64 {
    let x = word ^ byte_splat;
    let m = (x.wrapping_sub(0x0101_0101_0101_0101)) & !x & 0x8080_8080_8080_8080;
    m
}

fn scan_csv_swar<const N: usize, F>(buf: &[u8], len: usize, on_row: &mut F)
where
    F: FnMut([&[u8]; N]),
{
    const SPLAT_NL: u64 = 0x0A0A_0A0A_0A0A_0A0A;
    const SPLAT_COMMA: u64 = 0x2C2C_2C2C_2C2C_2C2C;
    const SPLAT_QUOTE: u64 = 0x2222_2222_2222_2222;

    let mut line_start = 0usize;
    let mut field_idx = 0usize;
    let mut field_ends = [0usize; N];
    let mut line_has_quote = false;

    let mut offset = 0usize;
    while offset < len {
        let word = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
        let mut nl_mask = has_eq_mask(word, SPLAT_NL);
        let mut comma_mask = has_eq_mask(word, SPLAT_COMMA);
        let mut quote_mask = has_eq_mask(word, SPLAT_QUOTE);

        let rem = len - offset;
        if rem < 8 {
            let valid = (1u64 << (rem * 8)) - 1;
            nl_mask &= valid;
            comma_mask &= valid;
            quote_mask &= valid;
        }

        if quote_mask != 0 {
            line_has_quote = true;
        }

        let mut delim_mask = nl_mask | comma_mask;
        while delim_mask != 0 {
            let bit_msb = delim_mask.trailing_zeros() as usize;
            let byte_idx = bit_msb >> 3;
            let pos = offset + byte_idx;
            if ((nl_mask >> bit_msb) & 1) != 0 {
                if field_idx == N - 1 {
                    field_ends[N - 1] = pos;
                    dispatch_row::<N, F>(buf, line_start, &field_ends, line_has_quote, on_row);
                }
                line_start = pos + 1;
                field_idx = 0;
                line_has_quote = if bit_msb < 63 {
                    (quote_mask >> (bit_msb + 1)) != 0
                } else {
                    false
                };
            } else {
                if field_idx < N {
                    field_ends[field_idx] = pos;
                }
                field_idx += 1;
            }
            delim_mask &= delim_mask - 1;
        }

        offset += 8;
    }

    if line_start < len && field_idx == N - 1 {
        field_ends[N - 1] = len;
        dispatch_row::<N, F>(buf, line_start, &field_ends, line_has_quote, on_row);
    }
}

pub fn list_csv_files(dir: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    if !dir.is_dir() {
        return Ok(files);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.extension().and_then(|e| e.to_str()) == Some("csv") && p.is_file() {
            files.push(p);
        }
    }
    files.sort();
    Ok(files)
}

pub fn read_whole_csv<const N: usize, F>(path: &Path, on_row: F) -> io::Result<()>
where
    F: FnMut([&[u8]; N]),
{
    let mut data = std::fs::read(path)?;
    let first_nl = match data.iter().position(|&b| b == b'\n') {
        Some(pos) => pos + 1,
        None => return Ok(()),
    };
    if first_nl >= data.len() {
        return Ok(());
    }
    let body_len = data.len() - first_nl;
    data.resize(data.len() + 64, 0);
    scan_csv_chunk::<N, F>(&data[first_nl..], body_len, on_row);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_dialect_edge_cases() {
        let raw = b"a,b\n\"u1\",u2\nu1,u2,u3\nu1,\"u2\"x\n\n\"\",v\r\nno_nl_last,ok";
        let mut padded = raw.to_vec();
        let len = padded.len();
        padded.resize(len + 64, 0);
        let mut rows_simd = Vec::new();
        scan_csv_chunk::<2, _>(&padded, len, |f| {
            rows_simd.push((f[0].to_vec(), f[1].to_vec()));
        });
        let mut rows_swar = Vec::new();
        scan_csv_swar::<2, _>(&padded, len, &mut |f| {
            rows_swar.push((f[0].to_vec(), f[1].to_vec()));
        });
        let expected = vec![
            (b"a".to_vec(), b"b".to_vec()),
            (b"u1".to_vec(), b"u2".to_vec()),
            (b"".to_vec(), b"v\r".to_vec()),
            (b"no_nl_last".to_vec(), b"ok".to_vec()),
        ];
        assert_eq!(rows_simd, expected);
        assert_eq!(rows_swar, expected);
    }
}
