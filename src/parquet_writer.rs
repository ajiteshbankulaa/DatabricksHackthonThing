use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::Instant;

#[derive(Default, Clone, Copy)]
pub struct ParquetStats {
    pub user_ns: u64,
    pub campaign_ns: u64,
    pub user_daily_rows: usize,
    pub campaign_daily_rows: usize,
}

use crate::id::append_id_bytes;
use crate::lookup::{Lookups, NON_PURCHASE_CAT};
use crate::session::{CampaignDayStat, UserDailyBuffer};

// Thrift Compact Protocol types
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

// Parquet enums
const TYPE_INT32: i32 = 1;
const TYPE_INT64: i32 = 2;
const TYPE_BYTE_ARRAY: i32 = 6;

const REP_REQUIRED: i32 = 0;
const REP_OPTIONAL: i32 = 1;

const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;

const CODEC_UNCOMPRESSED: i32 = 0;
const PAGE_DATA: i32 = 0;

const CTYPE_UTF8: i32 = 0;
const CTYPE_DATE: i32 = 6;

struct ThriftWriter {
    buf: Vec<u8>,
}

impl ThriftWriter {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
        }
    }

    #[inline(always)]
    fn write_varint(&mut self, mut n: u64) {
        while n >= 0x80 {
            self.buf.push(((n as u8) & 0x7F) | 0x80);
            n >>= 7;
        }
        self.buf.push(n as u8);
    }

    #[inline(always)]
    fn write_i32(&mut self, v: i32) {
        let zz = ((v << 1) ^ (v >> 31)) as u32;
        self.write_varint(zz as u64);
    }

    #[inline(always)]
    fn write_i64(&mut self, v: i64) {
        let zz = ((v << 1) ^ (v >> 63)) as u64;
        self.write_varint(zz);
    }

    #[inline(always)]
    fn write_field_header(&mut self, last_id: &mut i16, id: i16, ty: u8) {
        let delta = id - *last_id;
        if delta > 0 && delta <= 15 {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            self.write_i32(id as i32);
        }
        *last_id = id;
    }

    #[inline(always)]
    fn write_stop(&mut self) {
        self.buf.push(0);
    }

    #[inline(always)]
    fn write_binary(&mut self, bytes: &[u8]) {
        self.write_varint(bytes.len() as u64);
        self.buf.extend_from_slice(bytes);
    }

    #[inline(always)]
    fn write_list_header(&mut self, elem_ty: u8, len: usize) {
        if len < 15 {
            self.buf.push(((len as u8) << 4) | elem_ty);
        } else {
            self.buf.push(0xF0 | elem_ty);
            self.write_varint(len as u64);
        }
    }
}

struct ColSchema {
    name: &'static [u8],
    pq_type: i32,
    rep: i32,
    converted_type: Option<i32>,
}

struct ChunkMeta {
    name: &'static [u8],
    pq_type: i32,
    optional: bool,
    num_values: i64,
    total_size: i64,
    data_page_offset: i64,
}

fn write_schema_element_root(tw: &mut ThriftWriter, num_children: i32) {
    let mut last = 0i16;
    tw.write_field_header(&mut last, 4, T_BINARY);
    tw.write_binary(b"schema");
    tw.write_field_header(&mut last, 5, T_I32);
    tw.write_i32(num_children);
    tw.write_stop();
}

fn write_schema_element_leaf(tw: &mut ThriftWriter, col: &ColSchema) {
    let mut last = 0i16;
    tw.write_field_header(&mut last, 1, T_I32);
    tw.write_i32(col.pq_type);
    tw.write_field_header(&mut last, 3, T_I32);
    tw.write_i32(col.rep);
    tw.write_field_header(&mut last, 4, T_BINARY);
    tw.write_binary(col.name);
    if let Some(ct) = col.converted_type {
        tw.write_field_header(&mut last, 6, T_I32);
        tw.write_i32(ct);
    }
    tw.write_stop();
}

fn build_data_page_header(num_values: i32, uncompressed_size: i32) -> Vec<u8> {
    let mut tw = ThriftWriter::new();
    let mut last = 0i16;
    tw.write_field_header(&mut last, 1, T_I32);
    tw.write_i32(PAGE_DATA);
    tw.write_field_header(&mut last, 2, T_I32);
    tw.write_i32(uncompressed_size);
    tw.write_field_header(&mut last, 3, T_I32);
    tw.write_i32(uncompressed_size);
    tw.write_field_header(&mut last, 5, T_STRUCT);
    // Nested DataPageHeader
    {
        let mut dlast = 0i16;
        tw.write_field_header(&mut dlast, 1, T_I32);
        tw.write_i32(num_values);
        tw.write_field_header(&mut dlast, 2, T_I32);
        tw.write_i32(ENC_PLAIN);
        tw.write_field_header(&mut dlast, 3, T_I32);
        tw.write_i32(ENC_RLE);
        tw.write_field_header(&mut dlast, 4, T_I32);
        tw.write_i32(ENC_RLE);
        tw.write_stop();
    }
    tw.write_stop();
    tw.buf
}

fn write_file_metadata(
    schema: &[ColSchema],
    num_rows: i64,
    chunks: &[ChunkMeta],
    row_group_bytes: i64,
) -> Vec<u8> {
    let mut tw = ThriftWriter::new();
    let mut last = 0i16;

    // 1: version = 1
    tw.write_field_header(&mut last, 1, T_I32);
    tw.write_i32(1);

    // 2: schema
    tw.write_field_header(&mut last, 2, T_LIST);
    tw.write_list_header(T_STRUCT, 1 + schema.len());
    write_schema_element_root(&mut tw, schema.len() as i32);
    for col in schema {
        write_schema_element_leaf(&mut tw, col);
    }

    // 3: num_rows
    tw.write_field_header(&mut last, 3, T_I64);
    tw.write_i64(num_rows);

    // 4: row_groups
    tw.write_field_header(&mut last, 4, T_LIST);
    if num_rows == 0 {
        tw.write_list_header(T_STRUCT, 0);
    } else {
        tw.write_list_header(T_STRUCT, 1);
        let mut rlast = 0i16;
        // 1: columns
        tw.write_field_header(&mut rlast, 1, T_LIST);
        tw.write_list_header(T_STRUCT, chunks.len());
        for ch in chunks {
            let mut clast = 0i16;
            tw.write_field_header(&mut clast, 2, T_I64);
            tw.write_i64(ch.data_page_offset);
            tw.write_field_header(&mut clast, 3, T_STRUCT);
            // ColumnMetaData
            {
                let mut mlast = 0i16;
                tw.write_field_header(&mut mlast, 1, T_I32);
                tw.write_i32(ch.pq_type);
                tw.write_field_header(&mut mlast, 2, T_LIST);
                if ch.optional {
                    tw.write_list_header(T_I32, 2);
                    tw.write_i32(ENC_RLE);
                    tw.write_i32(ENC_PLAIN);
                } else {
                    tw.write_list_header(T_I32, 1);
                    tw.write_i32(ENC_PLAIN);
                }
                tw.write_field_header(&mut mlast, 3, T_LIST);
                tw.write_list_header(T_BINARY, 1);
                tw.write_binary(ch.name);
                tw.write_field_header(&mut mlast, 4, T_I32);
                tw.write_i32(CODEC_UNCOMPRESSED);
                tw.write_field_header(&mut mlast, 5, T_I64);
                tw.write_i64(ch.num_values);
                tw.write_field_header(&mut mlast, 6, T_I64);
                tw.write_i64(ch.total_size);
                tw.write_field_header(&mut mlast, 7, T_I64);
                tw.write_i64(ch.total_size);
                tw.write_field_header(&mut mlast, 9, T_I64);
                tw.write_i64(ch.data_page_offset);
                tw.write_stop();
            }
            tw.write_stop();
        }
        // 2: total_byte_size
        tw.write_field_header(&mut rlast, 2, T_I64);
        tw.write_i64(row_group_bytes);
        // 3: num_rows
        tw.write_field_header(&mut rlast, 3, T_I64);
        tw.write_i64(num_rows);
        tw.write_stop();
    }

    // 6: created_by
    tw.write_field_header(&mut last, 6, T_BINARY);
    tw.write_binary(b"shopping-sessions");

    tw.write_stop();
    tw.buf
}

fn write_column_page<W: Write>(
    w: &mut W,
    file_pos: &mut i64,
    name: &'static [u8],
    pq_type: i32,
    optional: bool,
    num_rows: i64,
    payload: &[u8],
) -> io::Result<ChunkMeta> {
    let header = build_data_page_header(num_rows as i32, payload.len() as i32);
    let data_page_offset = *file_pos;
    w.write_all(&header)?;
    w.write_all(payload)?;
    let total_size = (header.len() + payload.len()) as i64;
    *file_pos += total_size;
    Ok(ChunkMeta {
        name,
        pq_type,
        optional,
        num_values: num_rows,
        total_size,
        data_page_offset,
    })
}

#[inline(always)]
fn slice_as_le_bytes<T>(slice: &[T]) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(slice.as_ptr() as *const u8, std::mem::size_of_val(slice))
    }
}

#[inline(always)]
fn push_byte_array(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s);
}

fn encode_def_levels_bitpacked_1bit(cats: &[u8], out: &mut Vec<u8>) {
    // Reserve 4 bytes for RLE stream length
    let len_pos = out.len();
    out.extend_from_slice(&0i32.to_le_bytes());

    let mut i = 0usize;
    let n = cats.len();
    while i < n {
        // Up to 63 groups of 8 = 504 values per bit-packed run (compatible with all readers)
        let chunk_rows = (n - i).min(504);
        let num_groups = chunk_rows.div_ceil(8);
        let header = ((num_groups as u32) << 1) | 1;
        // Write varint header (num_groups <= 63 -> always 1 byte <= 127)
        out.push(header as u8);

        let mut j = 0usize;
        while j < chunk_rows {
            let mut byte = 0u8;
            let limit = (chunk_rows - j).min(8);
            for b in 0..limit {
                if cats[i + j + b] != NON_PURCHASE_CAT {
                    byte |= 1u8 << b;
                }
            }
            out.push(byte);
            j += 8;
        }
        i += chunk_rows;
    }

    let rle_len = (out.len() - len_pos - 4) as i32;
    out[len_pos..len_pos + 4].copy_from_slice(&rle_len.to_le_bytes());
}

const USER_DAILY_SCHEMA: [ColSchema; 6] = [
    ColSchema {
        name: b"user_id",
        pq_type: TYPE_BYTE_ARRAY,
        rep: REP_REQUIRED,
        converted_type: Some(CTYPE_UTF8),
    },
    ColSchema {
        name: b"day",
        pq_type: TYPE_INT32,
        rep: REP_REQUIRED,
        converted_type: Some(CTYPE_DATE),
    },
    ColSchema {
        name: b"sessions",
        pq_type: TYPE_INT64,
        rep: REP_REQUIRED,
        converted_type: None,
    },
    ColSchema {
        name: b"events",
        pq_type: TYPE_INT64,
        rep: REP_REQUIRED,
        converted_type: None,
    },
    ColSchema {
        name: b"revenue_cents",
        pq_type: TYPE_INT64,
        rep: REP_REQUIRED,
        converted_type: None,
    },
    ColSchema {
        name: b"top_category",
        pq_type: TYPE_BYTE_ARRAY,
        rep: REP_OPTIONAL,
        converted_type: Some(CTYPE_UTF8),
    },
];

const CAMPAIGN_DAILY_SCHEMA: [ColSchema; 7] = [
    ColSchema {
        name: b"campaign_id",
        pq_type: TYPE_BYTE_ARRAY,
        rep: REP_REQUIRED,
        converted_type: Some(CTYPE_UTF8),
    },
    ColSchema {
        name: b"channel",
        pq_type: TYPE_BYTE_ARRAY,
        rep: REP_REQUIRED,
        converted_type: Some(CTYPE_UTF8),
    },
    ColSchema {
        name: b"day",
        pq_type: TYPE_INT32,
        rep: REP_REQUIRED,
        converted_type: Some(CTYPE_DATE),
    },
    ColSchema {
        name: b"sessions",
        pq_type: TYPE_INT64,
        rep: REP_REQUIRED,
        converted_type: None,
    },
    ColSchema {
        name: b"purchasing_sessions",
        pq_type: TYPE_INT64,
        rep: REP_REQUIRED,
        converted_type: None,
    },
    ColSchema {
        name: b"revenue_cents",
        pq_type: TYPE_INT64,
        rep: REP_REQUIRED,
        converted_type: None,
    },
    ColSchema {
        name: b"assisted_sessions",
        pq_type: TYPE_INT64,
        rep: REP_REQUIRED,
        converted_type: None,
    },
];

pub fn write_user_daily_part(
    path: &Path,
    buf: &UserDailyBuffer,
    lookups: &Lookups,
) -> io::Result<()> {
    let file = File::create(path)?;
    let mut w = BufWriter::with_capacity(1 << 20, file);
    w.write_all(b"PAR1")?;
    let mut file_pos = 4i64;
    let num_rows = buf.len() as i64;
    let mut chunks = Vec::new();
    let mut row_group_bytes = 0i64;

    if num_rows > 0 {
        // 1. user_id
        let mut uid_bytes = Vec::with_capacity(buf.len() * 14);
        let mut tmp_id = Vec::with_capacity(16);
        for &uid in &buf.user_ids {
            tmp_id.clear();
            append_id_bytes(uid, &mut tmp_id);
            push_byte_array(&mut uid_bytes, &tmp_id);
        }
        let c0 = write_column_page(
            &mut w,
            &mut file_pos,
            b"user_id",
            TYPE_BYTE_ARRAY,
            false,
            num_rows,
            &uid_bytes,
        )?;
        row_group_bytes += c0.total_size;
        chunks.push(c0);
        drop(uid_bytes);

        // 2. day (INT32 DATE)
        let c1 = write_column_page(
            &mut w,
            &mut file_pos,
            b"day",
            TYPE_INT32,
            false,
            num_rows,
            slice_as_le_bytes(&buf.days),
        )?;
        row_group_bytes += c1.total_size;
        chunks.push(c1);

        // 3. sessions
        let c2 = write_column_page(
            &mut w,
            &mut file_pos,
            b"sessions",
            TYPE_INT64,
            false,
            num_rows,
            slice_as_le_bytes(&buf.sessions),
        )?;
        row_group_bytes += c2.total_size;
        chunks.push(c2);

        // 4. events
        let c3 = write_column_page(
            &mut w,
            &mut file_pos,
            b"events",
            TYPE_INT64,
            false,
            num_rows,
            slice_as_le_bytes(&buf.events),
        )?;
        row_group_bytes += c3.total_size;
        chunks.push(c3);

        // 5. revenue_cents
        let c4 = write_column_page(
            &mut w,
            &mut file_pos,
            b"revenue_cents",
            TYPE_INT64,
            false,
            num_rows,
            slice_as_le_bytes(&buf.revenue_cents),
        )?;
        row_group_bytes += c4.total_size;
        chunks.push(c4);

        // 6. top_category (OPTIONAL BYTE_ARRAY)
        let mut cat_payload = Vec::with_capacity(buf.len() / 2 + 1024);
        encode_def_levels_bitpacked_1bit(&buf.top_category, &mut cat_payload);
        for &cid in &buf.top_category {
            if cid != NON_PURCHASE_CAT {
                push_byte_array(&mut cat_payload, &lookups.categories[cid as usize]);
            }
        }
        let c5 = write_column_page(
            &mut w,
            &mut file_pos,
            b"top_category",
            TYPE_BYTE_ARRAY,
            true,
            num_rows,
            &cat_payload,
        )?;
        row_group_bytes += c5.total_size;
        chunks.push(c5);
    }

    let meta = write_file_metadata(&USER_DAILY_SCHEMA, num_rows, &chunks, row_group_bytes);
    w.write_all(&meta)?;
    w.write_all(&(meta.len() as i32).to_le_bytes())?;
    w.write_all(b"PAR1")?;
    w.flush()?;
    Ok(())
}

pub fn write_campaign_daily(
    path: &Path,
    stats: &[CampaignDayStat],
    lookups: &Lookups,
) -> io::Result<usize> {
    let num_campaigns = lookups.campaigns.len();
    let num_days = lookups.num_days;
    let start_epoch_day = lookups.start_epoch_day;

    let mut camp_id_bytes = Vec::new();
    let mut channel_bytes = Vec::new();
    let mut days = Vec::new();
    let mut sessions = Vec::new();
    let mut purchasing_sessions = Vec::new();
    let mut revenue_cents = Vec::new();
    let mut assisted_sessions = Vec::new();

    for c in 0..num_campaigns {
        let info = &lookups.campaigns[c];
        for d in 0..num_days {
            let st = stats[c * num_days + d];
            if st.sessions > 0 || st.assisted_sessions > 0 {
                push_byte_array(&mut camp_id_bytes, &info.id);
                push_byte_array(&mut channel_bytes, &info.channel);
                days.push(start_epoch_day + d as i32);
                sessions.push(st.sessions);
                purchasing_sessions.push(st.purchasing_sessions);
                revenue_cents.push(st.revenue_cents);
                assisted_sessions.push(st.assisted_sessions);
            }
        }
    }

    let file = File::create(path)?;
    let mut w = BufWriter::with_capacity(1 << 20, file);
    w.write_all(b"PAR1")?;
    let mut file_pos = 4i64;
    let num_rows = days.len() as i64;
    let mut chunks = Vec::new();
    let mut row_group_bytes = 0i64;

    if num_rows > 0 {
        let c0 = write_column_page(
            &mut w,
            &mut file_pos,
            b"campaign_id",
            TYPE_BYTE_ARRAY,
            false,
            num_rows,
            &camp_id_bytes,
        )?;
        row_group_bytes += c0.total_size;
        chunks.push(c0);

        let c1 = write_column_page(
            &mut w,
            &mut file_pos,
            b"channel",
            TYPE_BYTE_ARRAY,
            false,
            num_rows,
            &channel_bytes,
        )?;
        row_group_bytes += c1.total_size;
        chunks.push(c1);

        let c2 = write_column_page(
            &mut w,
            &mut file_pos,
            b"day",
            TYPE_INT32,
            false,
            num_rows,
            slice_as_le_bytes(&days),
        )?;
        row_group_bytes += c2.total_size;
        chunks.push(c2);

        let c3 = write_column_page(
            &mut w,
            &mut file_pos,
            b"sessions",
            TYPE_INT64,
            false,
            num_rows,
            slice_as_le_bytes(&sessions),
        )?;
        row_group_bytes += c3.total_size;
        chunks.push(c3);

        let c4 = write_column_page(
            &mut w,
            &mut file_pos,
            b"purchasing_sessions",
            TYPE_INT64,
            false,
            num_rows,
            slice_as_le_bytes(&purchasing_sessions),
        )?;
        row_group_bytes += c4.total_size;
        chunks.push(c4);

        let c5 = write_column_page(
            &mut w,
            &mut file_pos,
            b"revenue_cents",
            TYPE_INT64,
            false,
            num_rows,
            slice_as_le_bytes(&revenue_cents),
        )?;
        row_group_bytes += c5.total_size;
        chunks.push(c5);

        let c6 = write_column_page(
            &mut w,
            &mut file_pos,
            b"assisted_sessions",
            TYPE_INT64,
            false,
            num_rows,
            slice_as_le_bytes(&assisted_sessions),
        )?;
        row_group_bytes += c6.total_size;
        chunks.push(c6);
    }

    let meta = write_file_metadata(&CAMPAIGN_DAILY_SCHEMA, num_rows, &chunks, row_group_bytes);
    w.write_all(&meta)?;
    w.write_all(&(meta.len() as i32).to_le_bytes())?;
    w.write_all(b"PAR1")?;
    w.flush()?;
    Ok(num_rows as usize)
}

pub fn write_all_outputs(
    out_dir: &Path,
    user_buffers: &[UserDailyBuffer],
    campaign_stats: &[CampaignDayStat],
    lookups: &Lookups,
    timing: bool,
) -> io::Result<ParquetStats> {
    let ud_dir = out_dir.join("user_daily");
    let cd_dir = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud_dir)?;
    std::fs::create_dir_all(&cd_dir)?;

    let user_daily_rows: usize = user_buffers.iter().map(|b| b.len()).sum();
    let wall_t0 = if timing { Some(Instant::now()) } else { None };
    let mut campaign_daily_rows = 0usize;
    let mut u_cpu_ns = 0u64;
    let mut c_cpu_ns = 0u64;

    if user_buffers.is_empty() {
        let empty = UserDailyBuffer::default();
        let t_u = if timing { Some(Instant::now()) } else { None };
        write_user_daily_part(&ud_dir.join("part-0.parquet"), &empty, lookups)?;
        if let Some(t) = t_u {
            u_cpu_ns = t.elapsed().as_nanos() as u64;
        }
        let t_c = if timing { Some(Instant::now()) } else { None };
        campaign_daily_rows =
            write_campaign_daily(&cd_dir.join("part-0.parquet"), campaign_stats, lookups)?;
        if let Some(t) = t_c {
            c_cpu_ns = t.elapsed().as_nanos() as u64;
        }
    } else {
        std::thread::scope(|s| -> io::Result<()> {
            let mut u_handles = Vec::with_capacity(user_buffers.len());
            for (i, buf) in user_buffers.iter().enumerate() {
                let p = ud_dir.join(format!("part-{i}.parquet"));
                u_handles.push(s.spawn(move || -> io::Result<u64> {
                    let t = if timing { Some(Instant::now()) } else { None };
                    write_user_daily_part(&p, buf, lookups)?;
                    Ok(t.map(|x| x.elapsed().as_nanos() as u64).unwrap_or(0))
                }));
            }
            let cd_path = cd_dir.join("part-0.parquet");
            let c_handle = s.spawn(move || -> io::Result<(usize, u64)> {
                let t = if timing { Some(Instant::now()) } else { None };
                let rows = write_campaign_daily(&cd_path, campaign_stats, lookups)?;
                Ok((rows, t.map(|x| x.elapsed().as_nanos() as u64).unwrap_or(0)))
            });
            for h in u_handles {
                u_cpu_ns = u_cpu_ns.max(h.join().unwrap()?);
            }
            let (rows, c_ns) = c_handle.join().unwrap()?;
            campaign_daily_rows = rows;
            c_cpu_ns = c_ns;
            Ok(())
        })?;
    }

    let _wall_ns = wall_t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
    Ok(ParquetStats {
        user_ns: u_cpu_ns,
        campaign_ns: c_cpu_ns,
        user_daily_rows,
        campaign_daily_rows,
    })
}
