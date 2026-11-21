use std::collections::HashMap;
use std::io;
use std::path::Path;

use crate::csv::read_whole_csv;
use crate::id::pack_id;
use crate::timestamp::{DAY_MS, SEVEN_DAYS_MS, parse_ts};

pub const NON_PURCHASE_CAT: u8 = 255;

pub struct CampaignInfo {
    pub id: Vec<u8>,
    pub channel: Vec<u8>,
}

pub struct Lookups {
    pub window_start: i64,
    pub window_end: i64,
    pub click_start: i64,
    pub click_end: i64,
    pub start_epoch_day: i32,
    pub num_days: usize,

    pub categories: Vec<Vec<u8>>,
    pub unknown_cat_id: u8,
    fast_products: Vec<u8>,
    extra_products: HashMap<u64, u8>,

    pub campaigns: Vec<CampaignInfo>,
    fast_campaigns: Vec<u16>,
    extra_campaigns: HashMap<u64, u16>,
}

impl Lookups {
    pub fn load(data_dir: &Path) -> io::Result<Self> {
        let manifest_bytes = std::fs::read(data_dir.join("manifest.json"))?;
        let (window_start, window_end) = parse_manifest(&manifest_bytes)?;
        let click_start = window_start - SEVEN_DAYS_MS;
        let click_end = window_end;
        let start_epoch_day = window_start.div_euclid(DAY_MS) as i32;
        let end_epoch_day = (window_end - 1).div_euclid(DAY_MS) as i32;
        let num_days = (end_epoch_day - start_epoch_day + 1).max(1) as usize;

        // Products
        let mut prod_rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut cat_set: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
        let products_path = data_dir.join("products.csv");
        if products_path.exists() {
            read_whole_csv::<2, _>(&products_path, |f| {
                let pid = f[0];
                let cat = f[1];
                if !pid.is_empty() {
                    prod_rows.push((pid.to_vec(), cat.to_vec()));
                    if !cat_set.iter().any(|c| c.as_slice() == cat) {
                        cat_set.push(cat.to_vec());
                    }
                }
            })?;
        }
        cat_set.sort();
        assert!(cat_set.len() <= 255, "too many distinct product categories");
        let unknown_cat_id = cat_set
            .iter()
            .position(|c| c.as_slice() == b"unknown")
            .unwrap() as u8;

        let mut max_pnum = 0usize;
        for (pid, _) in &prod_rows {
            if let Some(n) = parse_prefixed_num(pid, b"p", 500_000) {
                max_pnum = max_pnum.max(n);
            }
        }
        let mut fast_products = vec![unknown_cat_id; max_pnum + 1];
        let mut extra_products = HashMap::new();
        for (pid, cat) in prod_rows {
            let cid = cat_set
                .iter()
                .position(|c| c.as_slice() == cat.as_slice())
                .unwrap() as u8;
            if let Some(n) = parse_prefixed_num(&pid, b"p", 500_000) {
                fast_products[n] = cid;
            } else {
                extra_products.insert(pack_id(&pid), cid);
            }
        }

        // Campaigns (index 0 = organic)
        let mut campaigns = vec![CampaignInfo {
            id: b"organic".to_vec(),
            channel: b"organic".to_vec(),
        }];
        let mut camp_rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let campaigns_path = data_dir.join("campaigns.csv");
        if campaigns_path.exists() {
            read_whole_csv::<2, _>(&campaigns_path, |f| {
                let cid = f[0];
                let ch = f[1];
                if !cid.is_empty() {
                    camp_rows.push((cid.to_vec(), ch.to_vec()));
                }
            })?;
        }
        let mut max_cnum = 0usize;
        for (cid, _) in &camp_rows {
            if let Some(n) = parse_prefixed_num(cid, b"c_", 200_000) {
                max_cnum = max_cnum.max(n);
            }
        }
        let mut fast_campaigns = vec![0u16; max_cnum + 1];
        let mut extra_campaigns = HashMap::new();
        for (cid, ch) in camp_rows {
            let idx = campaigns.len() as u16;
            if let Some(n) = parse_prefixed_num(&cid, b"c_", 200_000) {
                fast_campaigns[n] = idx;
            } else {
                extra_campaigns.insert(pack_id(&cid), idx);
            }
            campaigns.push(CampaignInfo {
                id: cid,
                channel: ch,
            });
        }

        Ok(Self {
            window_start,
            window_end,
            click_start,
            click_end,
            start_epoch_day,
            num_days,
            categories: cat_set,
            unknown_cat_id,
            fast_products,
            extra_products,
            campaigns,
            fast_campaigns,
            extra_campaigns,
        })
    }

    #[inline(always)]
    pub fn lookup_product(&self, pid: &[u8]) -> u8 {
        if let Some(n) = parse_prefixed_num(pid, b"p", self.fast_products.len().saturating_sub(1)) {
            return unsafe { *self.fast_products.get_unchecked(n) };
        }
        if self.extra_products.is_empty() {
            return self.unknown_cat_id;
        }
        *self
            .extra_products
            .get(&pack_id(pid))
            .unwrap_or(&self.unknown_cat_id)
    }

    #[inline(always)]
    pub fn lookup_campaign(&self, cid: &[u8]) -> u16 {
        if let Some(n) =
            parse_prefixed_num(cid, b"c_", self.fast_campaigns.len().saturating_sub(1))
        {
            return unsafe { *self.fast_campaigns.get_unchecked(n) };
        }
        if self.extra_campaigns.is_empty() {
            return 0;
        }
        *self.extra_campaigns.get(&pack_id(cid)).unwrap_or(&0)
    }
}

#[inline(always)]
fn parse_prefixed_num(s: &[u8], prefix: &[u8], max_val: usize) -> Option<usize> {
    if !s.starts_with(prefix) {
        return None;
    }
    let rest = &s[prefix.len()..];
    if rest.is_empty() || rest.len() > 7 {
        return None;
    }
    // Note: "p01" is NOT the same string as "p1", so reject leading zero!
    if rest[0] == b'0' {
        return None;
    }
    let mut val = 0usize;
    for &b in rest {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + d as usize;
    }
    if val <= max_val {
        Some(val)
    } else {
        None
    }
}

fn parse_manifest(bytes: &[u8]) -> io::Result<(i64, i64)> {
    let ws = extract_json_string(bytes, b"\"window_start\"")
        .and_then(parse_ts)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid window_start"))?;
    let we = extract_json_string(bytes, b"\"window_end\"")
        .and_then(parse_ts)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid window_end"))?;
    Ok((ws, we))
}

fn extract_json_string<'a>(bytes: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let pos = bytes.windows(key.len()).position(|w| w == key)?;
    let rest = &bytes[pos + key.len()..];
    let colon = rest.iter().position(|&b| b == b':')?;
    let after_colon = &rest[colon + 1..];
    let q1 = after_colon.iter().position(|&b| b == b'"')?;
    let value_start = &after_colon[q1 + 1..];
    let q2 = value_start.iter().position(|&b| b == b'"')?;
    Some(&value_start[..q2])
}
