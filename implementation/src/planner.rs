use crate::config::{Config, QueryWindow};
use std::f64::consts::PI;
use crate::sfc::{
    build_sfc_params,
    ranges_for_window, 
    encode_point,       
    merge_ranges,
    SfcParams,
};



/// Result structure
#[derive(Debug, Clone)]
pub struct PlanResult {
    pub sfc_params: SfcParams,
    pub ranges_raw: Vec<(u64, u64)>,
    pub ranges_merged: Vec<(u64, u64)>,
    pub t_start_s: u64,
    pub t_end_s: u64,
}

/// Convert string/integer time to UTC seconds
fn parse_ts_to_epoch_s(ts_str: &str) -> Option<u64> {
    if let Ok(v) = ts_str.trim().parse::<i64>() { return Some(v.max(0) as u64); }
    if let Ok(dt) = ts_str.parse::<chrono::DateTime<chrono::Utc>>() {
        return Some(dt.timestamp().max(0) as u64);
    }
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(ts_str.trim(), "%Y-%m-%d %H:%M:%S") {
        let dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc);
        return Some(dt.timestamp().max(0) as u64);
    }
    None
}

#[inline]
fn meters_to_deg_lat(m: f64) -> f64 { m / 111_320.0 }
#[inline]
fn meters_to_deg_lon(m: f64, lat_deg: f64) -> f64 {
    let rad = lat_deg * PI / 180.0;
    m / (111_320.0 * rad.cos().max(1e-6))
}

/// Merge intervals using the specified gap
fn merge_ranges_with_gap(mut ranges: Vec<(u64, u64)>, gap: u64) -> Vec<(u64, u64)> {
    if ranges.is_empty() { return ranges; }
    ranges.sort_unstable_by_key(|r| r.0);
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (s, e) in ranges.into_iter() {
        if let Some(last) = out.last_mut() {
            if s <= last.1.saturating_add(gap) {
                if e > last.1 { last.1 = e; }
                continue;
            }
        }
        out.push((s, e));
    }
    out
}

/// Inject bucket intervals for anchor keys and merge again with the gap
fn inject_anchor_buckets_with_tailbits(
    tail_bits: u8,
    mut merged: Vec<(u64, u64)>,
    anchors: &[u64],
    merge_gap_keys: u64,
) -> Vec<(u64, u64)> {
    if anchors.is_empty() { return merged; }
    let span = if tail_bits >= 63 { u64::MAX } else { 1u64 << tail_bits };
    for &k in anchors {
        let b_start = if span == u64::MAX { 0 } else { k & !(span - 1) };
        let b_end   = if span == u64::MAX { u64::MAX } else { b_start.saturating_add(span - 1) };
        merged.push((b_start, b_end));
    }
    merge_ranges_with_gap(merged, merge_gap_keys)
}

/// Provide external SFC parameter construction (delegated to sfc.rs)
pub fn build_params(cfg: &Config) -> SfcParams {
    build_sfc_params(cfg)
}

/// 约束注入：将锚点桶与已合并区间求交后再合并，避免引入与窗口无关的大桶。
fn constrained_inject(
    merged: &[(u64, u64)],
    anchor_buckets: &[(u64, u64)],
) -> Vec<(u64, u64)> {
    let mut out = merged.to_vec();
    for &(bs, be) in anchor_buckets {
        for &(rs, re) in merged.iter() {
            let is = rs.max(bs);
            let ie = re.min(be);
            if is <= ie {
                out.push((is, ie));
            }
        }
    }
    merge_ranges(out)
}

fn build_anchor_points(q: &QueryWindow) -> Vec<(f64, f64)> {
    let lat_c = 0.5 * (q.lat_min + q.lat_max);
    let lon_c = 0.5 * (q.lon_min + q.lon_max);
    vec![
        (lat_c, lon_c),           // 中心
        (q.lat_min, q.lon_min),   // 四角
        (q.lat_min, q.lon_max),
        (q.lat_max, q.lon_min),
        (q.lat_max, q.lon_max),
    ]
}


pub fn plan_window(cfg: &Config, q: &QueryWindow) -> PlanResult {
    // 1) 构建 SFC 参数（注意：build_sfc_params(&Config)）
    let p = build_sfc_params(cfg);

    // 2) 解析时间（如果你已有 parse_ts_to_epoch_s，就用现成的）
    //    若函数名不同，请替换为你的解析函数。这里用 p.gtime.* 做边界兜底。
    let t_start_s = crate::planner::parse_ts_to_epoch_s(&q.t_start).unwrap_or(p.gtime.0);
    let t_end_s   = crate::planner::parse_ts_to_epoch_s(&q.t_end).unwrap_or(p.gtime.1);

    // 3) 常规 SFC 覆盖（原始区间）
    let ranges_raw = ranges_for_window(
        &p,
        q.lat_min, q.lat_max,
        q.lon_min, q.lon_max,
        t_start_s, t_end_s,
    );

    // 4) 常规合并
    let mut ranges_merged = merge_ranges(ranges_raw.clone());

    // 5) 约束注入法
    let stop_tail_bits: u8 = cfg.experiment.stop_tail_bits; // 你的字段是 u8（从报错判断）
    if stop_tail_bits > 0 {
        let anchors = build_anchor_points(q);
        let span: u64 = if stop_tail_bits >= 63 { u64::MAX } else { (1u64 << stop_tail_bits) - 1 };

        // 选用时间维的中位数生成锚点 key（也可用 t_start_s）
        let t_mid = if t_end_s >= t_start_s {
            t_start_s + (t_end_s - t_start_s) / 2
        } else {
            t_start_s
        };

        let mut anchor_buckets: Vec<(u64, u64)> = Vec::with_capacity(anchors.len());
        for (lat, lon) in anchors {
            let key = encode_point(&p, lat, lon, t_mid);
            let b_start = key & !span;
            let b_end   = key |  span;
            anchor_buckets.push((b_start, b_end));
        }

        // 核心：与已有 merged 的交集再合并
        ranges_merged = constrained_inject(&ranges_merged, &anchor_buckets);
    }

    // 6) 软上限截断（如果你有 max_ranges 配置，可以在 cfg 或 p 上取）
    if let Some(maxr) = cfg.experiment.max_ranges.or(p.max_ranges) {
        if ranges_merged.len() > maxr {
            ranges_merged.truncate(maxr);
        }
    }

    PlanResult {
        sfc_params: p,
        ranges_raw,
        ranges_merged,
        t_start_s,
        t_end_s,
    }
}


