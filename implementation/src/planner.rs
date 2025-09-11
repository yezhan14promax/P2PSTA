use crate::config::{Config, QueryWindow};
use crate::sfc::{SfcParams, build_sfc_params as sfc_build_params, ranges_for_window, encode_point};
use std::f64::consts::PI;

/// Result structure
#[derive(Debug, Clone)]
pub struct PlanResult {
    pub sfc_params: SfcParams,
    pub ranges_raw: Vec<(u64, u64)>,
    pub ranges_merged: Vec<(u64, u64)>,
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
    sfc_build_params(cfg)
}

/// Plan window: round the upper bound and add 5 anchor points (center + four corners)
pub fn plan_window(cfg: &Config, p: &SfcParams, q: &QueryWindow) -> PlanResult {
    // 1) Parse time and adjust for closed right interval
    let t_start = parse_ts_to_epoch_s(&q.t_start)
        .unwrap_or_else(|| panic!("bad t_start: {}", q.t_start));
    let t_end_raw = parse_ts_to_epoch_s(&q.t_end)
        .unwrap_or_else(|| panic!("bad t_end: {}", q.t_end));
    // Time precision comes from cfg.sfc (consistent with encoding precision)
    let t_prec = cfg.sfc.t_precision_s.max(1);
    let t_end = t_end_raw.saturating_add((t_prec - 1) as u64); // closed right interval

    // 2) Expand upper bound of latitude and longitude by half a cell (equivalent to ceil()-1)
    let lat_c = (q.lat_min + q.lat_max) * 0.5;
    let half_cell_lat = meters_to_deg_lat(cfg.sfc.y_precision_m.max(1e-6)) * 0.5;
    let half_cell_lon = meters_to_deg_lon(cfg.sfc.x_precision_m.max(1e-6), lat_c) * 0.5;

    let lat_min = q.lat_min;
    let lon_min = q.lon_min;
    let lat_max = (q.lat_max + half_cell_lat).min(90.0);
    let lon_max = (q.lon_max + half_cell_lon).min(180.0);

    // 3) Generate intervals using the original algorithm and merge once
    let ranges_raw = ranges_for_window(p, lat_min, lat_max, lon_min, lon_max, t_start, t_end);
    let mut ranges_merged = merge_ranges_with_gap(ranges_raw.clone(), cfg.experiment.merge_gap_keys as u64);

    // 4) Inject 5 anchor points (center + four corners)
    let t_mid = ((t_start as u128 + t_end as u128) / 2) as u64;
    let anchors = [
        encode_point(p, (lat_min + lat_max) * 0.5, (lon_min + lon_max) * 0.5, t_mid),
        encode_point(p, lat_min, lon_min, t_start),
        encode_point(p, lat_min, lon_max, t_start),
        encode_point(p, lat_max, lon_min, t_end),
        encode_point(p, lat_max, lon_max, t_end),
    ];
    ranges_merged = inject_anchor_buckets_with_tailbits(
        cfg.experiment.stop_tail_bits as u8,       // Align with node bucket bits
        ranges_merged,
        &anchors,
        cfg.experiment.merge_gap_keys as u64,
    );

    // 5) Limit max ranges (experiment takes precedence, otherwise use upper bound from SFC parameters)
    if let Some(maxr) = cfg.experiment.max_ranges.or(p.max_ranges) {
        if ranges_merged.len() > maxr {
            ranges_merged.truncate(maxr);
        }
    }

    PlanResult {
        sfc_params: p.clone(),
        ranges_raw,
        ranges_merged,
    }
}
