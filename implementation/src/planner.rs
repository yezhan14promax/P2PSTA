use crate::config::{Config, QueryWindow,debug_enabled};
use crate::sfc;
use crate::sfc::{SfcParams, encode_point, encode_point_z3, ranges_for_window, merge_ranges, build_sfc_params};

// ====================== Debug switches and small helpers ======================

// fn debug_enabled(cfg: &Config) -> bool {
//     if let Some(b) = cfg.experiment.debug { return b; }
//     std::env::var("P2PSTA_DEBUG")
//         .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
//         .unwrap_or(false)
// }

fn key_covered(k: u64, ranges: &[(u64,u64)]) -> bool {
    for (s,e) in ranges { if *s <= k && k <= *e { return true; } }
    false
}

fn probe_time_coverage(
    p: &SfcParams,
    lat_min: f64, lat_max: f64, lon_min: f64, lon_max: f64,
    t_start_s: u64, t_end_s: u64,
    ranges_merged: &[(u64,u64)],
) {
    let lat_c = 0.5*(lat_min + lat_max);
    let lon_c = 0.5*(lon_min + lon_max);
    let anchors = [
        (lat_c, lon_c, "center"),
        (lat_min, lon_min, "SW"),
        (lat_min, lon_max, "SE"),
        (lat_max, lon_min, "NW"),
        (lat_max, lon_max, "NE"),
    ];

    println!(">>> [probe] time-edge coverage (t_start / t_end-ε):");
    for &(la, lo, name) in &anchors {
        let k0 = encode_point(p, la, lo, t_start_s);
        let k1 = encode_point(p, la, lo, t_end_s.saturating_sub(1));
        let c0 = key_covered(k0, ranges_merged);
        let c1 = key_covered(k1, ranges_merged);
        println!("    - {:>6}: t_start={} covered={}, t_end-ε={} covered={}",
            name, t_start_s, c0, t_end_s.saturating_sub(1), c1);
    }
}

fn probe_time_quantization(p: &SfcParams, t_start_s: u64, t_end_s: u64) {
    let mn = p.gtime.0;
    let mx = p.gtime.1;
    let L  = p.bits.lt;
    let bins = (1u64 << L) - 1;
    let width = (mx - mn) as f64 / bins as f64;
    let z = |v:u64| {
        let t = (v.saturating_sub(mn) as f64 / (mx - mn) as f64).clamp(0.0, 1.0);
        (t * bins as f64).round() as u64
    };
    println!(
        ">>> [probe] time quantization: gtime=[{},{}], lt={}, bin_width≈{:.2}s, z(t_start)={}, z(t_end-ε)={}",
        mn, mx, L, width, z(t_start_s), z(t_end_s.saturating_sub(1))
    );
}

fn scan_time_coverage(
    p: &SfcParams,
    lat_c: f64, lon_c: f64, t0: u64, t1: u64, step_s: u64,
    ranges_merged: &[(u64,u64)],
) {
    let mut covered = 0usize;
    let mut total = 0usize;
    let mut first_false: Option<u64> = None;
    let mut t = t0;
    while t < t1 {
        let k = encode_point(p, lat_c, lon_c, t);
        let c = key_covered(k, ranges_merged);
        total += 1;
        if c { covered += 1; } else if first_false.is_none() { first_false = Some(t); }
        t = t.saturating_add(step_s);
    }
    println!(
        ">>> [probe] time coverage at center: {}/{} (~{:.1}%), first_miss_at={:?}",
        covered, total, 100.0*covered as f64/total as f64, first_false
    );
}

fn probe_key_consistency(p: &SfcParams, q: &QueryWindow, t_start_s: u64, t_end_s: u64) {
    let lat_c = 0.5 * (q.lat_min + q.lat_max);
    let lon_c = 0.5 * (q.lon_min + q.lon_max);

    let anchors = [
        (lat_c, lon_c, "center"),
        (q.lat_min, q.lon_min, "SW"),
        (q.lat_min, q.lon_max, "SE"),
        (q.lat_max, q.lon_min, "NW"),
        (q.lat_max, q.lon_max, "NE"),
    ];

    let times = [
        (t_start_s,                     "t_start"),
        (t_start_s.saturating_add(3600),"t_start+1h"),
        (t_end_s.saturating_sub(1),     "t_end-ε"),
    ];

    println!(">>> [probe] key consistency (sfc::encode_point vs z3::encode_point_z3):");
    for &(la, lo, name) in &anchors {
        for &(tt, ttag) in &times {
            let k_sfc = encode_point(p, la, lo, tt);
            let k_z3  = encode_point_z3(p, la, lo, tt);
            let same = if k_sfc == k_z3 { "==" } else { "!=" };
            println!("    - {:>6}@{:>10}: sfc={} {} z3={}", name, ttag, k_sfc, same, k_z3);
        }
    }
}

// ====================== Planning result ======================

#[derive(Debug, Clone)]
pub struct PlanResult {
    pub sfc_params: SfcParams,
    pub ranges_raw: Vec<(u64, u64)>,
    pub ranges_merged: Vec<(u64, u64)>,
    pub t_start_s: u64,
    pub t_end_s: u64,
}

// ====================== Helpers: time / anchors / prefix buckets ======================

/// Parse string/integer timestamps into UTC seconds
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

fn build_anchor_points(q: &QueryWindow) -> Vec<(f64, f64)> {
    let lat_c = 0.5 * (q.lat_min + q.lat_max);
    let lon_c = 0.5 * (q.lon_min + q.lon_max);
    vec![
        (lat_c, lon_c),           // center
        (q.lat_min, q.lon_min),   // four corners
        (q.lat_min, q.lon_max),
        (q.lat_max, q.lon_min),
        (q.lat_max, q.lon_max),
    ]
}

/// Compute the key-space boundary [lo, hi] for a bucket with a given high-bit prefix
#[inline]
fn bucket_bounds(prefix: u64, p: u32, total_bits: u32) -> (u64, u64) {
    let p = p.min(total_bits);
    let shift = total_bits - p;
    let lo = if shift >= 64 { 0 } else { prefix << shift };
    let hi = if shift >= 64 { u64::MAX } else { lo | ((1u64 << shift).saturating_sub(1)) };
    (lo, hi)
}

/// Prefix-bit coalescing: bucket all intervals by their high-bit prefix of length p;
/// merge fragments in the same bucket into one [min, max] without shrinking, so true positives are never missed.
fn prefix_bucket_merge(
    ranges: &[(u64,u64)],
    p: u32,
    total_bits: u32,
) -> Vec<(u64,u64)> {
    use std::cmp::{min, max};
    use std::collections::HashMap;

    if ranges.is_empty() { return Vec::new(); }

    let p = p.min(total_bits);
    let shift = total_bits - p;
    let mut buckets: HashMap<u64, (u64,u64)> = HashMap::new();

    for &(a, b) in ranges {
        if a > b { continue; }

        if p == 0 {
            let e = buckets.entry(0).or_insert((a, b));
            e.0 = min(e.0, a);
            e.1 = max(e.1, b);
            continue;
        }

        let k0 = if shift >= 64 { 0 } else { a >> shift };
        let k1 = if shift >= 64 { 0 } else { b >> shift };

        for k in k0..=k1 {
            let (blo, bhi) = bucket_bounds(k, p, total_bits);
            let seg_lo = a.max(blo);
            let seg_hi = b.min(bhi);
            if seg_lo > seg_hi { continue; }
            let e = buckets.entry(k).or_insert((seg_lo, seg_hi));
            e.0 = min(e.0, seg_lo);
            e.1 = max(e.1, seg_hi);
        }
    }

    let mut out: Vec<(u64,u64)> = buckets.into_iter().map(|(_k, v)| v).collect();
    out.sort_unstable_by(|x, y| x.0.cmp(&y.0));
    merge_ranges(out)
}

// Public wrapper kept for backward compatibility
pub fn build_params(cfg: &Config) -> SfcParams { build_sfc_params(cfg) }

// ====================== Main entry ======================

pub fn plan_window(cfg: &Config, q: &QueryWindow) -> PlanResult {
    // 1) SFC parameters
    let p = build_sfc_params(cfg);

    // 2) Parse time into UTC seconds; fall back to global bounds on failure
    let t_start_s = crate::planner::parse_ts_to_epoch_s(&q.t_start).unwrap_or(p.gtime.0);
    let t_end_s   = crate::planner::parse_ts_to_epoch_s(&q.t_end).unwrap_or(p.gtime.1);

    // 3) Closed interval + padding (one quantization bin on each side)
    let bins  = ((1u64 << p.bits.lt) - 1) as f64;
    let bin_w = ((p.gtime.1 - p.gtime.0) as f64 / bins).ceil() as u64;
    let t_lo  = t_start_s.saturating_sub(bin_w);
    let t_hi  = t_end_s.saturating_add(bin_w).saturating_sub(1);

    // 4) Generate the raw cover (single-shot z3)
    let ranges_raw = ranges_for_window(
        &p,
        q.lat_min, q.lat_max,
        q.lon_min, q.lon_max,
        t_lo, t_hi,
    );

    // 5) Regular merging
    let mut ranges_merged = merge_ranges(ranges_raw.clone());

    // 6) Prefix-bit coalescing (core step)
    let total_bits: u32 = p.bits.lx + p.bits.ly + p.bits.lt;
    let p_bits: u32 = cfg.experiment.prefix_bits.unwrap_or(30).min(total_bits);
    ranges_merged = prefix_bucket_merge(&ranges_merged, p_bits, total_bits);

    // 7) Anchor fallback (union injection, never intersection)
    let stop_tail_bits: u8 = cfg.experiment.stop_tail_bits;
    if stop_tail_bits > 0 {
        let anchors_geo = build_anchor_points(q); // center + four corners
        let span_mask: u64 = if stop_tail_bits >= 63 { u64::MAX } else { (1u64 << stop_tail_bits) - 1 };

        let t_edges = [ t_start_s, t_end_s.saturating_sub(1) ];
        let mut edge_buckets: Vec<(u64,u64)> = Vec::with_capacity(anchors_geo.len() * t_edges.len());

        for &(lat, lon) in &anchors_geo {
            for &tt in &t_edges {
                let key = encode_point(&p, lat, lon, tt);
                let b_start = key & !span_mask;
                let b_end   = key |  span_mask;
                edge_buckets.push((b_start, b_end));
            }
        }

        // Union injection + merge
        let mut injected = ranges_merged.clone();
        injected.extend(edge_buckets.into_iter());
        ranges_merged = merge_ranges(injected);
    }

    // Debug output controlled by the same switch
    if debug_enabled(cfg) {
        probe_time_coverage(&p, q.lat_min, q.lat_max, q.lon_min, q.lon_max, t_start_s, t_end_s, &ranges_merged);
        probe_time_quantization(&p, t_start_s, t_end_s);
        let lat_c = 0.5*(q.lat_min + q.lat_max);
        let lon_c = 0.5*(q.lon_min + q.lon_max);
        scan_time_coverage(&p, lat_c, lon_c, t_start_s, t_end_s, 3600, &ranges_merged);
        probe_key_consistency(&p, q, t_start_s, t_end_s);
        println!("after prefix-bucket: {}", ranges_merged.len());
        // 4) Minimum-count validation (optional)
        let mut covered_hours = 0u64;
        let mut t = t_start_s;
        while t < t_end_s {
            let k = encode_point(&p, lat_c, lon_c, t);
            if key_covered(k, &ranges_merged) { covered_hours += 1; }
            t = t.saturating_add(3600);
        }
        println!(
            ">>> [probe] hours covered at center: {} (of total ~{}h)",
            covered_hours, (t_end_s - t_start_s)/3600
        );
    }

    PlanResult {
        sfc_params: p,
        ranges_raw,
        ranges_merged,
        t_start_s,
        t_end_s,
    }
}
