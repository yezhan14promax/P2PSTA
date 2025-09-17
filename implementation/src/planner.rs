use crate::config::{Config, QueryWindow};
use std::f64::consts::PI;
use crate::sfc;
use crate::sfc::{SfcParams, encode_point, encode_point_z3, ranges_for_window, merge_ranges, build_sfc_params};




// ========== DEBUG 开关 ==========
fn debug_enabled(cfg: &Config) -> bool {
    if let Some(b) = cfg.experiment.debug { return b; }
    std::env::var("P2PSTA_DEBUG")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

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

    // 三个代表性时间点
    let times = [
        (t_start_s,               "t_start"),
        (t_start_s.saturating_add(3600), "t_start+1h"),
        (t_end_s.saturating_sub(1),      "t_end-ε"),
    ];

    println!(">>> [probe] key consistency (sfc::encode_point vs z3::encode_point_z3):");
    for &(la, lo, name) in &anchors {
        for &(tt, ttag) in &times {
            // 统一入口（会走到 Z3 分支）
            let k_sfc = encode_point(p, la, lo, tt);
            // 直接调用 z3 的编码（已通过 sfc.rs re-export 暴露）
            let k_z3  = encode_point_z3(p, la, lo, tt);
            let same = if k_sfc == k_z3 { "==" } else { "!=" };
            println!("    - {:>6}@{:>10}: sfc={} {} z3={}", name, ttag, k_sfc, same, k_z3);
        }
    }
}

// ========= debug开关结束 ==========


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
    // 1) 构建 SFC 参数
    let p = build_sfc_params(cfg);

    // 2) 解析时间（UTC 秒；若解析失败用全局边界兜底）
    let t_start_s = crate::planner::parse_ts_to_epoch_s(&q.t_start).unwrap_or(p.gtime.0);
    let t_end_s   = crate::planner::parse_ts_to_epoch_s(&q.t_end).unwrap_or(p.gtime.1);

    // 3) 闭区间 + pad（两侧各 1 个量化 bin）
    let bins  = ((1u64 << p.bits.lt) - 1) as f64;
    let bin_w = ((p.gtime.1 - p.gtime.0) as f64 / bins).ceil() as u64;
    let t_lo  = t_start_s.saturating_sub(bin_w);
    let t_hi  = t_end_s.saturating_add(bin_w).saturating_sub(1);

    // 4) 生成原始覆盖
    let ranges_raw = ranges_for_window(
        &p,
        q.lat_min, q.lat_max,
        q.lon_min, q.lon_max,
        t_lo, t_hi,
    );

    // 5) 初次合并
    let mut ranges_merged = merge_ranges(ranges_raw.clone());
    // 如你已有带 gap 的合并，这里可换成：
    // let mut ranges_merged = merge_ranges_with_gap(ranges_raw.clone(), cfg.experiment.merge_gap_keys);

    // 6) 时间边界锚点注入（t_start 与 t_end-ε），与已有 merged 求交后再合并
    let stop_tail_bits: u8 = cfg.experiment.stop_tail_bits;
    if stop_tail_bits > 0 {
        let anchors = build_anchor_points(q); // 中心+四角
        let span_mask: u64 = if stop_tail_bits >= 63 { u64::MAX } else { (1u64 << stop_tail_bits) - 1 };

        let t_edges = [ t_start_s, t_end_s.saturating_sub(1) ];
        let mut edge_buckets: Vec<(u64,u64)> = Vec::with_capacity(anchors.len() * t_edges.len());

        for &(lat, lon) in &anchors {
            for &tt in &t_edges {
                let key = encode_point(&p, lat, lon, tt);
                let b_start = key & !span_mask;
                let b_end   = key |  span_mask;
                edge_buckets.push((b_start, b_end));
            }
        }

        let merged2 = constrained_inject(&ranges_merged, &edge_buckets);
        let merged2 = merge_ranges(merged2); // 或 merge_ranges_with_gap(merged2, cfg.experiment.merge_gap_keys);
        ranges_merged = merged2;
    }

    // 7) 软上限截断
    if debug_enabled(cfg) {
        println!(">>> [probe] before cap: raw={} merged={}", ranges_raw.len(), ranges_merged.len());
    }
    if let Some(maxr) = cfg.experiment.max_ranges.or(p.max_ranges) {
        if ranges_merged.len() > maxr {
            if debug_enabled(cfg) {
                println!(">>> [probe] TRUNCATING ranges_merged from {} to {}", ranges_merged.len(), maxr);
            }
            ranges_merged.truncate(maxr);
        }
    }

    if let Some(maxr) = cfg.experiment.max_ranges.or(p.max_ranges) {
        if ranges_merged.len() > maxr {
            ranges_merged.truncate(maxr);
        }
    }

    if debug_enabled(cfg) {
        println!(">>> [probe] after  cap: merged={}", ranges_merged.len());
    }

    // （可选）超大区间防御
    // let total_bits = (p.bits.lx + p.bits.ly + p.bits.lt).min(63);
    // let max_span   = (1u64 << total_bits) / 4;
    // ranges_merged.retain(|&(s,e)| e >= s && (e - s + 1) <= max_span);

    //debug start
    // === 探针：时间边界覆盖（t_start / t_end-ε） ===
    probe_time_coverage(
        &p,
        q.lat_min, q.lat_max, q.lon_min, q.lon_max,
        t_start_s, t_end_s,
        &ranges_merged,
    );

    // === 探针：量化信息 ===
    probe_time_quantization(&p, t_start_s, t_end_s);

    // === 探针：中心点按 1h 步长扫样 ===
    let lat_c = 0.5*(q.lat_min + q.lat_max);
    let lon_c = 0.5*(q.lon_min + q.lon_max);
    scan_time_coverage(&p, lat_c, lon_c, t_start_s, t_end_s, 3600, &ranges_merged);
    
    // === 探针：关键点编码一致性（sfc vs z3） ===
    probe_key_consistency(&p, q, t_start_s, t_end_s);
    //debug end

    if debug_enabled(cfg) {
        // 1) 时间边界覆盖
        probe_time_coverage(
            &p,
            q.lat_min, q.lat_max, q.lon_min, q.lon_max,
            t_start_s, t_end_s,
            &ranges_merged,
        );

        // 2) 时间量化信息
        probe_time_quantization(&p, t_start_s, t_end_s);

        // 3) 中心点 1 小时步进扫样
        let lat_c = 0.5*(q.lat_min + q.lat_max);
        let lon_c = 0.5*(q.lon_min + q.lon_max);

        let k_sfc = crate::sfc::encode_point(&p, lat_c, lon_c, t_start_s);
        let k_z3  = crate::sfc::encode_point_z3(&p, lat_c, lon_c, t_start_s);
        println!(">>> [probe] key compare at t_start: sfc={} z3={}", k_sfc, k_z3);

        probe_key_consistency(&p, q, t_start_s, t_end_s);
        scan_time_coverage(&p, lat_c, lon_c, t_start_s, t_end_s, 3600, &ranges_merged);

        // 4) 最小计数验证（可选）
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


