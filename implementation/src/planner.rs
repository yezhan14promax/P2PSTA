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

/// 把 [t_start, t_end) 切成等长的时间桶（每桶秒数 = bin_secs）
fn time_bins(t_start: u64, t_end: u64, bin_secs: u64) -> Vec<(u64,u64)> {
    let mut out = Vec::new();
    if t_end <= t_start || bin_secs == 0 { return out; }
    let mut s = t_start / bin_secs * bin_secs;
    while s < t_end {
        let e  = s.saturating_add(bin_secs);
        let lo = s.max(t_start);
        let hi = e.min(t_end);
        if hi > lo { out.push((lo, hi)); }
        s = e;
    }
    out
}

#[inline]
fn sample_uniform(v:&[(u64,u64)], cap:usize) -> Vec<(u64,u64)> {
    if cap == 0 || v.is_empty() { return Vec::new(); }
    if v.len() <= cap { return v.to_vec(); }
    let n = v.len();
    let mut out = Vec::with_capacity(cap);
    for i in 0..cap {
        let idx = (i as u128 * n as u128 / cap as u128) as usize;
        out.push(v[idx]);
    }
    out
}

/// 前缀抬升（自举抬升）：若某区间恰好是完整前缀（start 对齐，end = start | ((1<<rem)-1)），可逐层抬升。
fn prefix_lift_inplace(ranges: &mut Vec<(u64,u64)>, bits: crate::sfc::Bits3) {
    if ranges.is_empty() { return; }
    let _total = (bits.lx + bits.ly + bits.lt).min(63);
    for r in ranges.iter_mut() {
        let (mut s, mut e) = *r;
        loop {
            let x = s ^ e;
            if x == 0 { break; }
            // rem = 低位连续的 1 的数量，使 s..e = 某前缀的完整覆盖
            let rem = x.trailing_ones();
            if rem == 0 { break; }
            let mask_low = (1u64 << rem) - 1;
            if (s & mask_low) != 0 { break; }
            // 只有左子（上一位为 0）才可与兄弟拼父
            let parent_bit = 1u64 << rem;
            if (s & parent_bit) != 0 { break; }
            // 抬升一位：清掉 parent_bit 和低 rem 位，e 把它们全置 1
            s &= !(parent_bit | mask_low);
            e |=  parent_bit | mask_low;
            // 继续尝试更高一层
        }
        *r = (s, e);
    }
    // 抬升后再合并
    *ranges = merge_ranges(std::mem::take(ranges));
}

/// 最终预算：按“时间桶”均衡抽样（每桶均匀留样），避免偏置到某一键段或时段
fn truncate_by_time_bucket_uniform(
    ranges_per_bin: &mut Vec<Vec<(u64,u64)>>,
    cap: usize,
) -> Vec<(u64,u64)> {
    if cap == 0 {
        return Vec::new();
    }
    let bins = ranges_per_bin.len();
    if bins == 0 {
        return Vec::new();
    }
    let mut picked = Vec::with_capacity(cap);
    let per = (cap + bins - 1) / bins; // 向上取整
    for v in ranges_per_bin.iter_mut() {
        if picked.len() >= cap { break; }
        if v.is_empty() { continue; }
        if v.len() <= per {
            picked.extend(v.drain(..));
        } else {
            let n = v.len();
            for i in 0..per {
                if picked.len() >= cap { break; }
                let idx = (i as u128 * n as u128 / per as u128) as usize;
                picked.push(v[idx]);
            }
        }
    }
    // 若仍超过 cap（最后一桶溢出），整体再均匀抽一次
    if picked.len() > cap {
        let n = picked.len();
        let mut shrunk = Vec::with_capacity(cap);
        for i in 0..cap {
            let idx = (i as u128 * n as u128 / cap as u128) as usize;
            shrunk.push(picked[idx]);
        }
        return shrunk;
    }
    picked
}



/// 计算给定高位前缀 bucket 的键空间边界 [lo, hi]
#[inline]
fn bucket_bounds(prefix: u64, p: u32, total_bits: u32) -> (u64, u64) {
    let p = p.min(total_bits);
    let shift = total_bits - p;
    let lo = if shift >= 64 { 0 } else { prefix << shift };
    let hi = if shift >= 64 { u64::MAX } else { lo | ((1u64 << shift).saturating_sub(1)) };
    (lo, hi)
}

/// 位前缀归并：把所有区间按高位前缀（长度 p）分桶；
/// 同一桶内的片段并成一个 [min, max]，确保“不过度收缩”=> 不会漏真阳性。
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

    // 每个桶记录该桶内出现的最小起点和最大终点
    let mut buckets: HashMap<u64, (u64,u64)> = HashMap::new();

    for &(a, b) in ranges {
        if a > b { continue; }
        if p == 0 {
            // 只有一个桶（全域）
            let e = buckets.entry(0).or_insert((a, b));
            e.0 = min(e.0, a);
            e.1 = max(e.1, b);
            continue;
        }

        let k0 = if shift >= 64 { 0 } else { a >> shift };
        let k1 = if shift >= 64 { 0 } else { b >> shift };

        for k in k0..=k1 {
            let (blo, bhi) = bucket_bounds(k, p, total_bits);
            // 与该桶边界相交的片段（注意：我们不会把片段缩小到更小的范围，
            // 这里取 [a,b] 与桶的交集只是为了投放到正确的桶，
            // 最后同桶内用 [min,max] 整段覆盖，不会漏）
            let seg_lo = a.max(blo);
            let seg_hi = b.min(bhi);
            if seg_lo > seg_hi { continue; }

            let e = buckets.entry(k).or_insert((seg_lo, seg_hi));
            e.0 = min(e.0, seg_lo);
            e.1 = max(e.1, seg_hi);
        }
    }

    // 输出为按起点排序的列表
    let mut out: Vec<(u64,u64)> = buckets.into_iter().map(|(_k, v)| v).collect();
    out.sort_unstable_by(|x, y| x.0.cmp(&y.0));

    // 可选：相邻桶可能连成更大段，这里再合并一次，进一步降碎片
    merge_ranges(out)
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

    // 4) 生成原始覆盖（一次性跑 z3；不再做时间分桶）
    let ranges_raw = ranges_for_window(
        &p,
        q.lat_min, q.lat_max,
        q.lon_min, q.lon_max,
        t_lo, t_hi,
    );

    // 5) 初次合并（可换成带 gap 的版本）
    let mut ranges_merged = merge_ranges(ranges_raw.clone());
    // let mut ranges_merged = merge_ranges_with_gap(ranges_raw.clone(), cfg.experiment.merge_gap_keys);

    // 6) 位前缀归并（核心）：按高位前缀长度 p_bits 进行分桶归并
    let total_bits: u32 = (p.bits.lx + p.bits.ly + p.bits.lt).min(63);
    // 从配置读取前缀长度（位数），没有就给一个温和默认，比如 18
    let p_bits: u32 = cfg.experiment.prefix_bits.unwrap_or(18).min(total_bits);
    ranges_merged = prefix_bucket_merge(&ranges_merged, p_bits, total_bits);

    // 7)（可选）时间边界锚点注入 —— 采用“并集”方式兜底；绝不做相交
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

        // 并集注入，不裁剪
        let mut injected = ranges_merged.clone();
        injected.extend(edge_buckets.into_iter());
        ranges_merged = merge_ranges(injected);
    }

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

    // —— 保持你原有的返回字段（包括 ranges_raw）——
    PlanResult {
        sfc_params: p,
        ranges_raw,
        ranges_merged,
        t_start_s,
        t_end_s,
    }
}




