use super::{
    SfcParams, Bits3,
    q_f64, q_u64,
    morton3_interleave_var, morton3_prefix,
    merge_ranges,
};

/// 点编码（Z-order 3D）
pub fn encode_point_z3(p:&SfcParams, lat:f64, lon:f64, time:u64)->u64{
    let x = q_f64(lat,  p.glat.0,  p.glat.1,  p.bits.lx);
    let y = q_f64(lon,  p.glon.0,  p.glon.1,  p.bits.ly);
    let z = q_u64(time, p.gtime.0, p.gtime.1, p.bits.lt);
    morton3_interleave_var(x, y, z, p.bits)
}

/// 窗口覆盖为一组 Morton 前缀区间（已合并）
/// 注意：这里做的是“近似覆盖”，达到上限时直接输出“当前节点”的前缀区间并返回。
pub fn ranges_for_window_z3(
    p:&SfcParams,
    lat_min:f64, lat_max:f64,
    lon_min:f64, lon_max:f64,
    t_min:u64,   t_max:u64,
)->Vec<(u64,u64)>{
    // 容错 & 夹紧
    let (mut la0, mut la1) = if lat_min <= lat_max { (lat_min, lat_max) } else { (lat_max, lat_min) };
    let (mut lo0, mut lo1) = if lon_min <= lon_max { (lon_min, lon_max) } else { (lon_max, lon_min) };
    let (mut ts0, mut ts1) = if t_min   <= t_max   { (t_min,   t_max)   } else { (t_max,   t_min)   };
    la0 = la0.clamp(p.glat.0,  p.glat.1);  la1 = la1.clamp(p.glat.0,  p.glat.1);
    lo0 = lo0.clamp(p.glon.0,  p.glon.1);  lo1 = lo1.clamp(p.glon.0,  p.glon.1);
    ts0 = ts0.clamp(p.gtime.0, p.gtime.1); ts1 = ts1.clamp(p.gtime.0, p.gtime.1);

    // 保守量化：左边界 floor、右边界 ceil-1，确保覆盖不漏格
    let (x0, x1) = q_range_f64(la0, la1, p.glat.0,  p.glat.1,  p.bits.lx);
    let (y0, y1) = q_range_f64(lo0, lo1, p.glon.0, p.glon.1,   p.bits.ly);
    let (z0, z1) = q_range_u64(ts0, ts1, p.gtime.0, p.gtime.1, p.bits.lt);

    // 递归覆盖（八叉树），达到软上限则直接输出当前节点前缀区间
    let mut out: Vec<(u64,u64)> = Vec::new();
    cover_z3(
        p.bits,
        0, 0, 0,                   // 基底坐标（最高层）
        p.bits.lx, p.bits.ly, p.bits.lt, // 剩余位数
        x0, x1, y0, y1, z0, z1,
        &mut out,
        p.max_ranges,              // 软上限
    );
    merge_ranges(out)
}

#[inline]
fn q_range_f64(v0:f64, v1:f64, mn:f64, mx:f64, L:u32)->(u32,u32){
    if L==0 || mx<=mn { return (0,0); }
    let n = ((1u64<<L)-1) as f64;
    let t0 = ((v0 - mn) / (mx - mn)).clamp(0.0, 1.0);
    let t1 = ((v1 - mn) / (mx - mn)).clamp(0.0, 1.0);
    let lo = (t0 * n).floor() as i64;
    let hi = (t1 * n).ceil()  as i64 - 1;
    let lo = lo.clamp(0, n as i64) as u32;
    let hi = hi.clamp(0, n as i64) as u32;
    (lo.min(hi), hi.max(lo))
}

#[inline]
fn q_range_u64(v0:u64, v1:u64, mn:u64, mx:u64, L:u32)->(u32,u32){
    if L==0 || mx<=mn { return (0,0); }
    let n = ((1u64<<L)-1) as f64;
    let t0 = (v0.saturating_sub(mn) as f64 / (mx - mn) as f64).clamp(0.0, 1.0);
    let t1 = (v1.saturating_sub(mn) as f64 / (mx - mn) as f64).clamp(0.0, 1.0);
    let lo = (t0 * n).floor() as i64;
    let hi = (t1 * n).ceil()  as i64 - 1;
    let lo = lo.clamp(0, n as i64) as u32;
    let hi = hi.clamp(0, n as i64) as u32;
    (lo.min(hi), hi.max(lo))
}

/// 递归覆盖（八叉树）
/// - (xb,yb,zb) 是当前节点在量化格中的“左上前”基底索引；
/// - (rx,ry,rz) 是该节点在各维的剩余位数（节点尺度 = 2^r*）；
/// - [x0..x1]/[y0..y1]/[z0..z1] 是查询窗口在量化格的闭区间；
/// - 达到 `max_ranges_soft` 上限时，**直接输出“当前节点”的 Morton 前缀区间并返回**。
fn cover_z3(
    orig: Bits3,
    xb:u32, yb:u32, zb:u32,
    rx:u32, ry:u32, rz:u32,
    x0:u32, x1:u32, y0:u32, y1:u32, z0:u32, z1:u32,
    out:&mut Vec<(u64,u64)>,
    max_ranges_soft: Option<usize>,
){
    // 当前节点的闭区间盒
    let nx = if rx==0 { 1 } else { 1u32 << rx };
    let ny = if ry==0 { 1 } else { 1u32 << ry };
    let nz = if rz==0 { 1 } else { 1u32 << rz };
    let x_end = xb.saturating_add(nx.saturating_sub(1));
    let y_end = yb.saturating_add(ny.saturating_sub(1));
    let z_end = zb.saturating_add(nz.saturating_sub(1));

    // 与查询框无交集：直接返回
    if x_end < x0 || xb > x1 || y_end < y0 || yb > y1 || z_end < z0 || zb > z1 {
        return;
    }

    // 被查询框完整包含：直接输出“当前节点”前缀区间
    if x0 <= xb && x1 >= x_end && y0 <= yb && y1 >= y_end && z0 <= zb && z1 >= z_end {
        push_prefix_range(orig, xb, yb, zb, rx, ry, rz, out);
        return;
    }

    // 软上限：直接将“当前节点”作为一个前缀区间输出并返回（避免爆炸）
    if let Some(limit) = max_ranges_soft {
        if out.len() >= limit {
            push_prefix_range(orig, xb, yb, zb, rx, ry, rz, out);
            return;
        }
    }

    // 还没到叶子：沿各维减 1 bit（最多分 8 个子节点）
    if rx==0 && ry==0 && rz==0 {
        // 叶子单元（仍与窗口相交）
        push_prefix_range(orig, xb, yb, zb, rx, ry, rz, out);
        return;
    }

    let rx1 = rx.min(1);
    let ry1 = ry.min(1);
    let rz1 = rz.min(1);

    for dx in 0..(1u32 << rx1) {
        let cx = xb + (dx << rx.saturating_sub(1));
        for dy in 0..(1u32 << ry1) {
            let cy = yb + (dy << ry.saturating_sub(1));
            for dz in 0..(1u32 << rz1) {
                let cz = zb + (dz << rz.saturating_sub(1));
                cover_z3(
                    orig,
                    cx, cy, cz,
                    rx.saturating_sub(1),
                    ry.saturating_sub(1),
                    rz.saturating_sub(1),
                    x0, x1, y0, y1, z0, z1,
                    out,
                    max_ranges_soft,
                );
            }
        }
    }
}

#[inline]
fn push_prefix_range(orig:Bits3, xb:u32, yb:u32, zb:u32, rx:u32, ry:u32, rz:u32, out:&mut Vec<(u64,u64)>) {
    // 已用前缀位数（高位）
    let used  = (orig.lx - rx) + (orig.ly - ry) + (orig.lt - rz);
    let pref  = morton3_prefix(xb, yb, zb, orig, used);
    let total = (orig.lx + orig.ly + orig.lt) as u32;
    let shift = (total - used) as u64;
    let start = pref;
    let end   = if shift == 0 { start } else { start | ((1u64 << shift) - 1) };
    out.push((start, end));
}
