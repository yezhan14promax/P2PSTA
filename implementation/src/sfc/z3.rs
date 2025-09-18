use super::{
    SfcParams, Bits3,
    q_floor_f64, q_floor_u64,
    morton3_prefix,
    merge_ranges,
};

#[inline]
pub fn encode_point_z3(p:&SfcParams, lat:f64, lon:f64, time:u64)->u64{
    let x = q_floor_f64(lat,  p.glat.0,  p.glat.1, p.bits.lx);
    let y = q_floor_f64(lon,  p.glon.0,  p.glon.1, p.bits.ly);
    let z = q_floor_u64(time, p.gtime.0, p.gtime.1, p.bits.lt);
    let total = (p.bits.lx + p.bits.ly + p.bits.lt) as u32;
    morton3_prefix(x, y, z, p.bits, total)
}

pub fn ranges_for_window_z3(
    p:&SfcParams,
    lat_min:f64, lat_max:f64,
    lon_min:f64, lon_max:f64,
    t_min:u64,   t_max:u64,
)->Vec<(u64,u64)>{
    // 1) clamp + 排序
    let (mut la0, mut la1) = if lat_min <= lat_max { (lat_min, lat_max) } else { (lat_max, lat_min) };
    let (mut lo0, mut lo1) = if lon_min <= lon_max { (lon_min, lon_max) } else { (lon_max, lon_min) };
    let (mut ts0, mut ts1) = if t_min   <= t_max   { (t_min,   t_max)   } else { (t_max,   t_min)   };
    la0 = la0.clamp(p.glat.0,  p.glat.1);  la1 = la1.clamp(p.glat.0,  p.glat.1);
    lo0 = lo0.clamp(p.glon.0,  p.glon.1);  lo1 = lo1.clamp(p.glon.0,  p.glon.1);
    ts0 = ts0.clamp(p.gtime.0, p.gtime.1); ts1 = ts1.clamp(p.gtime.0, p.gtime.1);

    // 2) 连续→整型网格（闭区间：左 floor，右 ceil-1）
    let (x0, x1) = q_range_f64(la0, la1, p.glat.0,  p.glat.1,  p.bits.lx);
    let (y0, y1) = q_range_f64(lo0, lo1, p.glon.0,  p.glon.1,  p.bits.ly);
    let (z0, z1) = q_range_u64(ts0, ts1, p.gtime.0, p.gtime.1, p.bits.lt);

    // 3) 递归覆盖（带上限）
    let mut out: Vec<(u64,u64)> = Vec::with_capacity(4096);

    // —— 上限策略（不改对外接口；如果你以后想从 YAML 配，可把这些常量接到 SfcParams 或 Config 上）——
    let total_bits = (p.bits.lx + p.bits.ly + p.bits.lt) as u32;
    // 最深层数上限（相对 total_bits，保守取：到叶子前几层就允许粗接收）
    let max_depth: u32 = total_bits.saturating_sub(8).max(8);
    // 访问节点上限（防止极端窗口爆量；按经验 2~5 万就够用）
    let max_nodes: usize = 50_000;
    // “尾部剩余位”上限（低于该值即粗接收，避免尾部碎）
    let tail_bits_guard: u32 = 12;

    let mut visited: usize = 0;

    cover_node_adaptive_capped(
        p.bits,
        x0,x1, y0,y1, z0,z1,
        0,0,0, p.bits.lx, p.bits.ly, p.bits.lt,
        /*depth*/ 0, max_depth,
        &mut visited, max_nodes,
        tail_bits_guard,
        &mut out,
    );

    merge_ranges(out)
}

// ====================== 内部：递归 + 自适应分裂 + 上限粗接收 ======================

#[inline]
fn cover_node_adaptive_capped(
    bits: Bits3,
    x0:u32, x1:u32, y0:u32, y1:u32, z0:u32, z1:u32,          // 目标盒（闭区间）
    xb:u32, yb:u32, zb:u32, rx:u32, ry:u32, rz:u32,          // 当前节点（起点+剩余位）
    depth:u32, max_depth:u32,
    visited:&mut usize, max_nodes:usize,
    tail_bits_guard:u32,
    out:&mut Vec<(u64,u64)>,
){
    *visited += 1;
    if *visited >= max_nodes {
        // 节点数到上限：粗接收，确保不漏
        push_prefix_range(bits, xb,yb,zb, rx,ry,rz, out);
        return;
    }

    let nx0 = xb;
    let nx1 = xb + ((1u32 << rx).saturating_sub(1));
    let ny0 = yb;
    let ny1 = yb + ((1u32 << ry).saturating_sub(1));
    let nz0 = zb;
    let nz1 = zb + ((1u32 << rz).saturating_sub(1));

    // 不相交
    if nx1 < x0 || nx0 > x1 || ny1 < y0 || ny0 > y1 || nz1 < z0 || nz0 > z1 { return; }

    // 完全包含：直接产出
    if x0 <= nx0 && nx1 <= x1 && y0 <= ny0 && ny1 <= y1 && z0 <= nz0 && nz1 <= z1 {
        push_prefix_range(bits, xb,yb,zb, rx,ry,rz, out);
        return;
    }

    // —— 上限：深度到顶 或 “尾部剩余位很小” -> 粗接收，保证覆盖不漏
    let rem_bits = rx + ry + rz;
    if depth >= max_depth || rem_bits <= tail_bits_guard {
        push_prefix_range(bits, xb,yb,zb, rx,ry,rz, out);
        return;
    }

    // 叶
    if rx==0 && ry==0 && rz==0 {
        push_prefix_range(bits, xb,yb,zb, rx,ry,rz, out);
        return;
    }

    // 自适应选择分裂维（节点跨度/盒跨度 最大者；inside 的维降低权重）
    let ndx = (1u32 << rx).max(1);
    let ndy = (1u32 << ry).max(1);
    let ndz = (1u32 << rz).max(1);
    let bdx = (x1 - x0 + 1).max(1);
    let bdy = (y1 - y0 + 1).max(1);
    let bdz = (z1 - z0 + 1).max(1);

    let mut score_x = if rx > 0 { (ndx as f64) / (bdx as f64) } else { -1.0 };
    let mut score_y = if ry > 0 { (ndy as f64) / (bdy as f64) } else { -1.0 };
    let mut score_z = if rz > 0 { (ndz as f64) / (bdz as f64) } else { -1.0 };

    if x0 <= nx0 && nx1 <= x1 { score_x *= 0.25; }
    if y0 <= ny0 && ny1 <= y1 { score_y *= 0.25; }
    if z0 <= nz0 && nz1 <= z1 { score_z *= 0.25; }

    if score_x >= score_y && score_x >= score_z && rx > 0 {
        let half = 1u32 << (rx - 1);
        cover_node_adaptive_capped(bits, x0,x1, y0,y1, z0,z1, xb,         yb, zb, rx-1, ry,   rz,   depth+1, max_depth, visited, max_nodes, tail_bits_guard, out);
        cover_node_adaptive_capped(bits, x0,x1, y0,y1, z0,z1, xb + half, yb, zb, rx-1, ry,   rz,   depth+1, max_depth, visited, max_nodes, tail_bits_guard, out);
    } else if score_y >= score_x && score_y >= score_z && ry > 0 {
        let half = 1u32 << (ry - 1);
        cover_node_adaptive_capped(bits, x0,x1, y0,y1, z0,z1, xb, yb,         zb, rx,   ry-1, rz,   depth+1, max_depth, visited, max_nodes, tail_bits_guard, out);
        cover_node_adaptive_capped(bits, x0,x1, y0,y1, z0,z1, xb, yb + half, zb, rx,   ry-1, rz,   depth+1, max_depth, visited, max_nodes, tail_bits_guard, out);
    } else {
        debug_assert!(rz > 0);
        let half = 1u32 << (rz - 1);
        cover_node_adaptive_capped(bits, x0,x1, y0,y1, z0,z1, xb, yb, zb,         rx,   ry,   rz-1, depth+1, max_depth, visited, max_nodes, tail_bits_guard, out);
        cover_node_adaptive_capped(bits, x0,x1, y0,y1, z0,z1, xb, yb, zb + half, rx,   ry,   rz-1, depth+1, max_depth, visited, max_nodes, tail_bits_guard, out);
    }
}

#[inline]
fn push_prefix_range(bits:Bits3, xb:u32, yb:u32, zb:u32, rx:u32, ry:u32, rz:u32, out:&mut Vec<(u64,u64)>) {
    let used  = (bits.lx - rx) + (bits.ly - ry) + (bits.lt - rz);
    let pref  = morton3_prefix(xb, yb, zb, bits, used);
    let total = (bits.lx + bits.ly + bits.lt) as u32;
    let rem   = total - used;
    let start = pref;
    let end   = if rem == 0 { start } else { start | ((1u64 << rem) - 1) };
    out.push((start, end));
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
