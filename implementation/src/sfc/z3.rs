//! Z3（3D Morton Z-order）窗口覆盖：严格在整型网格上进行包含/相交判定；
//! 与 sfc::encode_point(Z3) 使用**相同的 floor 量化与 MSB-first X→Y→Z 交错**。

use super::{
    SfcParams, Bits3,
    q_floor_f64, q_floor_u64,
    morton3_interleave_var, morton3_prefix,
    merge_ranges,
};

// ============== 对外 API ==============

/// 点编码（Z3）。与 sfc::encode_point(Z3) 一致，提供独立接口以便单元测试/直接调用。
#[inline]
pub fn encode_point_z3(p:&SfcParams, lat:f64, lon:f64, time:u64)->u64{
    let x = q_floor_f64(lat,  p.glat.0,  p.glat.1,  p.bits.lx);
    let y = q_floor_f64(lon,  p.glon.0,  p.glon.1,  p.bits.ly);
    let z = q_floor_u64(time, p.gtime.0, p.gtime.1, p.bits.lt);
    morton3_interleave_var(x, y, z, p.bits)
}

/// 将 (lat,lon,time) 窗口映射为一组 Morton 前缀区间（返回前已合并）。
pub fn ranges_for_window_z3(
    p:&SfcParams,
    lat_min:f64, lat_max:f64,
    lon_min:f64, lon_max:f64,
    t_min:u64,   t_max:u64,
)->Vec<(u64,u64)>{
    // 夹紧 & 有序
    let (mut la0, mut la1) = if lat_min <= lat_max { (lat_min, lat_max) } else { (lat_max, lat_min) };
    let (mut lo0, mut lo1) = if lon_min <= lon_max { (lon_min, lon_max) } else { (lon_max, lon_min) };
    let (mut ts0, mut ts1) = if t_min   <= t_max   { (t_min,   t_max)   } else { (t_max,   t_min)   };
    la0 = la0.clamp(p.glat.0,  p.glat.1);  la1 = la1.clamp(p.glat.0,  p.glat.1);
    lo0 = lo0.clamp(p.glon.0,  p.glon.1);  lo1 = lo1.clamp(p.glon.0,  p.glon.1);
    ts0 = ts0.clamp(p.gtime.0, p.gtime.1); ts1 = ts1.clamp(p.gtime.0, p.gtime.1);

    // 保守量化到整型网格（闭区间：左端 floor，右端 ceil-1）
    let (x0, x1) = q_range_f64(la0, la1, p.glat.0,  p.glat.1,  p.bits.lx);
    let (y0, y1) = q_range_f64(lo0, lo1, p.glon.0,  p.glon.1,  p.bits.ly);
    let (z0, z1) = q_range_u64(ts0, ts1, p.gtime.0, p.gtime.1, p.bits.lt);

    // 递归覆盖
    let mut out: Vec<(u64,u64)> = Vec::with_capacity(512);
    cover(
        p.bits,
        0, p.bits.lx,   // xb, rx
        0, p.bits.ly,   // yb, ry
        0, p.bits.lt,   // zb, rz
        x0, x1, y0, y1, z0, z1,
        &mut out,
        p.max_ranges,   // 软上限：达到上限后直接输出当前前缀（保守覆盖）
    );

    merge_ranges(out)
}

// ============== 内部量化区间（闭区间） ==============

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

// ============== 递归覆盖（八叉树，整型网格上判定） ==============

fn cover(
    bits: Bits3,
    xb:u32, rx:u32,
    yb:u32, ry:u32,
    zb:u32, rz:u32,
    x0:u32, x1:u32, y0:u32, y1:u32, z0:u32, z1:u32,
    out:&mut Vec<(u64,u64)>,
    max_ranges_soft: Option<usize>,
) {
    // 当前节点的闭区间盒（整格）
    let x_end = xb + ((1u32 << rx).saturating_sub(1));
    let y_end = yb + ((1u32 << ry).saturating_sub(1));
    let z_end = zb + ((1u32 << rz).saturating_sub(1));

    // 无交集
    if x_end < x0 || xb > x1 || y_end < y0 || yb > y1 || z_end < z0 || zb > z1 {
        return;
    }

    // 完整包含
    if x0 <= xb && x_end <= x1 && y0 <= yb && y_end <= y1 && z0 <= zb && z_end <= z1 {
        push_prefix_range(bits, xb, yb, zb, rx, ry, rz, out);
        return;
    }

    // 软上限：保守覆盖当前节点
    if let Some(limit) = max_ranges_soft {
        if out.len() >= limit {
            push_prefix_range(bits, xb, yb, zb, rx, ry, rz, out);
            return;
        }
    }

    // 叶层
    if rx==0 && ry==0 && rz==0 {
        push_prefix_range(bits, xb, yb, zb, rx, ry, rz, out);
        return;
    }

    // 与 MSB-first X→Y→Z 一致的拆分优先级：总在“剩余位数最大的维”上二分
    if rx >= ry && rx >= rz && rx > 0 {
        let half = 1u32 << (rx - 1);
        cover(bits, xb,      rx-1, yb, ry, zb, rz, x0,x1,y0,y1,z0,z1, out, max_ranges_soft);
        cover(bits, xb+half, rx-1, yb, ry, zb, rz, x0,x1,y0,y1,z0,z1, out, max_ranges_soft);
    } else if ry >= rx && ry >= rz && ry > 0 {
        let half = 1u32 << (ry - 1);
        cover(bits, xb, rx, yb,      ry-1, zb, rz, x0,x1,y0,y1,z0,z1, out, max_ranges_soft);
        cover(bits, xb, rx, yb+half, ry-1, zb, rz, x0,x1,y0,y1,z0,z1, out, max_ranges_soft);
    } else if rz > 0 {
        let half = 1u32 << (rz - 1);
        cover(bits, xb, rx, yb, ry, zb,      rz-1, x0,x1,y0,y1,z0,z1, out, max_ranges_soft);
        cover(bits, xb, rx, yb, ry, zb+half, rz-1, x0,x1,y0,y1,z0,z1, out, max_ranges_soft);
    } else {
        push_prefix_range(bits, xb, yb, zb, rx, ry, rz, out);
    }
}

#[inline]
fn push_prefix_range(bits:Bits3, xb:u32, yb:u32, zb:u32, rx:u32, ry:u32, rz:u32, out:&mut Vec<(u64,u64)>) {
    // 已用高位数
    let used  = (bits.lx - rx) + (bits.ly - ry) + (bits.lt - rz);
    let pref  = morton3_prefix(xb, yb, zb, bits, used);
    let total = (bits.lx + bits.ly + bits.lt) as u32;
    let rem   = total - used;
    let start = pref;
    let end   = if rem == 0 { start } else { start | ((1u64 << rem) - 1) };
    out.push((start, end));
}
