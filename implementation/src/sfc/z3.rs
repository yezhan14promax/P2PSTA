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
    // 1) Clamp + sort
    let (mut la0, mut la1) = if lat_min <= lat_max { (lat_min, lat_max) } else { (lat_max, lat_min) };
    let (mut lo0, mut lo1) = if lon_min <= lon_max { (lon_min, lon_max) } else { (lon_max, lon_min) };
    let (mut ts0, mut ts1) = if t_min   <= t_max   { (t_min,   t_max)   } else { (t_max,   t_min)   };
    la0 = la0.clamp(p.glat.0,  p.glat.1);  la1 = la1.clamp(p.glat.0,  p.glat.1);
    lo0 = lo0.clamp(p.glon.0,  p.glon.1);  lo1 = lo1.clamp(p.glon.0,  p.glon.1);
    ts0 = ts0.clamp(p.gtime.0, p.gtime.1); ts1 = ts1.clamp(p.gtime.0, p.gtime.1);

    // 2) Continuous -> discrete (strict closed interval to avoid dropping the right edge)
    let (x0, x1) = q_range_f64_closed(la0, la1, p.glat.0,  p.glat.1,  p.bits.lx);
    let (y0, y1) = q_range_f64_closed(lo0, lo1, p.glon.0,  p.glon.1,  p.bits.ly);
    let (z0, z1) = q_range_u64_closed(ts0, ts1, p.gtime.0, p.gtime.1, p.bits.lt);

    let bits   = p.bits;
    let total  = (bits.lx + bits.ly + bits.lt) as u32;
    let maxb   = bits.lx.max(bits.ly).max(bits.lt);

    let mut out: Vec<(u64,u64)> = Vec::with_capacity(4096);

    // 3) Recursion + limits + coarse acceptance (to guarantee no misses)
    let mut visited: usize = 0;
    cover_by_bitplanes_capped(
        bits, total,
        x0, x1, y0, y1, z0, z1,
        /*prefix*/ 0,
        /*level */ maxb,
        /*base  */ 0, 0, 0,
        /*depth */ 0,
        p.max_depth,
        &mut visited,
        p.max_nodes,
        p.tail_bits_guard,
        &mut out,
    );

    merge_ranges(out)
}

/// Count how many bits remain unused at this level
#[inline]
fn used_bits_at_level(bits: Bits3, level: u32) -> u32 {
    // Same meaning as the earlier count_used_bits: used high bits = (lx - level_clamped) + ...
    let lx_used = bits.lx.saturating_sub(level.min(bits.lx));
    let ly_used = bits.ly.saturating_sub(level.min(bits.ly));
    let lt_used = bits.lt.saturating_sub(level.min(bits.lt));
    lx_used + ly_used + lt_used
}

/// Bit-level recursion carrying the three-axis base, with max depth / max nodes / coarse acceptance on the tail bits
fn cover_by_bitplanes_capped(
    bits: Bits3, total:u32,
    x0:u32, x1:u32, y0:u32, y1:u32, z0:u32, z1:u32,
    prefix:u64,
    level:u32,
    base_x:u64, base_y:u64, base_z:u64,
    depth:u32,
    max_depth:u32,
    visited:&mut usize,
    max_nodes:usize,
    tail_bits_guard:u32,
    out:&mut Vec<(u64,u64)>
){
    // Visit counter and node cap -> coarse-accept fallback
    *visited += 1;
    if *visited >= max_nodes {
        let used = used_bits_at_level(bits, level);
        let rem  = total - used;
        let start = prefix << rem;
        let end   = start | ((1u64 << rem).saturating_sub(1));
        out.push((start, end));
        return;
    }

    // Max depth reached or too few tail bits left -> coarse-accept fallback without losing true positives
    let used = used_bits_at_level(bits, level);
    let rem  = total - used;
    if depth >= max_depth || rem <= tail_bits_guard {
        let start = prefix << rem;
        let end   = start | ((1u64 << rem).saturating_sub(1));
        out.push((start, end));
        return;
    }

    if level == 0 {
        // Leaf node: exactly one key
        out.push((prefix, prefix));
        return;
    }

    // Whether each axis still has bits at this level
    let bx = bits.lx >= level;
    let by = bits.ly >= level;
    let bz = bits.lt >= level;

    // Block width on each axis at this level (in cells)
    let span_x = if bx { 1u64 << (level - 1) } else { 0 };
    let span_y = if by { 1u64 << (level - 1) } else { 0 };
    let span_z = if bz { 1u64 << (level - 1) } else { 0 };

    // Used bits at the next level (inside segments use rem_next for expansion)
    let used_next = used_bits_at_level(bits, level - 1);
    let rem_next  = total - used_next;

    let mut child = 0u8;
    while child < 8 {
        let xbit = if bx { (child >> 2) & 1 } else { 0 };
        let ybit = if by { (child >> 1) & 1 } else { 0 };
        let zbit = if bz { (child >> 0) & 1 } else { 0 };

        // Propagate the base to the child node
        let cbx = if bx { base_x + (xbit as u64) * span_x } else { base_x };
        let cby = if by { base_y + (ybit as u64) * span_y } else { base_y };
        let cbz = if bz { base_z + (zbit as u64) * span_z } else { base_z };

        // Closed interval of the child block
        let nx0 = cbx;
        let nx1 = if bx { cbx + span_x - 1 } else { cbx + ((1u64 << bits.lx) - 1) };
        let ny0 = cby;
        let ny1 = if by { cby + span_y - 1 } else { cby + ((1u64 << bits.ly) - 1) };
        let nz0 = cbz;
        let nz1 = if bz { cbz + span_z - 1 } else { cbz + ((1u64 << bits.lt) - 1) };

        let inter = !(nx1 < x0 as u64 || nx0 > x1 as u64
                   || ny1 < y0 as u64 || ny0 > y1 as u64
                   || nz1 < z0 as u64 || nz0 > z1 as u64);

        if inter {
            // Append the bits of this level
            let mut pref = prefix;
            if bx { pref = (pref << 1) | (xbit as u64); }
            if by { pref = (pref << 1) | (ybit as u64); }
            if bz { pref = (pref << 1) | (zbit as u64); }

            let inside = (x0 as u64) <= nx0 && nx1 <= (x1 as u64)
                      && (y0 as u64) <= ny0 && ny1 <= (y1 as u64)
                      && (z0 as u64) <= nz0 && nz1 <= (z1 as u64);

            if inside {
                // Accept the whole prefix range
                let start = pref << rem_next;
                let end   = start | ((1u64 << rem_next).saturating_sub(1));
                out.push((start, end));
            } else {
                cover_by_bitplanes_capped(
                    bits, total,
                    x0, x1, y0, y1, z0, z1,
                    pref,
                    level - 1,
                    cbx, cby, cbz,
                    depth + 1,
                    max_depth,
                    visited,
                    max_nodes,
                    tail_bits_guard,
                    out
                );
            }
        }

        child += 1;
    }
}

// ===== Quantization (strict closed interval to avoid dropping the right edge) =====

#[inline]
fn q_range_f64_closed(v0:f64, v1:f64, mn:f64, mx:f64, L:u32)->(u32,u32){
    if L==0 || !(mx>mn) { return (0,0); }
    let n  = ((1u64<<L)-1) as f64;
    let t0 = ((v0 - mn) / (mx - mn)).clamp(0.0, 1.0);
    let mut t1 = ((v1 - mn) / (mx - mn)).clamp(0.0, 1.0);
    t1 = f64::from_bits((t1.to_bits()).wrapping_sub(1)).max(t0); // next_down
    let lo = (t0 * n).floor() as i64;
    let hi = (t1 * n).floor() as i64;
    let lo = lo.clamp(0, n as i64) as u32;
    let hi = hi.clamp(0, n as i64) as u32;
    (lo.min(hi), hi.max(lo))
}

#[inline]
fn q_range_u64_closed(v0:u64, v1:u64, mn:u64, mx:u64, L:u32)->(u32,u32){
    if L==0 || !(mx>mn) { return (0,0); }
    let n  = ((1u64<<L)-1) as f64;
    let t0 = (v0.saturating_sub(mn) as f64 / (mx - mn) as f64).clamp(0.0, 1.0);
    let mut t1 = (v1.saturating_sub(mn) as f64 / (mx - mn) as f64).clamp(0.0, 1.0);
    t1 = f64::from_bits((t1.to_bits()).wrapping_sub(1)).max(t0);
    let lo = (t0 * n).floor() as i64;
    let hi = (t1 * n).floor() as i64;
    let lo = lo.clamp(0, n as i64) as u32;
    let hi = hi.clamp(0, n as i64) as u32;
    (lo.min(hi), hi.max(lo))
}
