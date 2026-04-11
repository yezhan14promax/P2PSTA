use super::{
    Bits3, SfcParams, q_f64,
    morton2_interleave_var, morton3_prefix, merge_ranges,
};

/// Map a real-valued range to the quantized closed index interval [imin, imax], with the right edge using ceil()-1
fn idx_range_f64(vmin: f64, vmax: f64, mn: f64, mx: f64, l: u32) -> (u32, u32) {
    let n = ((1u64 << l) - 1) as f64;
    let clamp = |v: f64| v.max(0.0).min(1.0);
    let a = clamp((vmin - mn) / (mx - mn));
    let b = clamp((vmax - mn) / (mx - mn));
    let imin = (a * n).floor().max(0.0) as u32;
    let mut imax = ((b * n).ceil() - 1.0).max(0.0) as u32;
    let max_i = n as u32;
    if imax > max_i { imax = max_i; }
    (imin.min(imax), imax)
}

/// Whether a 2D node is fully inside the query rectangle
#[inline]
fn quad_inside(x0: u32, x1: u32, y0: u32, y1: u32, qx0: u32, qx1: u32, qy0: u32, qy1: u32) -> bool {
    x0 >= qx0 && x1 <= qx1 && y0 >= qy0 && y1 <= qy1
}
/// Whether a 2D node is disjoint from the query rectangle
#[inline]
fn quad_outside(x0: u32, x1: u32, y0: u32, y1: u32, qx0: u32, qx1: u32, qy0: u32, qy1: u32) -> bool {
    x1 < qx0 || x0 > qx1 || y1 < qy0 || y0 > qy1
}

/// Recursive 2D Z-order cover:
/// - orig = Bits3{lx, ly, lt:0}, used to compute the Morton prefix;
/// - current node (xb, yb) plus remaining bits (rx, ry);
/// - if fully contained, emit one XY Morton prefix range, then prepend the time-bucket prefix.
fn cover_z2(
    orig_xy: Bits3,
    xb: u32, yb: u32,
    rx: u32, ry: u32,
    qx0: u32, qx1: u32, qy0: u32, qy1: u32,
    out_xy: &mut Vec<(u64, u64)>,
    max_ranges_soft: Option<usize>,
) {
    let x1 = if rx == 0 { xb } else { xb + ((1u32 << rx) - 1) };
    let y1 = if ry == 0 { yb } else { yb + ((1u32 << ry) - 1) };

    if quad_outside(xb, x1, yb, y1, qx0, qx1, qy0, qy1) {
        return;
    }
    if quad_inside(xb, x1, yb, y1, qx0, qx1, qy0, qy1) {
        let used = (orig_xy.lx - rx) + (orig_xy.ly - ry); // lt=0
        let prefix = morton3_prefix(xb, yb, 0, orig_xy, used); // degenerates to 2D here
        let total = (orig_xy.lx + orig_xy.ly) as u32;
        let shift = (total - used) as u64;
        let start = (prefix as u64) << shift;
        let end = if shift == 0 { start } else { start | ((1u64 << shift) - 1) };
        out_xy.push((start, end));
        return;
    }

    if let Some(limit) = max_ranges_soft {
        if out_xy.len() >= limit.saturating_mul(4) {
            let rx1 = rx.min(1);
            let ry1 = ry.min(1);
            for dx in 0..(1u32 << rx1) {
                let nx = xb + (dx << (rx.saturating_sub(1)));
                for dy in 0..(1u32 << ry1) {
                    let ny = yb + (dy << (ry.saturating_sub(1)));
                    cover_z2(orig_xy, nx, ny, rx.saturating_sub(1), ry.saturating_sub(1),
                             qx0, qx1, qy0, qy1, out_xy, None);
                }
            }
            return;
        }
    }

    let rx1 = rx.min(1);
    let ry1 = ry.min(1);
    for dx in 0..(1u32 << rx1) {
        let nx = xb + (dx << (rx.saturating_sub(1)));
        for dy in 0..(1u32 << ry1) {
            let ny = yb + (dy << (ry.saturating_sub(1)));
            cover_z2(
                orig_xy,
                nx, ny,
                rx.saturating_sub(1), ry.saturating_sub(1),
                qx0, qx1, qy0, qy1,
                out_xy,
                max_ranges_soft,
            );
        }
    }
}

pub fn encode_point_z2t(p: &SfcParams, lat: f64, lon: f64, time: u64) -> u64 {
    let Bits3 { lx, ly, .. } = p.bits;
    // xy interleave
    let ix = q_f64(lat, p.glat.0, p.glat.1, lx);
    let iy = q_f64(lon, p.glon.0, p.glon.1, ly);
    let xy = morton2_interleave_var(ix, iy, p.bits);
    // time bucket
    let mut bucket = 0u64;
    if p.time_bucket_s > 0 {
        let num_buckets = ((p.gtime.1 - p.gtime.0) + p.time_bucket_s - 1) / p.time_bucket_s;
        if num_buckets > 0 {
            let b = ((time.saturating_sub(p.gtime.0)) / p.time_bucket_s) as u64;
            bucket = b.min((num_buckets - 1) as u64);
        }
    }
    (bucket << ((p.bits.lx + p.bits.ly) as u64)) | (xy as u64)
}

pub fn ranges_for_window_z2t(
    p: &SfcParams,
    lat_min: f64, lat_max: f64,
    lon_min: f64, lon_max: f64,
    t_min: u64, t_max: u64,
) -> Vec<(u64, u64)> {
    let Bits3 { lx, ly, .. } = p.bits;
    // Quantized XY closed intervals
    let (qx0, qx1) = idx_range_f64(lat_min, lat_max, p.glat.0, p.glat.1, lx);
    let (qy0, qy1) = idx_range_f64(lon_min, lon_max, p.glon.0, p.glon.1, ly);

    // Time-bucket range (closed interval)
    let num_buckets = ((p.gtime.1 - p.gtime.0) + p.time_bucket_s - 1) / p.time_bucket_s;
    let nb = num_buckets.max(1);
    let b_min = ((t_min.saturating_sub(p.gtime.0)) / p.time_bucket_s).min(nb - 1);
    let b_max = ((t_max.saturating_sub(p.gtime.0)) / p.time_bucket_s).min(nb - 1);

    let mut all: Vec<(u64, u64)> = Vec::with_capacity(256);
    let orig_xy = Bits3 { lx, ly, lt: 0 };
    let total_xy = (lx + ly) as u32;
    let max_soft = p.max_ranges;

    for b in b_min..=b_max {
        let mut xy_ranges: Vec<(u64, u64)> = Vec::with_capacity(128);
        cover_z2(
            orig_xy,
            0, 0,
            lx, ly,
            qx0, qx1, qy0, qy1,
            &mut xy_ranges,
            max_soft,
        );
        let bucket_prefix = (b as u64) << (total_xy as u64);
        for (s_xy, e_xy) in merge_ranges(xy_ranges) {
            let s = bucket_prefix | s_xy;
            let e = bucket_prefix | e_xy;
            all.push((s, e));
        }
    }

    merge_ranges(all)
}
