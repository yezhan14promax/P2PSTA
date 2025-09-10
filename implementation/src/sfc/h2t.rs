use super::{
    Bits3, SfcParams,
    q_f64,
    hilbert2_encode,
    merge_with_gap, merge_ranges,
};

pub fn encode_point_h2t(p:&SfcParams, lat:f64, lon:f64, time:u64)->u64{
    let Bits3{lx,ly, ..}=p.bits;
    let lh=lx.min(ly).max(1);
    let x=q_f64(lat,p.glat.0,p.glat.1,lh);
    let y=q_f64(lon,p.glon.0,p.glon.1,lh);
    let h=hilbert2_encode(x,y,lh);
    let bucket=(time.saturating_sub(p.gtime.0)/p.time_bucket_s) as u64;
    (bucket << (lh+lh)) | h as u64
}

/// 近似覆盖：每个时间桶内做 2D 采样 + 容忍间隙合并
pub fn ranges_for_window_h2t(p:&SfcParams,
    lat_min:f64, lat_max:f64, lon_min:f64, lon_max:f64, t_min:u64, t_max:u64
)->Vec<(u64,u64)>{
    let Bits3{lx,ly, ..}=p.bits;
    let lh = lx.min(ly).max(1);
    let b0 = (t_min.saturating_sub(p.gtime.0)/p.time_bucket_s) as u64;
    let b1 = (t_max.saturating_sub(p.gtime.0)/p.time_bucket_s) as u64;
    let mut all=Vec::new();
    let sx=16usize; let sy=16usize;
    for b in b0..=b1 {
        let mut keys=Vec::with_capacity(sx*sy);
        for i in 0..sx {
            let lat = if sx==1 {(lat_min+lat_max)*0.5}
                      else { lat_min + (lat_max-lat_min)*(i as f64)/(sx as f64 -1.0) };
            for j in 0..sy {
                let lon = if sy==1 {(lon_min+lon_max)*0.5}
                          else { lon_min + (lon_max-lon_min)*(j as f64)/(sy as f64 -1.0) };
                let x = q_f64(lat,p.glat.0,p.glat.1, lh);
                let y = q_f64(lon,p.glon.0,p.glon.1, lh);
                let h = hilbert2_encode(x,y,lh);
                let key = (b << (lh+lh)) as u64 | h as u64;
                keys.push(key);
            }
        }
        let mut ranges = merge_with_gap(keys, 4);
        all.append(&mut ranges);
    }
    merge_ranges(all)
}
