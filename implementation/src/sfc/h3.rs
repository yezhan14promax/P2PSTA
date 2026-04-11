use super::{
    Bits3, SfcParams,
    q_f64, q_u64,
    hilbert3_encode_equal_bits,
    merge_with_gap, // merge_ranges is no longer imported here
};

pub fn encode_point_h3(p: &SfcParams, lat: f64, lon: f64, time: u64) -> u64 {
    let Bits3{lx,ly,lt}=p.bits;
    let lh=lx.min(ly).min(lt).max(1);
    let x=q_f64(lat,p.glat.0,p.glat.1,lh);
    let y=q_f64(lon,p.glon.0,p.glon.1,lh);
    let z=q_u64(time,p.gtime.0,p.gtime.1,lh);
    hilbert3_encode_equal_bits(x,y,z,lh)
}

/// Approximate cover: sampling + gap-tolerant merging
pub fn ranges_for_window_h3(p:&SfcParams,
    lat_min:f64, lat_max:f64, lon_min:f64, lon_max:f64, t_min:u64, t_max:u64
)->Vec<(u64,u64)>{
    let Bits3{lx,ly,lt}=p.bits;
    let lh = lx.min(ly).min(lt).max(1);
    let sx=12usize; let sy=12usize; let sz=6usize;
    let mut keys=Vec::with_capacity(sx*sy*sz);
    for i in 0..sx {
        let lat = if sx==1 {(lat_min+lat_max)*0.5}
                  else { lat_min + (lat_max-lat_min)*(i as f64)/(sx as f64 -1.0) };
        for j in 0..sy {
            let lon = if sy==1 {(lon_min+lon_max)*0.5}
                      else { lon_min + (lon_max-lon_min)*(j as f64)/(sy as f64 -1.0) };
            for k in 0..sz {
                let t = if sz==1 {(t_min+t_max)/2}
                        else { t_min + (t_max-t_min)*(k as u64)/(sz as u64 -1) };
                let x = q_f64(lat,p.glat.0,p.glat.1, lh);
                let y = q_f64(lon,p.glon.0,p.glon.1, lh);
                let z = q_u64(t,p.gtime.0,p.gtime.1, lh);
                keys.push(hilbert3_encode_equal_bits(x,y,z,lh));
            }
        }
    }
    merge_with_gap(keys, 4)
}
