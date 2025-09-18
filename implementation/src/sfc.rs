use crate::config::Config;

/*
需要的 SfcConfig 最小字段集合（请确认 config.rs 与 YAML 对齐）：
pub struct SfcConfig {
    pub algorithm: String,
    pub center_lat: f64,
    pub time_bucket_s: Option<u64>,
    pub max_ranges: Option<usize>,

    // 递归/粗接收参数（可选）
    pub max_depth: Option<u32>,
    pub max_nodes: Option<usize>,
    pub tail_bits_guard: Option<u32>,
}
*/

/// ===================== Public Structures =====================

#[derive(Clone, Copy, Debug)]
pub enum SfcAlgorithm {
    Z3,   // 3D Z-order
    H3,   // 3D Hilbert (当前用 Morton 占位，接口对齐)
    Z2T,  // 2D Z-order + time bucket
    H2T,  // 2D Hilbert + time bucket (当前用 2D Morton 近似)
}

impl SfcAlgorithm {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "z3" => Self::Z3,
            "h3" => Self::H3,
            "z2t" => Self::Z2T,
            "h2t" => Self::H2T,
            _ => Self::Z3,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Bits3 { pub lx: u32, pub ly: u32, pub lt: u32 }

#[derive(Clone, Debug)]
pub struct SfcParams {
    pub algo: SfcAlgorithm,
    pub bits: Bits3,                 // z3/h3 使用三轴，z2t/h2t 只用 lx,ly
    pub glat: (f64,f64),
    pub glon: (f64,f64),
    pub gtime: (u64,u64),
    pub center_lat: f64,
    pub time_bucket_s: u64,          // 仅 z2t/h2t 使用
    pub bucket_bits: u32,            // 仅 z2t/h2t 使用
    pub max_ranges: Option<usize>,   // 查询区间上限（防爆）
    pub ring_m: usize,               // DHT/ring 有效位数
    // 递归与粗接收控制（z3/h3 用；z2t/h2t 忽略）
    pub max_depth: u32,
    pub max_nodes: usize,
    pub tail_bits_guard: u32,
}

/// ===================== Parameter Construction =====================

/// Derive SfcParams from Config (fixed bits for z3/h3; z2t/h2t uses buckets)
pub fn build_sfc_params(cfg: &Config) -> SfcParams {
    // 数据集全局范围
    let glat: (f64,f64) = cfg.dataset.lat_range;
    let glon: (f64,f64) = cfg.dataset.lon_range;
    let gtime: (u64,u64) = cfg.dataset.time_range
        .unwrap_or((1_175_308_800, 1_346_457_600)); // Geolife 2007-04-01..2012-08-31
    let dt_s: u64 = gtime.1.saturating_sub(gtime.0).max(1);

    let algo = SfcAlgorithm::from_str(&cfg.sfc.algorithm);
    let center_lat: f64 = cfg.sfc.center_lat;

    // 递归/粗接收参数（给出温和默认）
    let max_depth       = cfg.sfc.max_depth.unwrap_or(30);
    let max_nodes       = cfg.sfc.max_nodes.unwrap_or(200_000);
    let tail_bits_guard = cfg.sfc.tail_bits_guard.unwrap_or(8);

    // z2t/h2t：时间桶配置（Option<u64> → 默认 3600s）
    let time_bucket_s: u64 = cfg.sfc.time_bucket_s.unwrap_or(3600).max(1);
    let num_buckets = (dt_s + time_bucket_s - 1) / time_bucket_s;
    let mut bucket_bits = ceil_log2_u64(num_buckets).clamp(0, 31) as u32;
    bucket_bits = bucket_bits.max(1); // 至少 1 bit

    // 固定位长策略：
    // - Z3/H3: x=10, y=10, t=10 (total=30)
    // - Z2T/H2T: x=10, y=10, t 走桶（bucket_bits）
    let (bits, ring_m) = match algo {
        SfcAlgorithm::Z3 | SfcAlgorithm::H3 => {
            let b = Bits3 { lx: 10, ly: 10, lt: 10 };
            let m = (b.lx + b.ly + b.lt) as usize; // 30
            (b, m)
        }
        SfcAlgorithm::Z2T | SfcAlgorithm::H2T => {
            let b = Bits3 { lx: 10, ly: 10, lt: 0 };
            let m = (b.lx + b.ly + bucket_bits) as usize;
            (b, m)
        }
    };

    let max_ranges = cfg.sfc.max_ranges;

    SfcParams {
        algo, bits, glat, glon, gtime,
        center_lat, time_bucket_s, bucket_bits,
        max_ranges, ring_m,
        max_depth, max_nodes, tail_bits_guard,
    }
}

/// ===================== Basic Quantization =====================

#[inline]
pub(crate) fn q_f64(v: f64, mn: f64, mx: f64, L: u32) -> u32 {
    if mx <= mn || L == 0 { return 0; }
    let t = ((v - mn) / (mx - mn)).clamp(0.0, 1.0);
    let n = ((1u64 << L) - 1) as f64;
    (t * n).round() as u32
}

#[inline]
pub fn q_floor_f64(v: f64, mn: f64, mx: f64, L: u32) -> u32 {
    if mx <= mn || L == 0 { return 0; }
    let t = ((v - mn) / (mx - mn)).clamp(0.0, 1.0);
    let n = ((1u64 << L) - 1) as f64;
    (t * n).floor() as u32
}

#[inline]
pub(crate) fn q_u64(v: u64, mn: u64, mx: u64, L: u32) -> u32 {
    if mx <= mn || L == 0 { return 0; }
    let t = (v.saturating_sub(mn) as f64 / (mx - mn) as f64).clamp(0.0, 1.0);
    let n = ((1u64 << L) - 1) as f64;
    (t * n).round() as u32
}

#[inline]
pub fn q_floor_u64(v: u64, mn: u64, mx: u64, L: u32) -> u32 {
    if mx <= mn || L == 0 { return 0; }
    let t = (v.saturating_sub(mn) as f64 / (mx - mn) as f64).clamp(0.0, 1.0);
    let n = ((1u64 << L) - 1) as f64;
    (t * n).floor() as u32
}

/// ===================== Morton / Hilbert Utilities =====================

pub(crate) fn morton3_interleave_var(x: u32, y: u32, z: u32, bits: Bits3) -> u64 {
    let Bits3 { lx, ly, lt } = bits;
    let mut out: u64 = 0;
    let maxb = lx.max(ly).max(lt);
    for b in (0..maxb).rev() {
        if b < lx { out = (out << 1) | (((x >> b) & 1) as u64); }
        if b < ly { out = (out << 1) | (((y >> b) & 1) as u64); }
        if b < lt { out = (out << 1) | (((z >> b) & 1) as u64); }
    }
    out
}

pub(crate) fn morton3_interleave_take(x: u32, y: u32, z: u32, bits: Bits3, take: u32) -> u64 {
    let Bits3 { lx, ly, lt } = bits;
    let maxb = lx.max(ly).max(lt);
    let total = lx + ly + lt;
    let mut out: u64 = 0;
    let mut used = 0u32;
    for b in (0..maxb).rev() {
        if used >= take { break; }
        if b < lx { out = (out << 1) | (((x >> b) & 1) as u64); used += 1; if used >= take { break; } }
        if b < ly { out = (out << 1) | (((y >> b) & 1) as u64); used += 1; if used >= take { break; } }
        if b < lt { out = (out << 1) | (((z >> b) & 1) as u64); used += 1; if used >= take { break; } }
    }
    let shift = total.saturating_sub(take);
    out << shift
}

pub(crate) fn morton3_prefix(x: u32, y: u32, z: u32, bits: Bits3, take: u32) -> u64 {
    let full = morton3_interleave_var(x, y, z, bits);
    let total = bits.lx + bits.ly + bits.lt;
    let shift = total.saturating_sub(take);
    let mask_low = if shift >= 64 { 0 } else { (1u64 << shift).saturating_sub(1) };
    full & !mask_low
}

pub(crate) fn morton2_interleave_var(x: u32, y: u32, bits: Bits3) -> u64 {
    morton2_interleave_var_lxly(x, y, bits.lx, bits.ly)
}

pub(crate) fn morton2_interleave_var_lxly(x: u32, y: u32, lx: u32, ly: u32) -> u64 {
    let mut out: u64 = 0;
    let maxb = lx.max(ly);
    for b in (0..maxb).rev() {
        if b < lx { out = (out << 1) | (((x >> b) & 1) as u64); }
        if b < ly { out = (out << 1) | (((y >> b) & 1) as u64); }
    }
    out
}

pub(crate) fn hilbert2_encode(x:u32,y:u32,bits:u32)->u64{
    morton2_interleave_var_lxly(x,y,bits,bits)
}

pub(crate) fn hilbert3_encode_equal_bits(x:u32,y:u32,z:u32,bits:u32)->u64{
    morton3_interleave_take(x,y,z, Bits3{lx:bits,ly:bits,lt:bits}, bits*3)
}

/// ===================== Range Merging Utilities =====================

pub(crate) fn merge_with_gap(mut keys:Vec<u64>, max_gap:u64)->Vec<(u64,u64)>{
    if keys.is_empty(){ return Vec::new(); }
    keys.sort_unstable();
    let mut out: Vec<(u64,u64)> = Vec::new();
    let mut cur_start = keys[0];
    let mut cur_end   = keys[0];
    for &k in keys.iter().skip(1) {
        if k <= cur_end.saturating_add(max_gap) {
            if k > cur_end { cur_end = k; }
        } else {
            out.push((cur_start, cur_end));
            cur_start = k;
            cur_end = k;
        }
    }
    out.push((cur_start, cur_end));
    out
}

pub(crate) fn merge_ranges(mut v:Vec<(u64,u64)>)->Vec<(u64,u64)>{
    if v.is_empty(){ return v; }
    v.sort_unstable_by_key(|r| r.0);
    let mut out: Vec<(u64,u64)> = Vec::new();
    let mut cur = v[0];
    for (s,e) in v.into_iter().skip(1) {
        if s <= cur.1.saturating_add(1) {
            if e > cur.1 { cur.1 = e; }
        } else {
            out.push(cur);
            cur = (s,e);
        }
    }
    out.push(cur);
    out
}

/// ===================== Bit Shrinking Utilities =====================

#[inline] fn ceil_log2_u64(x:u64)->i32{ if x<=1 {0} else { ((x-1) as f64).log2().ceil() as i32 } }

fn shrink_bits_sum(b:&mut Bits3, limit:u32){
    let mut sum = b.lx + b.ly + b.lt;
    while sum>limit {
        if b.lt>0 { b.lt-=1; }
        else if b.lx>=b.ly && b.lx>0 { b.lx-=1; }
        else if b.ly>0 { b.ly-=1; }
        sum = b.lx + b.ly + b.lt;
    }
}
fn shrink_xy_to_limit(b:&mut Bits3, limit:u32){
    let mut sum = b.lx + b.ly;
    while sum>limit {
        if b.lx>=b.ly && b.lx>0 { b.lx-=1; }
        else if b.ly>0 { b.ly-=1; }
        sum = b.lx + b.ly;
    }
}

/// ===================== Submodule Declarations (located in src/sfc/) ===

mod z3;
pub use self::z3::encode_point_z3;
mod z2t;
mod h3;
mod h2t;

/// ===================== Public API =====================

pub fn encode_point(p:&SfcParams, lat:f64, lon:f64, time:u64)->u64{
    match p.algo {
        SfcAlgorithm::Z3  => z3::encode_point_z3(p, lat, lon, time),
        SfcAlgorithm::Z2T => z2t::encode_point_z2t(p, lat, lon, time),
        SfcAlgorithm::H3  => h3::encode_point_h3(p, lat, lon, time),
        SfcAlgorithm::H2T => h2t::encode_point_h2t(p, lat, lon, time),
    }
}

pub fn ranges_for_window(p:&SfcParams,
    lat_min:f64, lat_max:f64, lon_min:f64, lon_max:f64, t_min:u64, t_max:u64
)->Vec<(u64,u64)>{
    match p.algo {
        SfcAlgorithm::Z3  => z3::ranges_for_window_z3(p, lat_min,lat_max,lon_min,lon_max,t_min,t_max),
        SfcAlgorithm::Z2T => z2t::ranges_for_window_z2t(p, lat_min,lat_max,lon_min,lon_max,t_min,t_max),
        SfcAlgorithm::H3  => h3::ranges_for_window_h3(p, lat_min,lat_max,lon_min,lon_max,t_min,t_max),
        SfcAlgorithm::H2T => h2t::ranges_for_window_h2t(p, lat_min,lat_max,lon_min,lon_max,t_min,t_max),
    }
}
