/// 将 (lat, lon, time) 映射到 64 位 Morton (Z-order) key
pub fn encode_hilbert(lat: f64, lon: f64, time: u64) -> u64 {
    let x = ((lat + 90.0) * 1e5) as u64;    // 纬度转整数
    let y = ((lon + 180.0) * 1e5) as u64;   // 经度转整数
    let t = (time % 1_000_000) as u64;      // 时间压缩到有限范围
    let key = interleave3(x, y, t) % 1_000_000;
    interleave3(x, y, t)
}

/// 3D Morton 编码（逐位交织）
fn interleave3(x: u64, y: u64, z: u64) -> u64 {
    let mut answer = 0u64;
    for i in 0..21 { // 每个维度 21 bit -> 总共 63 bit
        answer |= ((x >> i) & 1) << (3 * i);
        answer |= ((y >> i) & 1) << (3 * i + 1);
        answer |= ((z >> i) & 1) << (3 * i + 2);
    }
    answer
}
