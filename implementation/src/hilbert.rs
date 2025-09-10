/// 将 (lat, lon, time) 映射到 64 位 Morton (Z-order) key
/// Maps (lat, lon, time) to a 64-bit Morton (Z-order) key.
///
/// # 参数 / Parameters
/// - `lat`: 纬度 (latitude), 范围为 [-90.0, 90.0]
/// - `lon`: 经度 (longitude), 范围为 [-180.0, 180.0]
/// - `time`: 时间戳 (timestamp), 将被压缩到有限范围
///
/// # 返回值 / Returns
/// 返回 64 位 Morton 编码 (Z-order key)
/// Returns a 64-bit Morton encoded (Z-order) key
/// 将 (lat, lon, time) 映射到 64 位 Morton (Z-order) key
/// Maps (lat, lon, time) to a 64-bit Morton (Z-order) key.
pub fn encode_hilbert(lat: f64, lon: f64, time: u64) -> u64 {
    // 纬度转整数 (convert latitude to integer)
    let x = ((lat + 90.0) * 1e5) as u64;
    // 经度转整数 (convert longitude to integer)
    let y = ((lon + 180.0) * 1e5) as u64;
    // 时间压缩到有限范围 (compress time to a limited range)
    let t = (time % 1_000_000) as u64;
    // 生成 Morton 编码 (generate Morton code)
    let key = interleave3(x, y, t) % 1_000_000;
    interleave3(x, y, t)
}

/// 3D Morton 编码（逐位交织）
/// 3D Morton encoding (bitwise interleaving)
fn interleave3(x: u64, y: u64, z: u64) -> u64 {
    let mut answer = 0u64;
    // 每个维度 21 bit -> 总共 63 bit
    // 21 bits per dimension -> total 63 bits
    for i in 0..21 {
        answer |= ((x >> i) & 1) << (3 * i);
        answer |= ((y >> i) & 1) << (3 * i + 1);
        answer |= ((z >> i) & 1) << (3 * i + 2);
    }
    answer
}
