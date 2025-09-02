use std::fs;
use std::path::Path;
use chrono::NaiveDateTime;

/// 读取一个 .plt 文件，返回轨迹点
pub fn load_plt_file(path: &Path) -> Vec<(f64, f64, u64)> {
    let mut points = Vec::new();

    if let Ok(content) = fs::read_to_string(path) {
        for (i, line) in content.lines().enumerate() {
            if i < 6 { continue; } // 跳过前6行头信息
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() < 7 { continue; }

            let lat: f64 = parts[0].parse().unwrap_or(0.0);
            let lon: f64 = parts[1].parse().unwrap_or(0.0);
            let date = parts[5].trim();
            let time = parts[6].trim();

            if let Ok(dt) = NaiveDateTime::parse_from_str(
                &format!("{} {}", date, time),
                "%Y-%m-%d %H:%M:%S",
            ) {
                let timestamp = dt.timestamp() as u64;
                points.push((lat, lon, timestamp));
            }
        }
    }
    points
}

/// 遍历 Data 目录，加载所有用户的轨迹
pub fn load_geolife_dataset(base_dir: &str) -> Vec<(f64, f64, u64)> {
    let mut dataset = Vec::new();

    for user_dir in fs::read_dir(base_dir).unwrap() {
        let user_dir = user_dir.unwrap();
        let traj_dir = user_dir.path().join("Trajectory");
        if traj_dir.exists() {
            for entry in fs::read_dir(traj_dir).unwrap() {
                let entry = entry.unwrap();
                if entry.path().extension().map(|s| s == "plt").unwrap_or(false) {
                    let pts = load_plt_file(&entry.path());
                    dataset.extend(pts);
                }
            }
        }
    }
    dataset
}
