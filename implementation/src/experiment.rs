use crate::config::Config;
use crate::network::Network;
use crate::node::Segment;
use crate::planner::{plan_window, PlanResult};
use crate::query::QueryExecutor;
use crate::sfc::{build_sfc_params, encode_point, SfcParams};

use chrono::{Local, Utc};
use csv::StringRecord;
use std::fs::{create_dir_all, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub fn run_experiment(cfg: &Config) {
    // 1) 输出目录
    let run_dir = make_run_dir();
    println!("Output dir = {}", run_dir.display());

    // 2) SFC 参数
    let sfc_params = build_sfc_params(cfg);

    // 3) 构建网络
    let num_nodes = cfg.network.num_nodes.max(1);
    let tail_bits = cfg.experiment.stop_tail_bits as u8;
    let mut net = Network::new(num_nodes, sfc_params.ring_m, tail_bits);

    // 4) Ingest CSV -> DHT
    let t_ingest = std::time::Instant::now();
    let (ingest_count, lat_min, lat_max, lon_min, lon_max, ts_min, ts_max, sample_keys) =
        ingest_csv_into_network(cfg, &sfc_params, &mut net);
    println!("Inserted {} records in network.", ingest_count);

    // timings
    {
        use std::io::Write;
        let mut tf = std::fs::OpenOptions::new()
            .create(true).append(true)
            .open(run_dir.join("timings.txt"))
            .expect("open timings.txt");
        writeln!(tf, "ingest_ms={}", t_ingest.elapsed().as_millis()).ok();
    }

    // 5) node_distribution.csv
    {
        let rows = net.node_distribution_rows();
        let mut f = BufWriter::new(File::create(run_dir.join("node_distribution.csv")).expect("create node_distribution.csv"));
        writeln!(f, "node_idx,node_id,total_count,min_key,max_key").ok();
        for (idx, id, total, mn, mx) in rows {
            writeln!(
                f,
                "{},{},{},{},{}",
                idx,
                id,
                total,
                mn.map(|v| v.to_string()).unwrap_or_default(),
                mx.map(|v| v.to_string()).unwrap_or_default()
            ).ok();
        }
    }

    // 5.5) 节点负责区间 + 节点数据导出
    {
        use std::io::Write;

        // 5.5.1 节点负责区间（保持不变）
        let mut f = BufWriter::new(
            File::create(run_dir.join("node_ranges.csv")).expect("create node_ranges.csv"),
        );
        writeln!(f, "node_idx,node_id,resp_start,resp_end,wrapped,stored_total,stored_min,stored_max").ok();
        for (i, id, rs, re, wrapped, total, mn, mx) in net.export_node_ranges() {
            writeln!(
                    f,
                    "{},{},{},{},{},{},{},{}",
                    i,
                    id,
                    rs,
                    re,
                    wrapped,
                    total,
                    mn.map_or(String::new(), |v| v.to_string()),
                    mx.map_or(String::new(), |v| v.to_string())
                ).ok();
        }

        // 5.5.2 节点真实数据（改为单文件 nodes_dump.csv）
        let mut ndump = BufWriter::new(
            File::create(run_dir.join("nodes_dump.csv")).expect("create nodes_dump.csv"),
        );
        // 统一表头：node_idx,node_id,key,user,traj_id,lat,lon,datetime
        writeln!(ndump, "node_idx,node_id,key,user,traj_id,lat,lon,datetime").ok();

        for (i, id, _rs, _re, _wrapped, _total, _mn, _mx) in net.export_node_ranges() {
            for seg in net.export_node_data(i) {
                // payload 约定为5列: user,traj_id,lat,lon,datetime
                let mut it = seg.payload.split(',');
                let user = it.next().unwrap_or("");
                let traj_id = seg.traj_id;      // 注意：以 Segment 字段为准，避免解析误差
                let lat = seg.lat;
                let lon = seg.lon;
                // datetime：优先用 payload 第五列；若无则用 ts
                let datetime = it.nth(3).map(|s| s.to_string()).unwrap_or_else(|| seg.ts.to_string());

                writeln!(
                    ndump,
                    "{},{},{},{},{},{},{},{}",
                    i,           // node_idx
                    id,          // node_id
                    seg.hilbert_key,
                    user,
                    traj_id,
                    lat,
                    lon,
                    datetime
                ).ok();
            }
        }
    }

    // 6) ingest_summary.txt
    {
        let mut f = BufWriter::new(File::create(run_dir.join("ingest_summary.txt")).expect("create ingest_summary.txt"));
        writeln!(f, "lat_min = {}", lat_min).ok();
        writeln!(f, "lat_max = {}", lat_max).ok();
        writeln!(f, "lon_min = {}", lon_min).ok();
        writeln!(f, "lon_max = {}", lon_max).ok();
        writeln!(f, "ts_min  = {}", ts_min).ok();
        writeln!(f, "ts_max  = {}", ts_max).ok();
    }

    // 7) 路由自检
    sanity_probe(&net, &sample_keys);

    // 8) 执行查询
    {
        let executor = QueryExecutor::new(&net, run_dir.clone(), cfg);
        for (qi, q) in cfg.experiment.queries.iter().enumerate() {
            let name = q.name.clone().unwrap_or_else(|| format!("window_{:02}", qi));
            let plan: PlanResult = plan_window(cfg, &sfc_params, q);
            let t_q = std::time::Instant::now();
            if let Err(e) = executor.run_one_window(qi, &name, q, &plan) {
                eprintln!("Query window {} failed: {}", name, e);
            }
            use std::io::Write;
            let mut tf = std::fs::OpenOptions::new()
                .create(true).append(true)
                .open(run_dir.join("timings.txt"))
                .expect("open timings.txt");
            writeln!(tf, "query[{}]_ms={}", name, t_q.elapsed().as_millis()).ok();
        }
    }

    println!("All queries finished. Results at {:?}", run_dir);
}

fn make_run_dir() -> PathBuf {
    let ts = Local::now().format("run_%Y%m%d_%H%M%S").to_string();
    let dir = PathBuf::from("results").join(ts);
    create_dir_all(&dir).expect("create results/");
    dir
}

fn ingest_csv_into_network(
    cfg: &Config,
    sfc_params: &SfcParams,
    net: &mut Network,
) -> (usize, f64, f64, f64, f64, u64, u64, Vec<u64>) {
    let csv_path = &cfg.data.csv_path;
    println!("Loading CSV: {}", csv_path);

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)
        .expect("open csv");

    let headers = rdr.headers().expect("read headers").clone();
    let idx_user = find_col(&headers, &["user", "uid", "user_id"]).unwrap_or(None);
    let idx_traj = find_col(&headers, &["traj_id", "trajectory_id", "tid"]).unwrap_or(None);
    let idx_lat  = find_col(&headers, &["lat", "latitude"]).expect("lat col").expect("lat col");
    let idx_lon  = find_col(&headers, &["lon", "lng", "longitude"]).expect("lon col").expect("lon col");
    let idx_dt   = find_col(&headers, &["datetime", "time"]).expect("datetime col").expect("datetime col");

    let mut count: usize = 0;
    let max_ingest = cfg.data.max_ingest.unwrap_or(usize::MAX);

    let mut lat_min = f64::INFINITY;
    let mut lat_max = f64::NEG_INFINITY;
    let mut lon_min = f64::INFINITY;
    let mut lon_max = f64::NEG_INFINITY;
    let mut ts_min: u64 = u64::MAX;
    let mut ts_max: u64 = 0;

    let mut sample_keys: Vec<u64> = Vec::new();
    let entry_node: usize = 0;

    for result in rdr.records() {
        let rec = match result { Ok(r) => r, Err(_) => continue };

        let lat: f64 = match rec.get(idx_lat).and_then(|s| s.parse().ok()) { Some(v) => v, None => continue };
        let lon: f64 = match rec.get(idx_lon).and_then(|s| s.parse().ok()) { Some(v) => v, None => continue };
        let ts:  u64 = parse_time(rec.get(idx_dt).unwrap_or(""));

        let traj_id: u64 = idx_traj.and_then(|i| rec.get(i)).and_then(|s| s.parse().ok()).unwrap_or(0);
        let user   = idx_user.and_then(|i| rec.get(i)).unwrap_or("");

        let key = encode_point(sfc_params, lat, lon, ts);

        // payload: 5 列，便于直接写 query_results.csv
        let payload = format!("{},{},{},{},{}", user, traj_id, lat, lon, rec.get(idx_dt).unwrap_or(""));

        let seg = Segment {
            traj_id,
            segment_id: 0,
            hilbert_key: key,
            payload,
            lat,
            lon,
            ts,
        };

        net.insert(entry_node, seg);
        count += 1;

        // stats
        if lat < lat_min { lat_min = lat; }
        if lat > lat_max { lat_max = lat; }
        if lon < lon_min { lon_min = lon; }
        if lon > lon_max { lon_max = lon; }
        if ts < ts_min { ts_min = ts; }
        if ts > ts_max { ts_max = ts; }

        if sample_keys.len() < 100 { sample_keys.push(key); }
        if count % 100000 == 0 { println!("Ingested {} rows...", count); }
        if count >= max_ingest { println!("Reached max_ingest: {}", max_ingest); break; }
    }

    if count == 0 {
        lat_min = 0.0; lat_max = 0.0; lon_min = 0.0; lon_max = 0.0; ts_min = 0; ts_max = 0;
    }
    (count, lat_min, lat_max, lon_min, lon_max, ts_min, ts_max, sample_keys)
}

fn find_col(headers: &StringRecord, names: &[&str]) -> Option<Option<usize>> {
    let lower: Vec<String> = headers.iter().map(|s| s.to_lowercase()).collect();
    for cand in names {
        if let Some(i) = lower.iter().position(|h| h == cand) { return Some(Some(i)); }
    }
    None
}

fn parse_time(s: &str) -> u64 {
    // 1) 纯数字秒
    if let Ok(v) = s.trim().parse::<i64>() { return v.max(0) as u64; }
    // 2) RFC3339
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return dt.with_timezone(&Utc).timestamp().max(0) as u64;
    }
    // 3) 常见无时区格式（按 UTC 解析）
    use chrono::NaiveDateTime;
    const FMTS: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ];
    for fmt in FMTS {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return chrono::DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc).timestamp() as u64;
        }
    }
    0
}

// ========== sanity ==========
use rand::seq::SliceRandom;
use rand::rng;

fn sanity_probe(net: &Network, sample_keys: &Vec<u64>) {
    let mut rng = rand::rng();
    let mut samples = sample_keys.clone();
    samples.shuffle(&mut rng);
    samples.truncate(5);
    println!("Sanity check with {} sampled keys...", samples.len());

    let mut ok = 0usize;
    for &key in samples.iter() {
        let (hits, hops) = net.query_range(0, (key, key));
        println!("  probe key {} -> hits={}, hops={}", key, hits.len(), hops);
        if !hits.is_empty() { ok += 1; }
    }
    println!("Sanity result: {}/{} keys retrievable.", ok, samples.len().max(1));
}
