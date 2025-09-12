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

/// Experiment entry point, called by main.rs
pub fn run_experiment(cfg: &Config) {
    // 1) Output directory
    let run_dir = make_run_dir();
    println!("Output dir = {}", run_dir.display());

    // 2) Build SFC parameters
    let sfc_params = build_sfc_params(cfg);

    // 3) Build network (Chord DHT baseline)
    let num_nodes = cfg.network.num_nodes.max(1);
    let tail_bits = cfg.experiment.stop_tail_bits as u8;
    let mut net = Network::new(num_nodes, sfc_params.ring_m, tail_bits);

    // 4) Load CSV -> Segment -> DHT
    let (ingest_count, lat_min, lat_max, lon_min, lon_max, ts_min, ts_max, sample_keys) =
        ingest_csv_into_network(cfg, &sfc_params, &mut net);
    println!("Inserted {} records in network.", ingest_count);

    // 5) Dump node distribution to disk
    {
        let rows = net.node_distribution_rows();
        let mut f = BufWriter::new(
            File::create(run_dir.join("node_distribution.csv")).expect("create node_distribution.csv"),
        );
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
            )
            .ok();
        }
    }

    // 6) Ingestion summary
    {
        let mut f = BufWriter::new(
            File::create(run_dir.join("ingest_summary.txt")).expect("create ingest_summary.txt"),
        );
        writeln!(f, "lat_min = {}", lat_min).ok();
        writeln!(f, "lat_max = {}", lat_max).ok();
        writeln!(f, "lon_min = {}", lon_min).ok();
        writeln!(f, "lon_max = {}", lon_max).ok();
        writeln!(f, "ts_min  = {}", ts_min).ok();
        writeln!(f, "ts_max  = {}", ts_max).ok();
    }

    // 7) Sanity check: sample 5 keys for lookup
    sanity_probe(&net, &sample_keys);

    // 8) Execute query window (using planner + QueryExecutor)
    {
        let executor = QueryExecutor::new(&net, run_dir.clone(), cfg);
        for (qi, q) in cfg.experiment.queries.iter().enumerate() {
            let name = q.name.clone().unwrap_or_else(|| format!("window_{:02}", qi));
            let plan: PlanResult = plan_window(cfg, &sfc_params, q);
            if let Err(e) = executor.run_one_window(qi, &name, q, &plan) {
                eprintln!("Query window {} failed: {}", name, e);
            }
        }
    }

    println!("All queries finished. Results at {:?}", run_dir);
}

/// Generate run directory: results/run_YYYYMMDD_HHMMSS
fn make_run_dir() -> PathBuf {
    let ts = Local::now().format("run_%Y%m%d_%H%M%S").to_string();
    let dir = PathBuf::from("results").join(ts);
    create_dir_all(&dir).expect("create results dir");
    dir
}

/// Load CSV -> Segment -> DHT
/// Returns: (count, lat_min, lat_max, lon_min, lon_max, ts_min, ts_max, sample_keys)
fn ingest_csv_into_network(
    cfg: &Config,
    sfc: &SfcParams,
    net: &mut Network,
) -> (usize, f64, f64, f64, f64, u64, u64, Vec<u64>) {
    let csv_path = &cfg.data.csv_path;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)
        .expect("open csv");

    // Parse column names (case-insensitive and synonym tolerant)
    let headers = rdr.headers().expect("read headers").clone();
    let idx_user = find_col(&headers, &["user", "uid", "user_id"]).unwrap_or(None);
    let idx_traj = find_col(&headers, &["traj_id", "trajectory_id", "tid"]).unwrap_or(None);
    let idx_lat = find_col(&headers, &["lat", "latitude"]).expect("lat column not found").expect("lat");
    let idx_lon = find_col(&headers, &["lon", "lng", "longitude"]).expect("lon column not found").expect("lon");
    let idx_dt  = find_col(&headers, &["datetime", "time", "timestamp"]).expect("datetime column not found").expect("datetime");

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
        let ts: u64  = parse_time(rec.get(idx_dt).unwrap_or(""));

        let key = encode_point(sfc, lat, lon, ts);
        let traj_id = idx_traj.and_then(|i| rec.get(i)).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
        let segment_id = count as u32;
        let user   = idx_user.and_then(|i| rec.get(i)).unwrap_or("");
        let dt_str = rec.get(idx_dt).unwrap_or(""); 
        let payload = format!("{},{},{},{},{}", user, traj_id, lat, lon, dt_str);
        let seg = Segment { traj_id, segment_id, hilbert_key: key, lat, lon, ts, payload };

        net.insert(entry_node, seg);
        count += 1;

        if count % 200_000 == 0 { println!("Inserted {} records...", count); }

        // Update boundaries
        if lat < lat_min { lat_min = lat; }
        if lat > lat_max { lat_max = lat; }
        if lon < lon_min { lon_min = lon; }
        if lon > lon_max { lon_max = lon; }
        if ts < ts_min { ts_min = ts; }
        if ts > ts_max { ts_max = ts; }

        // Sample sanity keys
        if count % 100_000 == 0 { sample_keys.push(key); }

        if count >= max_ingest {
            println!("Reached max_ingest limit: {} rows. Stopping ingest.", max_ingest);
            break;
        }
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
    // 1) Pure integer (epoch seconds)
    if let Ok(v) = s.trim().parse::<i64>() { return v.max(0) as u64; }
    // 2) RFC3339 / ISO8601 (with timezone)
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return dt.with_timezone(&Utc).timestamp().max(0) as u64;
    }
    // 3) Common format without timezone (treated as UTC)
    use chrono::NaiveDateTime;
    const FMTS: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
    ];
    for f in &FMTS {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, f) {
            let ts = ndt.and_utc().timestamp();
            return ts.max(0) as u64;
        }
    }
    // 4) Fallback: current time (avoid 0)
    Utc::now().timestamp() as u64
}

fn sanity_probe(net: &Network, sample_keys: &Vec<u64>) {
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    let mut samples = sample_keys.clone();
    samples.shuffle(&mut thread_rng());
    samples.truncate(5);
    println!("Sanity check with {} sampled keys...", samples.len());

    let total = samples.len(); // Save length before potential moves
    let mut ok = 0usize;
    for &key in samples.iter() { // Iterate by reference to avoid moving samples
        let (hits, hops) = net.query_range(0, (key, key));
        println!("  probe key {} -> hits={}, hops={}", key, hits.len(), hops);
        if !hits.is_empty() { ok += 1; }
    }
    println!("Sanity result: {}/{} keys retrievable.", ok, total.max(1));
}
