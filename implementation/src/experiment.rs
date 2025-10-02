use crate::config::Config;
use crate::network::Network;
use crate::node::Segment;
use crate::planner::{plan_window, PlanResult};
use crate::query::QueryExecutor;
use crate::sfc::{build_sfc_params, encode_point, SfcParams};
use crate::smart::SmartDirect; // ⬅ 新增：snode 模式
use chrono::{Local, Utc};
use csv::StringRecord;
use std::fs::{create_dir_all, File};
use std::io::{BufWriter, Write};
use std::path::{PathBuf, Path};
use std::collections::HashMap;
use crate::vnode::VNetwork;
use crate::placement::Placement;

pub fn run_experiment(cfg: &Config) {
    // 1) 输出目录
    let run_dir = make_run_dir();
    println!("Output dir = {}", run_dir.display());

    // 2) SFC 参数
    let sfc_params = build_sfc_params(cfg);

    // 3) 构建网络（placement 可选：baseline | vnode | snode）
    let pnodes = cfg.network.num_nodes.max(1);
    let tail_bits = cfg.experiment.stop_tail_bits as u8;
    let m = sfc_params.ring_m;

    let mut net_box: Box<dyn Placement> = match cfg.placement.mode.as_str() {
        "vnode" => {
            let v = cfg.placement.vnodes_per_node.unwrap_or(8);
            Box::new(VNetwork::new(pnodes, v, m, tail_bits))
        }
        "snode" => {
            // ⬅ SmartDirect：导入阶段先缓存，ingest 后 finalize()
            Box::new(SmartDirect::new(pnodes, m, tail_bits))
        }
        _ => {
            // baseline
            Box::new(Network::new(pnodes, m, tail_bits))
        }
    };

    // 4) Ingest CSV -> Placement
    let t_ingest = std::time::Instant::now();
    let (ingest_count, lat_min, lat_max, lon_min, lon_max, ts_min, ts_max, sample_keys) =
        ingest_csv_into_network(cfg, &sfc_params, &mut *net_box);
    println!("Inserted {} records in network.", ingest_count);

    // 👉 snode 模式在 ingest 之后 finalize（两阈值连续装箱 + pnode finger table 重建）
    if cfg.placement.mode.eq_ignore_ascii_case("snode") {
        let low = cfg.placement.smart.as_ref().and_then(|s| s.low_ratio).unwrap_or(0.95);
        let high = cfg.placement.smart.as_ref().and_then(|s| s.high_ratio).unwrap_or(1.10);
        if let Some(sd) = net_box.as_any_mut().downcast_mut::<SmartDirect>() {
            sd.finalize(low, high);
            println!("SmartDirect finalized with low_ratio={:.3}, high_ratio={:.3}", low, high);
        } else {
            eprintln!("[warn] placement.mode=snode but downcast to SmartDirect failed; skip finalize()");
        }
    }

    // timings
    {
        let mut tf = std::fs::OpenOptions::new()
            .create(true).append(true)
            .open(run_dir.join("timings.txt"))
            .expect("open timings.txt");
        writeln!(tf, "ingest_ms={}", t_ingest.elapsed().as_millis()).ok();
    }

    // 5) 导出 CSV（按模式区分：vnode | snode | baseline）
    {
        // 判定模式
        let is_vnode = cfg.placement.mode.eq_ignore_ascii_case("vnode");
        let is_snode = cfg.placement.mode.eq_ignore_ascii_case("snode");

        // 公共数据（pnode 聚合）
        let pnode_rows = net_box.node_distribution_rows();        // (idx,id,total,mn,mx)
        // vnode 明细（仅 vnode 模式使用；在 snode 下我们不再输出 vnode 列）
        let vnode_details = net_box.export_pnode_vnode_details(); // (pi,pid,vi,vid,rs,re,wrap,tot,mn,mx)

        #[inline]
        fn key_in_range(key: u64, start: u64, end: u64, wrapped: bool) -> bool {
            if !wrapped { key >= start && key <= end } else { key >= start || key <= end }
        }

        // ---------- 5.1) node_distribution.csv ----------
        if is_vnode {
            // vnode 模式：vnode 级别
            let mut f = BufWriter::new(File::create(run_dir.join("node_distribution.csv"))
                .expect("create node_distribution.csv"));
            writeln!(f, "pnode_idx,vnode_idx,vnode_id,total_count,min_key,max_key").ok();
            for (pi, _pid, vi, vid, _rs, _re, _wrapped, total, mn, mx) in vnode_details.iter().cloned() {
                writeln!(
                    f, "{},{},{},{},{},{}",
                    pi, vi, vid, total,
                    mn.map(|v| v.to_string()).unwrap_or_default(),
                    mx.map(|v| v.to_string()).unwrap_or_default()
                ).ok();
            }
        } else {
            // snode | baseline：pnode 级别
            let mut f = BufWriter::new(File::create(run_dir.join("node_distribution.csv"))
                .expect("create node_distribution.csv"));
            writeln!(f, "pnode_idx,node_id,total_count,min_key,max_key").ok();
            for (idx, id, total, mn, mx) in pnode_rows {
                writeln!(
                    f, "{},{},{},{},{}",
                    idx, id, total,
                    mn.map(|v| v.to_string()).unwrap_or_default(),
                    mx.map(|v| v.to_string()).unwrap_or_default()
                ).ok();
            }
        }

        // ---------- 5.2) node_ranges.csv ----------
        if is_vnode {
            // vnode 模式：vnode 级别
            let mut f = BufWriter::new(File::create(run_dir.join("node_ranges.csv"))
                .expect("create node_ranges.csv"));
            writeln!(f, "pnode_idx,vnode_idx,vnode_id,resp_start,resp_end,wrapped,stored_total,stored_min,stored_max").ok();
            for (pi, _pid, vi, vid, rs, re, wrapped, total, mn, mx) in vnode_details.iter().cloned() {
                writeln!(
                    f, "{},{},{},{},{},{},{},{},{}",
                    pi, vi, vid, rs, re, wrapped, total,
                    mn.map_or(String::new(), |v| v.to_string()),
                    mx.map_or(String::new(), |v| v.to_string())
                ).ok();
            }
        } else {
            // snode | baseline：pnode 级别（SmartDirect 的 export_node_ranges 已按 pnode 输出）
            let mut f = BufWriter::new(File::create(run_dir.join("node_ranges.csv"))
                .expect("create node_ranges.csv"));
            writeln!(f, "pnode_idx,node_id,resp_start,resp_end,wrapped,stored_total,stored_min,stored_max").ok();
            for (i, id, rs, re, wrapped, total, mn, mx) in net_box.export_node_ranges() {
                writeln!(
                    f, "{},{},{},{},{},{},{},{}",
                    i, id, rs, re, wrapped, total,
                    mn.map_or(String::new(), |v| v.to_string()),
                    mx.map_or(String::new(), |v| v.to_string())
                ).ok();
            }
        }

        // ---------- 5.3) node_dump.csv ----------
        let mut ndump = BufWriter::new(File::create(run_dir.join("node_dump.csv"))
            .expect("create node_dump.csv"));

        if is_vnode {
            // vnode 模式：带 vnode 列
            writeln!(ndump, "pnode_idx,vnode_idx,vnode_id,key,user,traj_id,lat,lon,datetime").ok();
            // 为避免重复导出，同一个 pnode 的数据只取一次
            let mut cache: std::collections::HashMap<usize, Vec<&crate::node::Segment>> = std::collections::HashMap::new();
            for (pi, _pid, vi, vid, rs, re, wrapped, _total, _mn, _mx) in vnode_details.iter().cloned() {
                let segs = cache.entry(pi).or_insert_with(|| net_box.export_node_data(pi));
                for seg in segs.iter().copied() {
                    if key_in_range(seg.sfc_key, rs, re, wrapped) {
                        let mut it = seg.payload.split(',');
                        let user = it.next().unwrap_or("");
                        let traj_id = seg.traj_id;
                        let lat = seg.lat;
                        let lon = seg.lon;
                        let datetime = it.nth(3).map(|s| s.to_string()).unwrap_or_else(|| seg.ts.to_string());
                        writeln!(
                            ndump, "{},{},{},{},{},{},{},{},{}",
                            pi, vi, vid, seg.sfc_key, user, traj_id, lat, lon, datetime
                        ).ok();
                    }
                }
            }
        } else {
            // snode | baseline：pnode 级别
            writeln!(ndump, "pnode_idx,node_id,key,user,traj_id,lat,lon,datetime").ok();
            for (i, id, _rs, _re, _wrapped, _total, _mn, _mx) in net_box.export_node_ranges() {
                for seg in net_box.export_node_data(i) {
                    let mut it = seg.payload.split(',');
                    let user = it.next().unwrap_or("");
                    let traj_id = seg.traj_id;
                    let lat = seg.lat;
                    let lon = seg.lon;
                    let datetime = it.nth(3).map(|s| s.to_string()).unwrap_or_else(|| seg.ts.to_string());
                    writeln!(
                        ndump, "{},{},{},{},{},{},{},{}",
                        i, id, seg.sfc_key, user, traj_id, lat, lon, datetime
                    ).ok();
                }
            }
        }
    }

    // 6) ingest_summary.txt and params.txt
    {
        let mut f = BufWriter::new(File::create(run_dir.join("ingest_summary.txt"))
            .expect("create ingest_summary.txt"));
        writeln!(f, "lat_min = {}", lat_min).ok();
        writeln!(f, "lat_max = {}", lat_max).ok();
        writeln!(f, "lon_min = {}", lon_min).ok();
        writeln!(f, "lon_max = {}", lon_max).ok();
        writeln!(f, "ts_min  = {}", ts_min).ok();
        writeln!(f, "ts_max  = {}", ts_max).ok();
    }
    write_params_snapshot(&run_dir, cfg);

    // 7) 路由自检（SmartDirect 会在 finalize 后重建 pnode 指定的 finger table）
    sanity_probe(&*net_box, &sample_keys);

    // 8) 执行查询
    {
        let executor = QueryExecutor::new(&*net_box, run_dir.clone(), &cfg);
        for (qi, q) in cfg.experiment.queries.iter().enumerate() {
            let name = q.name.clone().unwrap_or_else(|| format!("window_{:02}", qi));
            let plan: PlanResult = plan_window(cfg, q);
            let t_q = std::time::Instant::now();

            let ranges_merged: &[(u64, u64)] = &plan.ranges_merged;
            let raw_ranges_len: usize = plan.ranges_raw.len();
            let t_start_s: u64 = plan.t_start_s;
            let t_end_s: u64 = plan.t_end_s;

            if let Err(e) = executor.run_one_window(
                qi, &name, q, ranges_merged, raw_ranges_len, t_start_s, t_end_s,
            ) {
                eprintln!("Query window {} failed: {}", name, e);
            }

            let mut tf = std::fs::OpenOptions::new()
                .create(true).append(true)
                .open(run_dir.join("timings.txt"))
                .expect("open timings.txt");
            writeln!(tf, "query[{}]_ms={}", name, t_q.elapsed().as_millis()).ok();
        }
    }

    println!("All queries finished. Results at {:?}", run_dir);
}

fn write_params_snapshot(run_dir: &Path, cfg: &crate::config::Config) {
    let params_path = run_dir.join("params.txt");
    let file = match File::create(&params_path) {
        Ok(f) => f,
        Err(e) => { eprintln!("create params.txt failed: {e}"); return; }
    };
    let mut f = BufWriter::new(file);
    let sfc_params = crate::sfc::build_sfc_params(cfg);

    _ = writeln!(f, "[Global Config]");
    _ = writeln!(f, "data.csv_path={}", cfg.data.csv_path);
    _ = writeln!(f, "data.max_ingest={:?}", cfg.data.max_ingest);

    _ = writeln!(f, "\n[SFC Config]");
    _ = writeln!(f, "algorithm={}", cfg.sfc.algorithm);
    _ = writeln!(f, "center_lat={}", cfg.sfc.center_lat);
    _ = writeln!(f, "time_bucket_s={:?}", cfg.sfc.time_bucket_s);
    _ = writeln!(f, "max_ranges={:?}", cfg.sfc.max_ranges);
    _ = writeln!(f, "max_depth={:?}", cfg.sfc.max_depth);
    _ = writeln!(f, "max_nodes={:?}", cfg.sfc.max_nodes);
    _ = writeln!(f, "tail_bits_guard={:?}", cfg.sfc.tail_bits_guard);

    _ = writeln!(f, "\n[Network Config]");
    _ = writeln!(f, "num_nodes={}", cfg.network.num_nodes);

    _ = writeln!(f, "\n[Placement Config]");
    _ = writeln!(f, "mode={}", cfg.placement.mode);
    _ = writeln!(f, "vnodes_per_node={}", cfg.placement.vnodes_per_node.unwrap_or(8));
    if let Some(s) = &cfg.placement.smart {
        _ = writeln!(f, "smart.low_ratio={:?}", s.low_ratio);
        _ = writeln!(f, "smart.high_ratio={:?}", s.high_ratio);
    }

    _ = writeln!(
        f,
        "derived_bits: lx={} ly={} lt={} ring_m={}",
        sfc_params.bits.lx, sfc_params.bits.ly, sfc_params.bits.lt, sfc_params.ring_m
    );
    _ = writeln!(
        f,
        "global_lat=[{}, {}] global_lon=[{}, {}] global_time=[{}, {}]",
        sfc_params.glat.0,  sfc_params.glat.1,
        sfc_params.glon.0,  sfc_params.glon.1,
        sfc_params.gtime.0, sfc_params.gtime.1
    );

    _ = writeln!(f, "\n[Experiment Config]");
    _ = writeln!(f, "stop_tail_bits={}", cfg.experiment.stop_tail_bits);
    _ = writeln!(f, "print_first={:?}", cfg.experiment.print_first);
    _ = writeln!(f, "debug={:?}", cfg.experiment.debug);
    _ = writeln!(f, "prefix_bits={:?}", cfg.experiment.prefix_bits);

    _ = writeln!(f, "\n[Metrics]");
    _ = writeln!(f, "precise_hits={:?}", cfg.experiment.metrics.precise_hits);
    _ = writeln!(f, "save_with_nodes={:?}", cfg.experiment.metrics.save_with_nodes);

    _ = writeln!(f, "\n[Queries] total={}", cfg.experiment.queries.len());
    for (i, q) in cfg.experiment.queries.iter().enumerate() {
        _ = writeln!(
            f,
            "Q{:02}: name={:?}, lat=[{},{}], lon=[{},{}], time=[{},{}]",
            i, q.name, q.lat_min, q.lat_max, q.lon_min, q.lon_max, q.t_start, q.t_end
        );
    }
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
    net: &mut dyn Placement,
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

    // 对“同一原始轨迹串”计数，生成单调的 segment_id
    use std::collections::HashMap;
    let mut seg_counter: HashMap<String, u32> = HashMap::new();

    for result in rdr.records() {
        if count >= max_ingest { break; }
        let rec = match result { Ok(r) => r, Err(_) => continue };

        let lat: f64 = match rec.get(idx_lat).and_then(|s| s.parse().ok()) { Some(v) => v, None => continue };
        let lon: f64 = match rec.get(idx_lon).and_then(|s| s.parse().ok()) { Some(v) => v, None => continue };
        let dt_str   = rec.get(idx_dt).unwrap_or("").trim();
        let ts:  u64 = parse_time(dt_str);

        let user_str = idx_user.and_then(|i| rec.get(i)).unwrap_or("").trim().to_string();
        let traj_str = idx_traj.and_then(|i| rec.get(i)).unwrap_or("").trim();

        let traj_id: u64 = if traj_str.is_empty() {
            stable_u64_from_str(&format!("{}_{}", user_str, dt_str))
        } else { stable_u64_from_str(traj_str) };

        let sid_ref = seg_counter.entry(traj_str.to_string()).or_insert(0);
        let segment_id: u32 = *sid_ref;
        *sid_ref = sid_ref.saturating_add(1);

        let key = encode_point(sfc_params, lat, lon, ts);
        let payload = format!("{},{},{},{},{}", user_str, traj_str, lat, lon, dt_str);

        let seg = Segment {
            user: user_str,
            traj_id,
            segment_id,
            sfc_key: key,
            payload,
            lat,
            lon,
            ts,
        };

        net.insert(entry_node, seg);
        count += 1;

        if count % 50_000 == 0 { sample_keys.push(key); }
        if count % 400_000 == 0 { println!("Ingested {} rows...", count); }

        if lat < lat_min { lat_min = lat; }
        if lat > lat_max { lat_max = lat; }
        if lon < lon_min { lon_min = lon; }
        if lon > lon_max { lon_max = lon; }
        if ts  < ts_min  { ts_min  = ts;  }
        if ts  > ts_max  { ts_max  = ts;  }
    }

    if sample_keys.is_empty() && count > 0 {
        sample_keys.push(encode_point(sfc_params, (lat_min + lat_max) * 0.5, (lon_min + lon_max) * 0.5, ts_min));
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

#[inline]
fn stable_u64_from_str(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in s.as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn parse_time(s: &str) -> u64 {
    // 1) 纯数字秒
    if let Ok(v) = s.trim().parse::<i64>() { return v.max(0) as u64; }
    // 2) RFC3339
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return dt.with_timezone(&Utc).timestamp().max(0) as u64;
    }
    // 3) 无时区常见格式（按 UTC 解析）
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

fn sanity_probe(net: &dyn Placement, sample_keys: &Vec<u64>) {
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
