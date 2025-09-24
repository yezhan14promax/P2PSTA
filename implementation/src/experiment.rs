use crate::config::{Config};
use crate::network::Network;
use crate::node::Segment;
use crate::planner::{plan_window, PlanResult};
use crate::query::QueryExecutor;
use crate::sfc::{build_sfc_params, encode_point, SfcParams};

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

    // 3) 构建网络
    let num_nodes = cfg.network.num_nodes.max(1);
    let tail_bits = cfg.experiment.stop_tail_bits as u8;
    let pnodes = cfg.network.num_nodes;
    let tail_bits = cfg.experiment.stop_tail_bits as u8;
    let m = sfc_params.ring_m;

    let mut net_box: Box<dyn Placement> = match cfg.placement.mode.as_str() {
        "vnode" => {
            let v = cfg.placement.vnodes_per_node.unwrap_or(8);
            Box::new(VNetwork::new(pnodes, v, m, tail_bits))
        }
        _ => {
            // baseline
            Box::new(Network::new(pnodes, m, tail_bits))
        }
    };


    // 4) Ingest CSV -> DHT
    let t_ingest = std::time::Instant::now();
    let (ingest_count, lat_min, lat_max, lon_min, lon_max, ts_min, ts_max, sample_keys) =
        ingest_csv_into_network(cfg, &sfc_params, &mut *net_box);
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

    // 5) vnode-aware 导出：按模式写不同 CSV
    {
        use std::io::Write;
        use std::collections::HashMap;

        // 判定是否 vnode 模式（也可用 details 行数 > pnode 行数判断）
        let is_vnode = cfg.placement.mode.eq_ignore_ascii_case("vnode");

        // 准备数据
        let pnode_rows = net_box.node_distribution_rows();            // baseline 用： (idx,id,total,mn,mx)
        let vnode_details = net_box.export_pnode_vnode_details();     // vnode 用： (pi,pid,vi,vid,rs,re,wrap,tot,mn,mx)

        // 小工具：判断 key 是否命中 [resp_start..=resp_end]（考虑 wrap）
        #[inline]
        fn key_in_range(key: u64, start: u64, end: u64, wrapped: bool) -> bool {
            if !wrapped { key >= start && key <= end } else { key >= start || key <= end }
        }

        // ---------- 5.1) node_distribution.csv ----------
        if is_vnode {
            // vnode 模式（每行 = 一个 vNode）
            let mut f = BufWriter::new(
                File::create(run_dir.join("node_distribution.csv")).expect("create node_distribution.csv")
            );
            // 表头：pnode_idx,vnode_idx,vnode_id,total_count,min_key,max_key
            writeln!(f, "pnode_idx,vnode_idx,vnode_id,total_count,min_key,max_key").ok();

            // 关键：按 vNode 详情解构 (pi, pid, vi, vid, rs, re, wrapped, total, mn, mx)
            for (pi, _pid, vi, vid, _rs, _re, _wrapped, total, mn, mx) in vnode_details.iter().cloned() {
                writeln!(
                    f, "{},{},{},{},{},{}",
                    pi,
                    vi,                     // vnode_idx
                    vid,                    // vnode_id
                    total,
                    mn.map(|v| v.to_string()).unwrap_or_default(),
                    mx.map(|v| v.to_string()).unwrap_or_default()
                ).ok();
            }

        } else {
            // baseline（每行 = 一个 pNode）
            let mut f = BufWriter::new(
                File::create(run_dir.join("node_distribution.csv")).expect("create node_distribution.csv")
            );
            writeln!(f, "pnode_idx,node_id,total_count,min_key,max_key").ok();
            for (idx, id, total, mn, mx) in pnode_rows {
                writeln!(
                    f, "{},{},{},{},{}",
                    idx,
                    id,
                    total,
                    mn.map(|v| v.to_string()).unwrap_or_default(),
                    mx.map(|v| v.to_string()).unwrap_or_default()
                ).ok();
            }
        }

        // ---------- 5.2) node_ranges.csv ----------
        if is_vnode {
            // vnode 模式（每行 = 一个 vNode 的负责区间 + 存储统计）
            let mut f = BufWriter::new(
                File::create(run_dir.join("node_ranges.csv")).expect("create node_ranges.csv")
            );
            // 表头：pnode_idx,vnode_idx,vnode_id,resp_start,resp_end,wrapped,stored_total,stored_min,stored_max
            writeln!(f, "pnode_idx,vnode_idx,vnode_id,resp_start,resp_end,wrapped,stored_total,stored_min,stored_max").ok();

            // 解构 vNode 详情
            for (pi, _pid, vi, vid, rs, re, wrapped, total, mn, mx) in vnode_details.iter().cloned() {
                writeln!(
                    f, "{},{},{},{},{},{},{},{},{}",
                    pi,
                    vi,             // vnode_idx
                    vid,            // vnode_id
                    rs,
                    re,
                    wrapped,
                    total,
                    mn.map_or(String::new(), |v| v.to_string()),
                    mx.map_or(String::new(), |v| v.to_string())
                ).ok();
            }

        } else {
            // baseline
            let mut f = BufWriter::new(
                File::create(run_dir.join("node_ranges.csv")).expect("create node_ranges.csv")
            );
            writeln!(f, "pnode_idx,node_id,resp_start,resp_end,wrapped,stored_total,stored_min,stored_max").ok();
            for (i, id, rs, re, wrapped, total, mn, mx) in net_box.export_node_ranges() {
                writeln!(
                    f, "{},{},{},{},{},{},{},{}",
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
        }

        // ---------- 5.3) node_dump.csv ----------
        // 统一写成 node_dump.csv；vnode 模式按 vNode 负责区间过滤
        let mut ndump = BufWriter::new(
            File::create(run_dir.join("node_dump.csv")).expect("create node_dump.csv")
        );

        if is_vnode {
            // 表头：pnode_idx,vnode_idx,vnode_id,key,user,traj_id,lat,lon,datetime
            writeln!(ndump, "pnode_idx,vnode_idx,vnode_id,key,user,traj_id,lat,lon,datetime").ok();

            // 缓存：每个 pnode 的数据，只取一次
            let mut cache: std::collections::HashMap<usize, Vec<&crate::node::Segment>> =
                std::collections::HashMap::new();

            // 解构 vNode 详情
            for (pi, _pid, vi, vid, rs, re, wrapped, _total, _mn, _mx) in vnode_details.iter().cloned() {
                let segs = cache.entry(pi).or_insert_with(|| net_box.export_node_data(pi));
                for seg in segs.iter().copied() {
                    if key_in_range(seg.sfc_key, rs, re, wrapped) {
                        // payload: user,traj_id,lat,lon,datetime
                        let mut it = seg.payload.split(',');
                        let user = it.next().unwrap_or("");
                        let traj_id = seg.traj_id;
                        let lat = seg.lat;
                        let lon = seg.lon;
                        let datetime = it.nth(3).map(|s| s.to_string()).unwrap_or_else(|| seg.ts.to_string());

                        writeln!(
                            ndump, "{},{},{},{},{},{},{},{},{}",
                            pi,     // pnode_idx
                            vi,     // vnode_idx
                            vid,    // vnode_id
                            seg.sfc_key,
                            user,
                            traj_id,
                            lat,
                            lon,
                            datetime
                        ).ok();
                    }
                }
            }
        } else {
            // baseline：保持 pnode 口径
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
                        i,           // pnode_idx
                        id,          // node_id
                        seg.sfc_key,
                        user,
                        traj_id,
                        lat,
                        lon,
                        datetime
                    ).ok();
                }
            }
        }
    }


    // 6) ingest_summary.txt and params.txt
    // ingest_summary.txt
    {
        let mut f = BufWriter::new(File::create(run_dir.join("ingest_summary.txt")).expect("create ingest_summary.txt"));
        writeln!(f, "lat_min = {}", lat_min).ok();
        writeln!(f, "lat_max = {}", lat_max).ok();
        writeln!(f, "lon_min = {}", lon_min).ok();
        writeln!(f, "lon_max = {}", lon_max).ok();
        writeln!(f, "ts_min  = {}", ts_min).ok();
        writeln!(f, "ts_max  = {}", ts_max).ok();
    }
    // params.txt
    write_params_snapshot(&run_dir, cfg);

    // 7) 路由自检
    sanity_probe(&*net_box, &sample_keys);

    // 8) 执行查询
    {
        let executor = QueryExecutor::new(&*net_box, run_dir.clone(), &cfg);
        for (qi, q) in cfg.experiment.queries.iter().enumerate() {
            let name = q.name.clone().unwrap_or_else(|| format!("window_{:02}", qi));
            // 这里把三参调用改为二参（与 planner.rs 新签名一致）
            let plan: PlanResult = plan_window(cfg, q);
            let t_q = std::time::Instant::now();

            // 展开把 PlanResult 的字段传给 run_one_window（与你当前 query.rs 对齐）
            let ranges_merged: &[(u64, u64)] = &plan.ranges_merged;
            let raw_ranges_len: usize = plan.ranges_raw.len();
            let t_start_s: u64 = plan.t_start_s;
            let t_end_s: u64 = plan.t_end_s;

            if let Err(e) = executor.run_one_window(
                qi,
                &name,
                q,
                ranges_merged,
                raw_ranges_len,
                t_start_s,
                t_end_s,
            ) {
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

fn write_params_snapshot(run_dir: &Path, cfg: &crate::config::Config) {
    let params_path = run_dir.join("params.txt");

    // 打开文件（失败就报错但不 panic）
    let file = match File::create(&params_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("create params.txt failed: {e}");
            return;
        }
    };
    let mut f = BufWriter::new(file);

    // 计算一次“推导后的 SFC 参数”（位数、环宽、全局范围等）
    let sfc_params = crate::sfc::build_sfc_params(cfg);

    // 写配置快照 —— 全部用 `_ = writeln!(...)`，避免 `?` 引起编译器抱怨
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

    // 推导出的位数与环宽等（来自 SfcParams）
    _ = writeln!(
        f,
        "derived_bits: lx={} ly={} lt={} ring_m={}",
        sfc_params.bits.lx, sfc_params.bits.ly, sfc_params.bits.lt, sfc_params.ring_m
    );
    _ = writeln!(
        f,
        "global_lat=[{}, {}] global_lon=[{}, {}] global_time=[{}, {}]",
        sfc_params.glat.0,
        sfc_params.glat.1,
        sfc_params.glon.0,
        sfc_params.glon.1,
        sfc_params.gtime.0,
        sfc_params.gtime.1
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

        // 读取基础字段
        let lat: f64 = match rec.get(idx_lat).and_then(|s| s.parse().ok()) { Some(v) => v, None => continue };
        let lon: f64 = match rec.get(idx_lon).and_then(|s| s.parse().ok()) { Some(v) => v, None => continue };
        let dt_str   = rec.get(idx_dt).unwrap_or("").trim();
        let ts:  u64 = parse_time(dt_str);

        // user 用原始字符串（可以保留前导零）
        let user_str = idx_user.and_then(|i| rec.get(i)).unwrap_or("").trim().to_string();

        // 轨迹“原始串”——例如 "000/20081023025304"
        let traj_str = idx_traj.and_then(|i| rec.get(i)).unwrap_or("").trim();

        // 1) 稳定哈希成 traj_id（不会全部变 0，也能复现实验）
        let traj_id: u64 = if traj_str.is_empty() {
            stable_u64_from_str(&format!("{}_{}", user_str, dt_str)) // 兜底：用 (user, datetime)
        } else {
            stable_u64_from_str(traj_str)
        };

        // 2) 同一轨迹内的 segment_id 单调自增
        let sid_ref = seg_counter.entry(traj_str.to_string()).or_insert(0);
        let segment_id: u32 = *sid_ref;
        *sid_ref = sid_ref.saturating_add(1);

        // 3) 计算 SFC key（Z3）
        let key = encode_point(sfc_params, lat, lon, ts);

        // 4) payload：保留原始 5 列，便于导出与审计
        let payload = format!("{},{},{},{},{}", user_str, traj_str, lat, lon, dt_str);

        // 5) 构造 Segment（需要 node.rs 里的 Segment 有 user:String / sfc_key / payload 等字段）
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

        // 插入网络
        net.insert(entry_node, seg);
        count += 1;

        // —— 进度与采样 —— //
        if count % 50_000 == 0 {
            sample_keys.push(key);         // 路由自检采样
        }
        if count % 400_000 == 0 {
            println!("Ingested {} rows...", count);  // ✅ 进度打印
        }

        // min/max 统计
        if lat < lat_min { lat_min = lat; }
        if lat > lat_max { lat_max = lat; }
        if lon < lon_min { lon_min = lon; }
        if lon > lon_max { lon_max = lon; }
        if ts  < ts_min  { ts_min  = ts;  }
        if ts  > ts_max  { ts_max  = ts;  }
    }

    // 保底：保证 sample_keys 非空
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::placement::{Placement, NodeDistRow};
//     use crate::node::Segment;

//     /// 一个最小可测网络：节点等距、按 node_id 后继命中；内部用 Vec<Vec<Segment>>（无 RefCell）
//     struct MiniNet {
//         m: usize,
//         nodes: Vec<Vec<Segment>>,
//         node_ids: Vec<u64>,
//         query_calls: usize,
//     }

//     impl MiniNet {
//         fn new(n: usize, m: usize) -> Self {
//             let ring: u128 = if m >= 64 { (u128::MAX) } else { (1u128 << m) - 1 };
//             let step = (ring + 1) / (n as u128);
//             let mut node_ids = Vec::with_capacity(n);
//             for i in 0..n {
//                 node_ids.push(((step * (i as u128)) & ring) as u64);
//             }
//             Self {
//                 m,
//                 nodes: (0..n).map(|_| Vec::new()).collect(),
//                 node_ids,
//                 query_calls: 0,
//             }
//         }
//         fn succ_index(&self, key: u64) -> usize {
//             for (i, &id) in self.node_ids.iter().enumerate() {
//                 if id >= key { return i; }
//             }
//             0
//         }
//     }

//     impl Placement for MiniNet {
//         fn node_id(&self, idx: usize) -> u64 { self.node_ids[idx] }

//         fn node_responsible_interval(&self, idx: usize) -> (u64, u64, bool) {
//             let n = self.node_ids.len();
//             let prev = if idx == 0 { n - 1 } else { idx - 1 };
//             let start = self.node_ids[prev].wrapping_add(1);
//             let end = self.node_ids[idx];
//             (start, end, start > end)
//         }

//         fn insert(&mut self, _entry_node: usize, seg: Segment) -> usize {
//             let i = self.succ_index(seg.sfc_key);
//             self.nodes[i].push(seg);
//             1
//         }

//         fn query_range(&self, _entry_node: usize, key_range: (u64, u64)) -> (Vec<&Segment>, usize) {
//             let (s, e) = key_range;
//             let mut v = Vec::new();
//             for bucket in &self.nodes {
//                 for seg in bucket.iter() {
//                     if seg.sfc_key >= s && seg.sfc_key <= e { v.push(seg); }
//                 }
//             }
//             (v, 1)
//         }

//         fn query_range_with_nodes(
//             &self,
//             entry_node: usize,
//             key_range: (u64, u64),
//         ) -> (Vec<(usize, &Segment)>, usize, Vec<usize>) {
//             let (rows, hops) = self.query_range(entry_node, key_range);
//             let mut pairs = Vec::new();
//             let mut visited = std::collections::BTreeSet::new();
//             for seg in rows {
//                 let i = self.succ_index(seg.sfc_key);
//                 pairs.push((i, seg));
//                 visited.insert(i);
//             }
//             (pairs, hops, visited.into_iter().collect())
//         }

//         fn node_distribution_rows(&self) -> Vec<NodeDistRow> {
//             let mut rows = Vec::new();
//             for (i, bucket) in self.nodes.iter().enumerate() {
//                 let id = self.node_ids[i];
//                 let total = bucket.len();
//                 let (mut mn, mut mx): (Option<u64>, Option<u64>) = (None, None);
//                 for seg in bucket.iter() {
//                     mn = Some(mn.map(|x: u64| x.min(seg.sfc_key)).unwrap_or(seg.sfc_key));
//                     mx = Some(mx.map(|x: u64| x.max(seg.sfc_key)).unwrap_or(seg.sfc_key));
//                 }
//                 rows.push((i, id, total, mn, mx));
//             }
//             rows
//         }

//         fn print_node_distribution(&self) {
//             for (i, id, total, mn, mx) in self.node_distribution_rows() {
//                 println!("Mini PNode {i} (id={id}) -> {total} [{mn:?}, {mx:?}]");
//             }
//         }

//         // ====== 这三条是你新加到 trait 的方法，必须在 impl 里实现 ======

//         fn export_node_ranges(&self) -> Vec<crate::placement::NodeRangeRow> {
//             let n = self.node_ids.len();
//             let mut out = Vec::with_capacity(n);
//             for i in 0..n {
//                 let prev = if i == 0 { n - 1 } else { i - 1 };
//                 let start = self.node_ids[prev].wrapping_add(1);
//                 let end = self.node_ids[i];
//                 let wrapped = start > end;

//                 let total = self.nodes[i].len();
//                 let mut mn: Option<u64> = None;
//                 let mut mx: Option<u64> = None;
//                 for seg in &self.nodes[i] {
//                     mn = Some(mn.map(|x: u64| x.min(seg.sfc_key)).unwrap_or(seg.sfc_key));
//                     mx = Some(mx.map(|x: u64| x.max(seg.sfc_key)).unwrap_or(seg.sfc_key));
//                 }
//                 out.push((i, self.node_ids[i], start, end, wrapped, total, mn, mx));
//             }
//             out
//         }

//         fn export_node_data<'a>(&'a self, idx: usize) -> Vec<&'a crate::node::Segment> {
//             self.nodes[idx].iter().collect()
//         }

//         fn export_pnode_vnode_details(&self) -> Vec<crate::placement::PNodeVNodeDetailRow> {
//             let n = self.node_ids.len();
//             let mut out = Vec::with_capacity(n);
//             for i in 0..n {
//                 let prev = if i == 0 { n - 1 } else { i - 1 };
//                 let start = self.node_ids[prev].wrapping_add(1);
//                 let end = self.node_ids[i];
//                 let wrapped = start > end;

//                 let total = self.nodes[i].len();
//                 let mut mn: Option<u64> = None;
//                 let mut mx: Option<u64> = None;
//                 for seg in &self.nodes[i] {
//                     mn = Some(mn.map(|x: u64| x.min(seg.sfc_key)).unwrap_or(seg.sfc_key));
//                     mx = Some(mx.map(|x: u64| x.max(seg.sfc_key)).unwrap_or(seg.sfc_key));
//                 }

//                 out.push((
//                     i, self.node_ids[i], // pnode 代表
//                     i, self.node_ids[i], // vnode 等价
//                     start, end, wrapped,
//                     total, mn, mx
//                 ));
//             }
//             out
//         }
//     }

//     fn seg_for_key(k: u64) -> Segment {
//         Segment {
//             user: "u".into(),
//             traj_id: 0,
//             segment_id: 0,
//             sfc_key: k,
//             payload: String::new(),
//             lat: 0.0, lon: 0.0, ts: 0,
//         }
//     }

//     #[test]
//     fn sanity_probe_calls_query_and_point_hits() {
//         let mut net = MiniNet::new(4, 16);
//         let keys: Vec<u64> = (0..4).map(|i| net.node_id(i)).collect();
//         for &k in &keys { net.insert(0, seg_for_key(k)); }

//         super::sanity_probe(&net, &keys);

//         for &k in &keys {
//             let (rows, _hops) = net.query_range(0, (k, k));
//             assert_eq!(rows.len(), 1, "point({k}) should hit exactly 1");
//         }
//     }

//     #[test]
//     fn node_distribution_rows_shape_ok() {
//         let mut net = MiniNet::new(3, 12);
//         for i in 0..3 {
//             let id = net.node_id(i);
//             net.insert(0, seg_for_key(id));
//             net.insert(0, seg_for_key(id.saturating_add(1)));
//         }
//         let rows = net.node_distribution_rows();
//         assert_eq!(rows.len(), 3);
//         let sum: usize = rows.iter().map(|r| r.2).sum();
//         assert_eq!(sum, 6);
//     }
// }

