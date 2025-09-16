//! query.rs
//! 分布式查询执行：消费 planner 产出的区间（已做“约束注入法”），分发到网络层，做精确过滤与去重，落盘统计。
//!
//! 输出（每个 query 一个目录）：
//! - ranges_and_hits_with_nodes.csv（逐条命中诊断，含 route_hops / node_visits）
//! - ranges_and_hits.csv（✅ 新增：每区间聚合汇总：range_idx,start,end,hits,route_hops,node_visits）
//! - ranges_node_cover.csv（兼容旧流程；其“覆盖数”已包含在上面的聚合文件中）
//! - query_results.csv（真实业务结果：user,traj_id,lat,lon,datetime）
//! - summary.txt（含 avg_route_hops / avg_node_cover 等）
//! - window.txt（窗口与区间数量快照）
//!
//! 控制台打印：
//! - [QueryWindow #i name] raw=X -> merged=Y
//! - Range[k] [s, e] -> N segments (H hops, V nodes)（最多打印前 N 条）

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use csv::WriterBuilder;
use chrono::{DateTime, Utc};

use crate::config::{Config, QueryWindow};
use crate::network::Network;
use crate::node::Segment;

/// 最多打印多少条区间的明细（避免刷屏）
const PRINT_RANGE_LIMIT: usize = 15;

/// 用于 ranges_and_hits.csv 的每区间聚合统计
struct RangeStat {
    start: u64,
    end: u64,
    route_hops: usize,   // 该区间路由步数（来自 network 的 hops）
    node_visits: usize,  // 该区间触达节点数
    hits: usize,         // 该区间最终写出的命中条数（经精确过滤+去重）
}

pub struct QueryExecutor<'a> {
    pub cfg: &'a Config,
    pub net: &'a Network,
    pub run_dir: PathBuf,
}

impl<'a> QueryExecutor<'a> {
    /// 与 experiment.rs 的调用次序保持一致：(net, run_dir, cfg)
    pub fn new(net: &'a Network, run_dir: impl AsRef<Path>, cfg: &'a Config) -> Self {
        Self {
            cfg,
            net,
            run_dir: run_dir.as_ref().to_path_buf(),
        }
    }

    /// 执行单个查询窗口
    ///
    /// - `ranges_merged`：planner 合并后的区间（已做“约束注入法”）
    /// - `raw_ranges_len`：合并前的区间数（仅用于日志/window.txt）
    /// - `t_start_s / t_end_s`：秒级时间边界（planner 已统一解析）
    pub fn run_one_window(
        &self,
        qi: usize,
        name: &str,
        q: &QueryWindow,
        ranges_merged: &[(u64, u64)],
        raw_ranges_len: usize,
        t_start_s: u64,
        t_end_s: u64,
    ) -> std::io::Result<(usize, f64, f64)> {
        // 读取开关（注意路径在 experiment.metrics 下）
        let save_with_nodes = self
            .cfg
            .experiment
            .metrics
            .save_with_nodes
            .unwrap_or(true); // 我们无论如何都写 with_nodes 明细
        let precise_hits = self
            .cfg
            .experiment
            .metrics
            .precise_hits
            .unwrap_or(true);

        // 目录与文件
        let window_dir = self.run_dir.join(format!("query_{:02}_{}", qi, name));
        fs::create_dir_all(&window_dir)?;

        // 逐条命中（诊断明细，统一写 with_nodes 版本）
        let hits_csv_path = window_dir.join("ranges_and_hits_with_nodes.csv");
        // 每区间聚合
        let ranges_summary_path = window_dir.join("ranges_and_hits.csv");
        // 兼容旧流程：区间→节点覆盖
        let cover_csv_path = window_dir.join("ranges_node_cover.csv");
        // 真实业务结果
        let results_csv_path = window_dir.join("query_results.csv");
        // 统计与窗口快照
        let summary_path = window_dir.join("summary.txt");
        let window_txt = window_dir.join("window.txt");

        // 写 window.txt（便于审计）
        {
            let mut f = BufWriter::new(File::create(&window_txt)?);
            writeln!(&mut f, "[QueryWindow #{} {}]", qi, name)?;
            writeln!(
                &mut f,
                "lat:[{},{}], lon:[{},{}], time:[{},{}]",
                q.lat_min, q.lat_max, q.lon_min, q.lon_max, q.t_start, q.t_end
            )?;
            writeln!(
                &mut f,
                "ranges: {}, merged: {}",
                raw_ranges_len,
                ranges_merged.len()
            )?;
            writeln!(&mut f, "t_start_s={}, t_end_s={}", t_start_s, t_end_s)?;
        }

        // === 控制台打印：窗口头 ===
        println!(
            "[QueryWindow #{:02} {}] raw={} -> merged={}",
            qi,
            name,
            raw_ranges_len,
            ranges_merged.len()
        );

        // 逐条命中（with_nodes）CSV 表头
        let mut wtr_hits = WriterBuilder::new().from_path(&hits_csv_path)?;
        if save_with_nodes {
            wtr_hits.write_record(&[
                "range_idx",
                "node_idx",
                "node_id",
                "sfc_key",
                "traj_id",
                "segment_id",
                "ts",
                "lat",
                "lon",
                "route_hops",
                "node_visits",
            ])?;
        }

        // ✅ 真实业务结果 CSV
        let mut wtr_results = WriterBuilder::new().from_path(&results_csv_path)?;
        wtr_results.write_record(&["user", "traj_id", "lat", "lon", "datetime"])?;

        // 每区间聚合统计容器
        let mut range_stats: Vec<RangeStat> = Vec::with_capacity(ranges_merged.len());

        // 业务键去重：避免同一段被多个区间重复写出
        // 注意：第三个键为 sfc_key
        let mut uniq: HashSet<(u64, u32, u64)> = HashSet::new();

        // 区间打印控制
        let mut printed = 0usize;
        let suppress_after = ranges_merged.len().saturating_sub(PRINT_RANGE_LIMIT);

        // 按区间执行
        for (ri, &(s, e)) in ranges_merged.iter().enumerate() {
            let entry_node = ri % self.net.nodes.len();
            let (hits_nodes, route_hops, touched_nodes) =
                self.net.query_range_with_nodes(entry_node, (s, e));

            let node_visits = touched_nodes.len();

            // 统计本区间最终写出的行数（通过过滤 + 去重之后）
            let mut this_range_written = 0usize;

            for (node_idx, seg) in hits_nodes {
                // 精确过滤（如关闭 precise_hits，则跳过）
                if precise_hits && !inside_window(seg, q, t_start_s, t_end_s) {
                    continue;
                }
                // 业务键去重（traj_id, segment_id, sfc_key）
                let key = (seg.traj_id, seg.segment_id, seg.sfc_key);
                if !uniq.insert(key) {
                    continue;
                }

                // 写逐条命中（with_nodes）
                if save_with_nodes {
                    wtr_hits.write_record(&[
                        ri.to_string(),
                        node_idx.to_string(),
                        self.net.node_ids[node_idx].to_string(),
                        seg.sfc_key.to_string(),
                        seg.traj_id.to_string(),
                        seg.segment_id.to_string(),
                        seg.ts.to_string(),
                        format!("{}", seg.lat),
                        format!("{}", seg.lon),
                        route_hops.to_string(),
                        node_visits.to_string(),
                    ])?;
                }

                // ✅ 写真实业务结果
                let user_str = seg.user.clone();
                wtr_results.write_record(&[
                    user_str,
                    seg.traj_id.to_string(),
                    format!("{}", seg.lat),
                    format!("{}", seg.lon),
                    ts_to_rfc3339(seg.ts),
                ])?;

                this_range_written += 1;
            }

            // 记录该区间的聚合统计
            range_stats.push(RangeStat {
                start: s,
                end: e,
                route_hops,
                node_visits,
                hits: this_range_written,
            });

            // 控制台打印（最多打印前 PRINT_RANGE_LIMIT 条）
            if printed < PRINT_RANGE_LIMIT {
                println!(
                    "  Range[{:<3}] [{}, {}] -> {} segments ({} hops, {} nodes)",
                    ri, s, e, this_range_written, route_hops, node_visits
                );
                printed += 1;

                if printed == PRINT_RANGE_LIMIT && suppress_after > 0 {
                    println!("  ... (suppressed {} more range logs)", suppress_after);
                }
            }
        }

        // flush 逐条命中 与 业务结果
        wtr_hits.flush()?;
        wtr_results.flush()?;

        // 写新增的按区间聚合文件：ranges_and_hits.csv
        {
            let mut w = WriterBuilder::new().from_path(&ranges_summary_path)?;
            w.write_record(&["range_idx", "start", "end", "hits", "route_hops", "node_visits"])?;
            for (ri, rs) in range_stats.iter().enumerate() {
                w.write_record(&[
                    ri.to_string(),
                    rs.start.to_string(),
                    rs.end.to_string(),
                    rs.hits.to_string(),
                    rs.route_hops.to_string(),
                    rs.node_visits.to_string(),
                ])?;
            }
            w.flush()?;
        }

        // （可保留以兼容）ranges_node_cover.csv：从 range_stats 回写
        {
            let mut wtr_cover = WriterBuilder::new().from_path(&cover_csv_path)?;
            wtr_cover.write_record(&["range_idx", "node_cover"])?;
            for (ri, rs) in range_stats.iter().enumerate() {
                wtr_cover.write_record(&[ri.to_string(), rs.node_visits.to_string()])?;
            }
            wtr_cover.flush()?;
        }

        // 汇总
        let total_hits = uniq.len();
        let avg_route_hops = if range_stats.is_empty() {
            0.0
        } else {
            (range_stats.iter().map(|r| r.route_hops).sum::<usize>() as f64)
                / (range_stats.len() as f64)
        };
        let avg_node_cover = if range_stats.is_empty() {
            0.0
        } else {
            (range_stats.iter().map(|r| r.node_visits).sum::<usize>() as f64)
                / (range_stats.len() as f64)
        };

        // 写 summary.txt
        {
            let mut f = BufWriter::new(File::create(&summary_path)?);
            writeln!(&mut f, "Query: #{} {}", qi, name)?;
            writeln!(&mut f, "Total precise hits: {}", total_hits)?;
            writeln!(&mut f, "Avg route hops: {:.3}", avg_route_hops)?;
            writeln!(&mut f, "Avg node cover: {:.3}", avg_node_cover)?;
            writeln!(
                &mut f,
                "Ranges (merged): {} (raw: {})",
                ranges_merged.len(),
                raw_ranges_len
            )?;
        }

        Ok((total_hits, avg_route_hops, avg_node_cover))
    }
}

/// 精确过滤：在 SFC 候选上二次判定是否真的落在空间 + 时间窗口内。
#[inline]
fn inside_window(seg: &Segment, q: &QueryWindow, t_start: u64, t_end: u64) -> bool {
    // 空间（闭区间）
    if !(seg.lat >= q.lat_min && seg.lat <= q.lat_max) {
        return false;
    }
    if !(seg.lon >= q.lon_min && seg.lon <= q.lon_max) {
        return false;
    }
    // 时间（闭区间）
    if !(seg.ts >= t_start && seg.ts <= t_end) {
        return false;
    }
    true
}

/// 秒 → RFC3339（UTC）
/// 使用新 API：DateTime::<Utc>::from_timestamp
#[inline]
fn ts_to_rfc3339(ts: u64) -> String {
    match DateTime::<Utc>::from_timestamp(ts as i64, 0) {
        Some(dt) => dt.to_rfc3339(),
        None => ts.to_string(),
    }
}
