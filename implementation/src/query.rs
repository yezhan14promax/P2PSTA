use std::collections::{HashMap, HashSet, BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use csv::WriterBuilder;
use chrono::{DateTime, Utc};

use crate::config::{Config, QueryWindow};
use crate::node::Segment;
use crate::placement::Placement;

/// 最多打印多少条区间的明细（避免刷屏）
const PRINT_RANGE_LIMIT: usize = 15;

/// 合并后一次取 —— 目录法：一个 vnode 上需要的若干非 wrap 子区间
#[derive(Debug, Clone)]
struct VnodeSlice {
    pnode_idx: usize,
    vnode_idx: usize,
    vnode_id:  u64,
    ranges:    Vec<(u64,u64)>, // 非 wrap 闭区间并集
}

/// 合并后一次取 —— 目录法：按 pnode 的取数计划
#[derive(Debug, Clone)]
struct FetchPlan {
    per_pnode: BTreeMap<usize, Vec<VnodeSlice>>, // pnode_idx -> [vnode需求...]
    visited_vnodes: Vec<usize>,
    visited_pnodes: Vec<usize>,
}

/// 用于控制台打印
struct RangeStatPrint {
    start: u64,
    end: u64,
    route_hops: usize,
    node_visits: usize,
    hits: usize,
}

pub struct QueryExecutor<'a> {
    pub cfg: &'a Config,
    pub net: &'a dyn Placement, // 多态
    pub run_dir: PathBuf,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(net: &'a dyn Placement, run_dir: impl AsRef<Path>, cfg: &'a Config) -> Self {
        Self { cfg, net, run_dir: run_dir.as_ref().to_path_buf() }
    }

    /// 执行单个查询窗口（合并后一次取）
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
        // 目录与文件
        let window_dir = self.run_dir.join(format!("query_{:02}_{}", qi, name));
        fs::create_dir_all(&window_dir)?;

        // 固定：window.txt（便于审计）
        let window_txt = window_dir.join("window.txt");
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
            qi, name, raw_ranges_len, ranges_merged.len()
        );

        // ✅ 真实业务结果 CSV（保持不变）
        let results_csv_path = window_dir.join("query_results.csv");
        let mut wtr_results = WriterBuilder::new().from_path(&results_csv_path)?;
        wtr_results.write_record(&["user", "traj_id", "lat", "lon", "datetime"])?;

        // =============================
        // 1) 计划阶段：目录法构建 FetchPlan
        // =============================
        let plan = build_fetch_plan(self.net, ranges_merged);

        // =============================
        // 2) 跳数统计（按 pnode）：每个 pnode 只估一次 route hops
        // =============================
        let entry_node = 0usize;
        let mut hops_per_pnode: HashMap<usize, usize> = HashMap::new();

        // 打印少量计划信息（可选）
        let mut printed = 0usize;
        for (pi, vn_slices) in plan.per_pnode.iter() {
            if printed >= PRINT_RANGE_LIMIT { break; }
            let total_subranges: usize = vn_slices.iter().map(|v| v.ranges.len()).sum();
            println!("  PNode {:>6}: vnode_slices={}, subranges={}", pi, vn_slices.len(), total_subranges);
            printed += 1;
        }
        if plan.per_pnode.len() > PRINT_RANGE_LIMIT {
            println!("  ... ({} more pnodes suppressed)", plan.per_pnode.len() - PRINT_RANGE_LIMIT);
        }

        // 对每个将访问的 pnode，取代表键并路由一次
        for (pi, vn_slices) in &plan.per_pnode {
            // 代表键：该 pnode 所有 vnode 的所有 ranges 的最小 a
            let rep_key = vn_slices
                .iter()
                .flat_map(|v| v.ranges.iter().map(|(a, _b)| *a))
                .min()
                .unwrap_or(0u64);

            // 路由到 rep_key 的后继，拿到 hops（不取数据，只统计开销）
            let (_pairs, route_hops, _touched) = self.net.query_range_with_nodes(entry_node, (rep_key, rep_key));
            hops_per_pnode.insert(*pi, route_hops);
        }

        let total_route_hops: usize = hops_per_pnode.values().copied().sum();


        // =============================
        // 3) 执行阶段：目录法合并后一次取
        // =============================
        let mut uniq: HashSet<(u64, u32, u64)> = HashSet::new(); // (traj_id, segment_id, sfc_key)
        let mut per_pnode_returned: HashMap<usize, usize> = HashMap::new();

        // 真实取数：每个 pnode 只取一次，按各 vnode 合并区间过滤
        for (pi, vn_slices) in &plan.per_pnode {
            let pool = self.net.export_node_data(*pi); // Vec<&Segment>
            if pool.is_empty() { continue; }

            let mut returned = 0usize;
            'POOL: for seg in pool {
                // 先做业务精确过滤
                if !inside_window(seg, q, t_start_s, t_end_s) { continue; }
                // 再做业务键去重
                if !uniq.insert((seg.traj_id, seg.segment_id, seg.sfc_key)) { continue; }
                // 落在该 pnode 所需的任意 vnode 合并区间内则写出
                let key = seg.sfc_key;
                let mut hit = false;
                'VN: for vn in vn_slices {
                    for &(a,b) in &vn.ranges {
                        if key >= a && key <= b { hit = true; break 'VN; }
                    }
                }
                if !hit { continue; }

                // ✅ 写真实业务结果
                let user_str = seg.user.clone();
                wtr_results.write_record(&[
                    user_str,
                    seg.traj_id.to_string(),
                    format!("{}", seg.lat),
                    format!("{}", seg.lon),
                    ts_to_rfc3339(seg.ts),
                ])?;
                returned += 1;
            }
            per_pnode_returned.insert(*pi, returned);
        }

        wtr_results.flush()?;

        // =============================
        // 4) 写 pnode_report.csv（按 pnode 汇总）
        // =============================
        let mut w_pn = WriterBuilder::new().from_path(window_dir.join("pnode_report.csv"))?;
        w_pn.write_record(&["pnode_idx","returned_count","route_hops"])?;

        // 这里的 route_hops 口径：把“合并区间的总 hops”记到每个被请求的 pnode 名下，
        // 便于横向比较 baseline/vnode/smartvnode 的“触达节点数越少，总 hops 越少”的趋势。
        // 如需更细粒度分摊，后续可改为只计触达该 pnode 的那些合并区间的 hops 之和。
        for (pi, returned) in per_pnode_returned.iter() {
            let hops_for_this_pnode = hops_per_pnode.get(pi).copied().unwrap_or(0);
            w_pn.write_record(&[
                pi.to_string(),
                returned.to_string(),
                hops_for_this_pnode.to_string(),
            ])?;
        }
        w_pn.flush()?;


        // =============================
        // 5) summary.txt
        // =============================
        let total_hits = uniq.len();
        let hit_pnodes_cnt = plan.per_pnode.len();
        let avg_route_hops = if hit_pnodes_cnt == 0 {
            0.0
        } else {
            total_route_hops as f64 / (hit_pnodes_cnt as f64)
        };


        let summary_path = window_dir.join("summary.txt");
        {
            let mut f = BufWriter::new(File::create(&summary_path)?);
            writeln!(&mut f, "Query: #{} {}", qi, name)?;
            writeln!(&mut f, "Total precise hits: {}", total_hits)?;
            writeln!(&mut f, "Hit pnodes: {}", hit_pnodes_cnt)?;
            writeln!(&mut f, "Total route hops: {}", total_route_hops)?;
            writeln!(&mut f, "Avg route hops: {:.3}", avg_route_hops)?;
        }

        Ok((total_hits, avg_route_hops, hit_pnodes_cnt as f64))
    }
}

/// ===== 工具函数：目录法计划 & 合并 =====

#[inline]
fn split_to_nonwrap((s,e):(u64,u64)) -> Vec<(u64,u64)> {
    if s <= e { vec![(s,e)] } else { vec![(s, u64::MAX), (0, e)] }
}

/// 合并一组非 wrap 闭区间
fn merge_nonwrap(mut arcs: Vec<(u64,u64)>) -> Vec<(u64,u64)> {
    if arcs.is_empty() { return arcs; }
    arcs.sort_by_key(|&(a, _)| a);
    let mut out: Vec<(u64,u64)> = Vec::with_capacity(arcs.len());
    for (a,b) in arcs {
        if let Some(last) = out.last_mut() {
            if a <= last.1.saturating_add(1) {
                last.1 = last.1.max(b);
            } else {
                out.push((a,b));
            }
        } else { out.push((a,b)); }
    }
    out
}

/// 把小窗 → vnode 子区间并集，再按 pnode 聚合（不触发路由）
fn build_fetch_plan(net: &dyn Placement, windows: &[(u64,u64)]) -> FetchPlan {
    let details = net.export_pnode_vnode_details(); // (pi,pid,vi,vid,rs,re,wrapped,tot,mn,mx)

    // 收集：vnode_idx -> Vec<非wrap子区间>
    let mut need: BTreeMap<usize, Vec<(u64,u64)>> = BTreeMap::new();
    let mut visited_v: BTreeSet<usize> = BTreeSet::new();

    for (_pi, _pid, vi, _vid, rs, re, _wrapped, _t, _mn, _mx) in details.iter().copied() {
        let mut list: Vec<(u64,u64)> = Vec::new();
        for &(ks, ke) in windows {
            for (qa, qb) in split_to_nonwrap((ks, ke)) {
                for (va, vb) in split_to_nonwrap((rs, re)) {
                    let a = qa.max(va);
                    let b = qb.min(vb);
                    if a <= b { list.push((a,b)); }
                }
            }
        }
        if !list.is_empty() {
            visited_v.insert(vi);
            need.insert(vi, merge_nonwrap(list));
        }
    }

    // 按 pnode 聚合，并填入 vnode_id
    let mut per_pnode: BTreeMap<usize, Vec<VnodeSlice>> = BTreeMap::new();
    for (pi, _pid, vi, vid, _rs, _re, _wrapped, _t, _mn, _mx) in details.into_iter() {
        if let Some(ranges) = need.remove(&vi) {
            per_pnode.entry(pi).or_default().push(VnodeSlice {
                pnode_idx: pi, vnode_idx: vi, vnode_id: vid, ranges
            });
        }
    }
    let visited_p: Vec<usize> = per_pnode.keys().cloned().collect();

    FetchPlan {
        per_pnode,
        visited_vnodes: visited_v.into_iter().collect(),
        visited_pnodes: visited_p,
    }
}

/// 精确过滤：在 SFC 候选上二次判定是否真的落在空间 + 时间窗口内。
#[inline]
fn inside_window(seg: &Segment, q: &QueryWindow, t_start: u64, t_end: u64) -> bool {
    // 空间（闭区间）
    if !(seg.lat >= q.lat_min && seg.lat <= q.lat_max) { return false; }
    if !(seg.lon >= q.lon_min && seg.lon <= q.lon_max) { return false; }
    // 时间（闭区间）
    if !(seg.ts >= t_start && seg.ts <= t_end) { return false; }
    true
}

/// 秒 → RFC3339（UTC）
#[inline]
fn ts_to_rfc3339(ts: u64) -> String {
    match DateTime::<Utc>::from_timestamp(ts as i64, 0) {
        Some(dt) => dt.to_rfc3339(),
        None => ts.to_string(),
    }
}
