use std::collections::{HashMap, HashSet, BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use csv::WriterBuilder;
use chrono::{DateTime, Utc};

use crate::config::{Config, QueryWindow};
use crate::node::Segment;
use crate::placement::Placement;

/// Maximum number of interval details to print, to avoid flooding the console
const PRINT_RANGE_LIMIT: usize = 15;

/// Single fetch after merging: directory-based planning for the non-wrapping subranges needed on one vnode
#[derive(Debug, Clone)]
struct VnodeSlice {
    pnode_idx: usize,
    vnode_idx: usize,
    vnode_id:  u64,
    ranges:    Vec<(u64,u64)>, // union of non-wrapping closed intervals
}

/// Single fetch after merging: directory-based fetch plan grouped by pnode
#[derive(Debug, Clone)]
struct FetchPlan {
    per_pnode: BTreeMap<usize, Vec<VnodeSlice>>, // pnode_idx -> required vnode slices
    visited_vnodes: Vec<usize>,
    visited_pnodes: Vec<usize>,
}

/// Used for console output
struct RangeStatPrint {
    start: u64,
    end: u64,
    route_hops: usize,
    node_visits: usize,
    hits: usize,
}

pub struct QueryExecutor<'a> {
    pub cfg: &'a Config,
    pub net: &'a dyn Placement, // polymorphic placement backend
    pub run_dir: PathBuf,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(net: &'a dyn Placement, run_dir: impl AsRef<Path>, cfg: &'a Config) -> Self {
        Self { cfg, net, run_dir: run_dir.as_ref().to_path_buf() }
    }

    /// Execute one query window with the merged single-fetch plan
    ///
    /// - `ranges_merged`: intervals merged by the planner after constraint injection
    /// - `raw_ranges_len`: number of intervals before merging, used only for logs/window.txt
    /// - `t_start_s / t_end_s`: second-level time bounds already normalized by the planner
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
        // Directories and files
        let window_dir = self.run_dir.join(format!("query_{:02}_{}", qi, name));
        fs::create_dir_all(&window_dir)?;

        // Always emit window.txt for auditing
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

        // === Console header for the window ===
        println!(
            "[QueryWindow #{:02} {}] raw={} -> merged={}",
            qi, name, raw_ranges_len, ranges_merged.len()
        );

        // Real business-result CSV (left unchanged)
        let results_csv_path = window_dir.join("query_results.csv");
        let mut wtr_results = WriterBuilder::new().from_path(&results_csv_path)?;
        wtr_results.write_record(&["user", "traj_id", "lat", "lon", "datetime"])?;

        // =============================
        // 1) Planning stage: build FetchPlan with the directory-based method
        // =============================
        let plan = build_fetch_plan(self.net, ranges_merged);

        // =============================
        // 2) Hop accounting by pnode: estimate route hops once per pnode
        // =============================
        let entry_node = 0usize;
        let mut hops_per_pnode: HashMap<usize, usize> = HashMap::new();

        // Print a small amount of planning information (optional)
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

        // For each pnode to be visited, pick a representative key and route once
        for (pi, vn_slices) in &plan.per_pnode {
            // Representative key: the smallest range start across all vnode slices under this pnode
            let rep_key = vn_slices
                .iter()
                .flat_map(|v| v.ranges.iter().map(|(a, _b)| *a))
                .min()
                .unwrap_or(0u64);

            // Route to the successor of rep_key to obtain hops without fetching data
            let (_pairs, route_hops, _touched) = self.net.query_range_with_nodes(entry_node, (rep_key, rep_key));
            hops_per_pnode.insert(*pi, route_hops);
        }

        let total_route_hops: usize = hops_per_pnode.values().copied().sum();


        // =============================
        // 3) Execution stage: one fetch after directory-based merging
        // =============================
        let mut uniq: HashSet<(u64, u32, u64)> = HashSet::new(); // (traj_id, segment_id, sfc_key)
        let mut per_pnode_returned: HashMap<usize, usize> = HashMap::new();

        // Real fetch: each pnode is read once, then filtered by the merged vnode ranges
        for (pi, vn_slices) in &plan.per_pnode {
            let pool = self.net.export_node_data(*pi); // Vec<&Segment>
            if pool.is_empty() { continue; }

            let mut returned = 0usize;
            'POOL: for seg in pool {
                // Apply exact business-level filtering first
                if !inside_window(seg, q, t_start_s, t_end_s) { continue; }
                // Then deduplicate by business key
                if !uniq.insert((seg.traj_id, seg.segment_id, seg.sfc_key)) { continue; }
                // Write the record if it falls into any merged vnode interval needed by this pnode
                let key = seg.sfc_key;
                let mut hit = false;
                'VN: for vn in vn_slices {
                    for &(a,b) in &vn.ranges {
                        if key >= a && key <= b { hit = true; break 'VN; }
                    }
                }
                if !hit { continue; }

                // Write the real business result
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
        // 4) Write pnode_report.csv aggregated by pnode
        // =============================
        let mut w_pn = WriterBuilder::new().from_path(window_dir.join("pnode_report.csv"))?;
        w_pn.write_record(&["pnode_idx","returned_count","route_hops"])?;

        // route_hops semantics here: assign the total hops of the merged intervals to each requested pnode,
        // making it easy to compare the shared trend across baseline/vnode/smartvnode: fewer touched nodes, fewer total hops.
        // For finer-grained attribution later, this can be changed to count only the hops of merged intervals that touch that pnode.
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

/// ===== Helper functions: directory-based planning and merging =====

#[inline]
fn split_to_nonwrap((s,e):(u64,u64)) -> Vec<(u64,u64)> {
    if s <= e { vec![(s,e)] } else { vec![(s, u64::MAX), (0, e)] }
}

/// Merge a set of non-wrapping closed intervals
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

/// Convert small windows into unions of vnode subranges, then aggregate by pnode without routing
fn build_fetch_plan(net: &dyn Placement, windows: &[(u64,u64)]) -> FetchPlan {
    let details = net.export_pnode_vnode_details(); // (pi,pid,vi,vid,rs,re,wrapped,tot,mn,mx)

    // Collect vnode_idx -> Vec<non-wrapping subranges>
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

    // Aggregate by pnode and fill in vnode_id
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

/// Exact filtering: perform a second check on SFC candidates to confirm they are inside the spatial and temporal window.
#[inline]
fn inside_window(seg: &Segment, q: &QueryWindow, t_start: u64, t_end: u64) -> bool {
    // Space (closed interval)
    if !(seg.lat >= q.lat_min && seg.lat <= q.lat_max) { return false; }
    if !(seg.lon >= q.lon_min && seg.lon <= q.lon_max) { return false; }
    // Time (closed interval)
    if !(seg.ts >= t_start && seg.ts <= t_end) { return false; }
    true
}

/// Seconds -> RFC3339 (UTC)
#[inline]
fn ts_to_rfc3339(ts: u64) -> String {
    match DateTime::<Utc>::from_timestamp(ts as i64, 0) {
        Some(dt) => dt.to_rfc3339(),
        None => ts.to_string(),
    }
}
