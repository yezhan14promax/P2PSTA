// src/query.rs
// Responsibility: Perform distributed query on given ranges, statistics (deduplication/avg_hops/node cover), and write results to disk

use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;

use crate::config::{Config, QueryWindow};
use crate::placement::Placement;
use crate::planner::PlanResult;

/// Calculate percentile (p ∈ (0,100]). Returns 0.0 if empty.
fn percentile(mut xs: Vec<usize>, p: f64) -> f64 {
    if xs.is_empty() { return 0.0; }
    xs.sort_unstable();
    let n = xs.len();
    let rank = ((p / 100.0) * (n as f64 - 1.0)).round() as usize;
    xs[rank.min(n - 1)] as f64
}

/// Query executor
pub struct QueryExecutor<'a, P: Placement> {
    pub placement: &'a P,
    pub out_dir: PathBuf,
    pub print_first: usize,
    pub cfg: &'a Config,
}

impl<'a, P: Placement> QueryExecutor<'a, P> {
    pub fn new(placement: &'a P, out_dir: PathBuf, cfg: &'a Config) -> Self {
        let print_first = cfg.experiment.print_first.unwrap_or(15);
        Self { placement, out_dir, print_first, cfg }
    }

    /// Execute a query window: write window.txt / ranges_and_hits.csv / query_results*.csv / ranges_node_cover.csv / summary.txt
    pub fn run_one_window(
        &self,
        qi: usize,
        name: &str,
        q: &QueryWindow,
        plan: &PlanResult,
    ) -> std::io::Result<()> {
        let qdir = self.out_dir.join(format!("query_{:02}_{}", qi, name));
        create_dir_all(&qdir)?;

        // ========== window.txt ========== 
        {
            let mut wf = File::create(qdir.join("window.txt"))?;
            writeln!(wf, "# Query Window")?;
            writeln!(wf, "name       : {}", name)?;
            writeln!(wf, "lat_min    : {}", q.lat_min)?;
            writeln!(wf, "lon_min    : {}", q.lon_min)?;
            writeln!(wf, "lat_max    : {}", q.lat_max)?;
            writeln!(wf, "lon_max    : {}", q.lon_max)?;
            writeln!(wf, "t_start    : {}", q.t_start)?;
            writeln!(wf, "t_end      : {}", q.t_end)?;
            writeln!(wf, "")?;
            writeln!(wf, "# SFC Controls")?;
            writeln!(wf, "algorithm      : {}", self.cfg.experiment.algorithm)?;
            writeln!(wf, "stop_tail_bits : {}", self.cfg.experiment.stop_tail_bits)?;
            writeln!(wf, "merge_gap_keys : {}", self.cfg.experiment.merge_gap_keys)?;
            writeln!(wf, "max_ranges     : {:?}", self.cfg.experiment.max_ranges)?;
            writeln!(wf, "ring_m         : {}", plan.sfc_params.ring_m)?;
        }

        println!(
            "[QueryWindow #{:02} {}] raw={} -> merged={}",
            qi, name, plan.ranges_raw.len(), plan.ranges_merged.len()
        );

        // ========== CSV: Statistics for each range ==========
        let mut fh = File::create(qdir.join("ranges_and_hits.csv"))?;
        writeln!(fh, "range_idx,start,end,hits,hops")?;

        // ========== CSV: Detail without node information ==========
        let mut rf = File::create(qdir.join("query_results.csv"))?;
        // Only retain the five columns requested by the user (saved as the original row in Segment.payload during ingest)
        writeln!(rf, "user,traj_id,lat,lon,datetime")?;

        // ========== CSV: Detail with node information (optional) ==========
        let save_with_nodes = self.cfg.experiment.metrics.save_with_nodes.unwrap_or(true);
        let mut rfn = if save_with_nodes {
            let mut f = File::create(qdir.join("query_results_with_nodes.csv"))?;
            writeln!(f, "range_idx,node_idx,traj_id,segment_id,hilbert_key,payload")?;
            Some(f)
        } else { None };

        // ========== CSV: Node cover for each range (optional) ==========
        let compute_node_cover = self.cfg.experiment.metrics.compute_node_cover.unwrap_or(true);
        let mut fcover = if compute_node_cover {
            let mut f = File::create(qdir.join("ranges_node_cover.csv"))?;
            writeln!(f, "range_idx,node_count")?;
            Some(f)
        } else { None };

        // ========== Statistics ==========
        let mut total_hits_with_overlap = 0usize;
        let mut total_hops = 0usize;
        let mut uniq: HashSet<usize> = HashSet::new();
        let mut cover_counts: Vec<usize> = Vec::with_capacity(plan.ranges_merged.len());

        // ========== Execute query ==========
        for (idx, (s, e)) in plan.ranges_merged.iter().cloned().enumerate() {
            // Use the interface with node information
            let (hits_nodes, hops, touched_nodes) =
                self.placement.query_range_with_nodes(0, (s, e));
            total_hops += hops;

            // Count hits (including overlaps)
            total_hits_with_overlap += hits_nodes.len();

            // Write statistics and details
            let mut just_hits: Vec<&crate::node::Segment> = Vec::with_capacity(hits_nodes.len());
            for (node_idx, seg) in &hits_nodes {
                // Cross-range deduplication (by object pointer)
                uniq.insert(*seg as *const _ as usize);
                just_hits.push(*seg);

                // Detail with node information (preserve original format)
                if let Some(ref mut fwn) = rfn {
                    writeln!(
                        fwn,
                        "{},{},{},{},{},{}",
                        idx, node_idx, seg.traj_id, seg.segment_id, seg.hilbert_key, seg.payload
                    )?;
                }
            }

            // Print and log range statistics
            if idx < self.print_first {
                println!(
                    "  Range[{:>3}] [{}, {}] -> {} segments ({} hops)",
                    idx, s, e, just_hits.len(), hops
                );
            }
            writeln!(fh, "{},{},{},{},{}", idx, s, e, just_hits.len(), hops)?;

            // Detail without node: write original CSV line (namely user,traj_id,lat,lon,datetime)
            for seg in just_hits {
                writeln!(rf, "{}", seg.payload)?;
            }

            // Node cover
            if let Some(ref mut fc) = fcover {
                writeln!(fc, "{},{}", idx, touched_nodes.len())?;
            }
            cover_counts.push(touched_nodes.len());
        }

        if plan.ranges_merged.len() > self.print_first {
            println!(
                "  ... (suppressed {} more range logs)",
                plan.ranges_merged.len() - self.print_first
            );
        }

        // ========== summary.txt (right here!) ==========
        {
            let mut sf = File::create(qdir.join("summary.txt"))?;
            writeln!(sf, "raw_ranges        : {}", plan.ranges_raw.len())?;
            writeln!(sf, "merged_ranges     : {}", plan.ranges_merged.len())?;
            writeln!(sf, "hits_with_overlap : {}", total_hits_with_overlap)?;
            writeln!(sf, "unique_hits       : {}", uniq.len())?;

            // hops
            writeln!(sf, "total_hops        : {}", total_hops)?;
            let avg_hops = if plan.ranges_merged.is_empty() {
                0.0
            } else {
                total_hops as f64 / plan.ranges_merged.len() as f64
            };
            writeln!(sf, "avg_hops          : {:.2}", avg_hops)?;

            // Node cover statistics
            if let Some(ref _fc) = fcover {
                let mean = if cover_counts.is_empty() {
                    0.0
                } else {
                    cover_counts.iter().copied().map(|x| x as f64).sum::<f64>() / cover_counts.len() as f64
                };
                let p95 = percentile(cover_counts.clone(), 95.0);
                let p99 = percentile(cover_counts.clone(), 99.0);
                let max = cover_counts.iter().copied().max().unwrap_or(0);
                writeln!(sf, "node_cover_mean   : {:.2}", mean)?;
                writeln!(sf, "node_cover_p95    : {:.2}", p95)?;
                writeln!(sf, "node_cover_p99    : {:.2}", p99)?;
                writeln!(sf, "node_cover_max    : {}", max)?;
            }
        }

        Ok(())
    }
}
