// src/query.rs
// Responsibility: Perform distributed query on given ranges, stats (deduplication/avg_hops/node cover), and write results to disk

use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;

use crate::config::{Config, QueryWindow};
use crate::placement::Placement;
use crate::planner::PlanResult;
use chrono;

/// Calculate percentile (p ∈ (0,100]). Returns 0.0 if empty.
fn percentile(mut xs: Vec<usize>, p: f64) -> f64 {
    if xs.is_empty() { return 0.0; }
    xs.sort_unstable();
    let n = xs.len();
    let rank = (p / 100.0) * (n as f64 - 1.0);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi { xs[lo] as f64 } else { xs[lo] as f64 * (hi as f64 - rank) + xs[hi] as f64 * (rank - lo as f64) }
}

fn parse_time_str(s: &str) -> u64 {
    if let Ok(v) = s.trim().parse::<i64>() { return v.max(0) as u64; }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return dt.with_timezone(&chrono::Utc).timestamp().max(0) as u64;
    }
    use chrono::NaiveDateTime;
    const FMTS: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ];
    for fmt in FMTS {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc).timestamp() as u64;
        }
    }
    0
}

pub struct QueryExecutor<'a, P: Placement> {
    placement: &'a P,
    out_dir: PathBuf,
    print_first: usize,
    cfg: &'a Config,
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
            writeln!(f, "range_idx,node_idx,node_id,user,traj_id,lat,lon,datetime")?;
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
        let mut cover_counts: Vec<usize> = Vec::new();

        let mut total_invalid_visits = 0usize;

        for (idx, &(s, e)) in plan.ranges_merged.iter().enumerate() {
            // run distributed query
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
                        "{},{},{},{},{},{},{},{}",
                        idx,
                        node_idx,
                        self.placement.node_id(*node_idx),
                        seg.payload.split(',').next().unwrap_or(""),
                        seg.traj_id,
                        seg.lat,
                        seg.lon,
                        seg.payload.split(',').nth(4).unwrap_or( &seg.ts.to_string() )
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

            // dump results (5 columns)
            for seg in just_hits {
                writeln!(rf, "{}", seg.payload)?;
            }

            // Node cover
            if let Some(ref mut fc) = fcover {
                writeln!(fc, "{},{}", idx, touched_nodes.len())?;
            }

            // Invalid node visits: touched nodes that do not intersect [s,e]
            let mut invalid_node_visits = 0usize;
            for &ni in &touched_nodes {
                let (ns, ne, _wrapped) = self.placement.node_responsible_interval(ni);
                if e < ns || ne < s { invalid_node_visits += 1; }
            }
            total_invalid_visits += invalid_node_visits;
            // Append invalid visits to summary range stats file (optional)
            // (We keep ranges_and_hits.csv format stable; invalid visits go to summary.txt below)

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
                total_hops as f64 / (plan.ranges_merged.len() as f64)
            };
            writeln!(sf, "avg_hops          : {:.2}", avg_hops)?;

            // cover percentiles
            if !cover_counts.is_empty() {
                let mean = {
                    let s: usize = cover_counts.iter().sum();
                    s as f64 / cover_counts.len() as f64
                };
                let p95 = percentile(cover_counts.clone(), 95.0);
                let p99 = percentile(cover_counts.clone(), 99.0);
                let max = cover_counts.iter().copied().max().unwrap_or(0);
                writeln!(sf, "node_cover_mean   : {:.2}", mean)?;
                writeln!(sf, "node_cover_p95    : {:.2}", p95)?;
                writeln!(sf, "node_cover_p99    : {:.2}", p99)?;
                writeln!(sf, "node_cover_max    : {}", max)?;
                writeln!(sf, "invalid_node_visits_total : {}", total_invalid_visits)?;
            }
        }

        // Verification
        let _ = self.verify_against_csv(&qdir, q);
        Ok(())
    }

    /// Verify: count points in original CSV within window and compare with returned points
    fn verify_against_csv(&self, qdir: &std::path::Path, q: &crate::config::QueryWindow) -> std::io::Result<()> {
        use std::io::BufRead;
        let res_file = std::fs::File::open(qdir.join("query_results.csv"))?;
        let returned = std::io::BufReader::new(res_file).lines().skip(1).count();

        // Count truth from original CSV
        let mut truth = 0usize;
        let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_path(&self.cfg.data.csv_path)?;
        let t0 = parse_time_str(&q.t_start);
        let t1 = parse_time_str(&q.t_end);
        for rec in rdr.records() {
            let r = rec?;
            let lat: f64 = r.get(2).unwrap_or("").parse().unwrap_or(f64::NAN);
            let lon: f64 = r.get(3).unwrap_or("").parse().unwrap_or(f64::NAN);
            let ts:  u64 = parse_time_str(r.get(4).unwrap_or(""));
            if lat>=q.lat_min && lat<=q.lat_max && lon>=q.lon_min && lon<=q.lon_max && ts>=t0 && ts<=t1 {
                truth += 1;
            }
        }
        let mut vf = std::fs::File::create(qdir.join("verification.txt"))?;
        use std::io::Write;
        writeln!(vf, "truth_points_in_csv={}", truth)?;
        writeln!(vf, "returned_points={}", returned)?;
        writeln!(vf, "match={}", (truth == returned))?;
        Ok(())
    }
}
