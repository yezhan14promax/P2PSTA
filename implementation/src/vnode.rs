use crate::network::Network;
use crate::node::Segment;
use crate::placement::{Placement, NodeDistRow, NodeRangeRow, PNodeVNodeDetailRow};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub struct VNetwork {
    inner: Network,
    pnodes: usize,
    vnodes_per_node: usize,
    owner: Vec<usize>, // vnode -> pnode (interleaved: v % pnodes)
}

impl VNetwork {
    pub fn new(pnodes: usize, vnodes_per_node: usize, m: usize, tail_bits: u8) -> Self {
        let total = pnodes * vnodes_per_node;
        let inner = Network::new(total, m, tail_bits);
        let mut me = Self { inner, pnodes, vnodes_per_node, owner: vec![0; total] };
        for v in 0..me.owner.len() { me.owner[v] = v % pnodes; }
        me
    }
    #[inline] fn pidx(&self, vnode_idx: usize) -> usize { self.owner[vnode_idx] }

    pub fn dump_owner_csv<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        let mut w = BufWriter::new(File::create(path)?);
        writeln!(w, "vnode_idx,pnode_idx")?;
        for (v, &p) in self.owner.iter().enumerate() {
            writeln!(w, "{},{}", v, p)?;
        }
        Ok(())
    }
}

impl Placement for VNetwork {
    #[inline]
    fn node_id(&self, idx: usize) -> u64 { self.inner.node_id(idx) }

    #[inline]
    fn node_responsible_interval(&self, idx: usize) -> (u64, u64, bool) {
        self.inner.node_responsible_interval(idx)
    }

    #[inline]
    fn insert(&mut self, entry_node: usize, seg: Segment) -> usize {
        self.inner.insert(entry_node, seg)
    }

    #[inline]
    fn query_range(&self, entry_node: usize, key_range: (u64, u64)) -> (Vec<&Segment>, usize) {
        self.inner.query_range(entry_node, key_range)
    }

    fn query_range_with_nodes(
        &self,
        entry_node: usize,
        key_range: (u64, u64),
    ) -> (Vec<(usize, &Segment)>, usize, Vec<usize>) {
        let (pairs, hops, visited_vnodes) = self.inner.query_range_with_nodes(entry_node, key_range);
        let mapped_pairs: Vec<(usize, &Segment)> =
            pairs.into_iter().map(|(vi, s)| (self.pidx(vi), s)).collect();
        let mut phys_visited: Vec<usize> = visited_vnodes.into_iter().map(|vi| self.pidx(vi)).collect();
        phys_visited.sort_unstable();
        phys_visited.dedup();
        (mapped_pairs, hops, phys_visited)
    }

    fn node_distribution_rows(&self) -> Vec<NodeDistRow> {
        // Aggregate vnode statistics back to pnodes
        let vnode_rows = self.inner.node_distribution_rows(); // (vi, id, total, mn, mx)
        let mut agg: Vec<NodeDistRow> = (0..self.pnodes).map(|pi| (pi, 0u64, 0usize, None, None)).collect();
        for (vi, id, total, mn, mx) in vnode_rows {
            let pi = self.pidx(vi);
            let slot = &mut agg[pi];
            if slot.1 == 0 { slot.1 = id; } // representative id
            slot.2 += total;
            slot.3 = match (slot.3, mn) { (Some(a), Some(b)) => Some(a.min(b)), (None, s) => s, (a, None) => a };
            slot.4 = match (slot.4, mx) { (Some(a), Some(b)) => Some(a.max(b)), (None, s) => s, (a, None) => a };
        }
        agg
    }

    fn print_node_distribution(&self) {
        let rows = self.node_distribution_rows();
        println!("--- Physical node data distribution (aggregated from vnodes) ---");
        for (idx, id, total, mn, mx) in rows {
            println!("PNode {} (rep id={}) -> {} records, key range [{:?}, {:?}]", idx, id, total, mn, mx);
        }
    }

    fn as_any(&self) -> &dyn std::any::Any { self }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }

    // ========= The three requested export helpers =========

    fn export_node_ranges(&self) -> Vec<NodeRangeRow> {
        // Goal: one row per pnode summary, without fabricating a continuous interval
        let total_v = self.inner.node_count(); // getter provided by network.rs
        let mut out: Vec<NodeRangeRow> = Vec::with_capacity(self.pnodes);

        for p in 0..self.pnodes {
            let mut rep_id: u64 = 0;
            let mut stored_total: usize = 0;
            let mut stored_min: Option<u64> = None;
            let mut stored_max: Option<u64> = None;

            let mut v = p;
            let mut first = true;
            while v < total_v {
                if first { rep_id = self.inner.node_id(v); first = false; }
                let (t, mn, mx) = self.inner.node_stats_range(v);
                stored_total += t;
                if let Some(mn) = mn {
                    stored_min = Some(stored_min.map(|x| x.min(mn)).unwrap_or(mn));
                }
                if let Some(mx) = mx {
                    stored_max = Some(stored_max.map(|x| x.max(mx)).unwrap_or(mx));
                }
                v += self.pnodes;
            }

            // Non-contiguous: mark as 0/0 + wrapped=true; use export_pnode_vnode_details() for the real intervals
            out.push((p, rep_id, 0, 0, true, stored_total, stored_min, stored_max));
        }
        out
    }


    fn export_node_data<'a>(&'a self, pnode_idx: usize) -> Vec<&'a Segment> {
        // Goal: return the combined data owned by all vnodes under this pnode
        let mut out: Vec<&Segment> = Vec::new();
        let total_v = self.inner.node_count(); // or self.owner.len()
        let mut v = pnode_idx;
        while v < total_v {
            // Use the inherent Network method to export by vnode, then merge the results
            let it = Network::export_node_data(&self.inner, v);
            out.extend(it);
            v += self.pnodes;
        } 
        out
    }

    fn export_pnode_vnode_details(&self) -> Vec<PNodeVNodeDetailRow> {
        let total_v = self.inner.node_count();
        let mut out: Vec<PNodeVNodeDetailRow> = Vec::new();

        for p in 0..self.pnodes {
            let mut v = p;
            let p_rep = self.inner.node_id(v); // use the first owned vnode id as the representative id
            while v < total_v {
                let (rs, re, wrapped) = self.inner.node_responsible_interval(v);
                let (t, mn, mx) = self.inner.node_stats_range(v); // see the helper method note
                out.push((
                    p, p_rep,
                    v, self.inner.node_id(v),
                    rs, re, wrapped,
                    t, mn, mx
                ));
                v += self.pnodes;
            }
        }
        out
    }
}
