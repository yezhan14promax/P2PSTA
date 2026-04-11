// Unified placement interface: Baseline / VNode / SmartVNode all implement this trait
use crate::node::Segment;
use std::any::Any;

/// Node distribution row: (pnode_idx, node_id, total_count, min_key, max_key)
pub type NodeDistRow = (usize, u64, usize, Option<u64>, Option<u64>);
// ===== Type aliases added to unify CSV row semantics =====
pub type NodeRangeRow = (
    usize,         // node_idx: physical node for baseline; aggregated physical node for vnode
    u64,           // node_id: representative id for vnode mode (the first vnode id owned by the pnode)
    u64, u64,      // resp_start, resp_end: responsible interval; only an approximate range after vnode aggregation
    bool,          // wrapped: whether the interval crosses 2^m
    usize,         // stored_total
    Option<u64>,   // stored_min
    Option<u64>,   // stored_max
);

// Each row captures the details of one vnode under a pnode
pub type PNodeVNodeDetailRow = (
    usize,   // pnode_idx
    u64,     // pnode_rep_id
    usize,   // vnode_idx
    u64,     // vnode_id
    u64, u64,// vnode_resp_start, vnode_resp_end
    bool,    // vnode_wrapped
    usize,   // vnode_stored_total
    Option<u64>, // vnode_stored_min
    Option<u64>, // vnode_stored_max
);

pub trait Placement: Any {
    fn node_id(&self, idx: usize) -> u64;
    fn node_responsible_interval(&self, idx: usize) -> (u64, u64, bool);

    /// Insert and return the routing hop count
    fn insert(&mut self, entry_node: usize, seg: crate::node::Segment) -> usize;

    /// Range query without node context
    fn query_range(&self, entry_node: usize, key_range: (u64, u64)) -> (Vec<&crate::node::Segment>, usize);

    /// Range query with node context
    fn query_range_with_nodes(
        &self,
        entry_node: usize,
        key_range: (u64, u64),
    ) -> (Vec<(usize, &crate::node::Segment)>, usize, Vec<usize>);

    /// Node distribution rows
    fn node_distribution_rows(&self) -> Vec<crate::placement::NodeDistRow>;
    fn print_node_distribution(&self);
    fn export_node_ranges(&self) -> Vec<crate::placement::NodeRangeRow>;
    fn export_node_data<'a>(&'a self, idx: usize) -> Vec<&'a crate::node::Segment>;
    fn export_pnode_vnode_details(&self) -> Vec<crate::placement::PNodeVNodeDetailRow>;

    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}


// ====== Baseline: Network directly implements Placement ======
use crate::network::Network;

impl Placement for Network {
    #[inline]
    fn node_id(&self, idx: usize) -> u64 {
        // Call the inherent Network method directly to avoid recursion
        self.node_id_of(idx)
    }

    #[inline]
    fn node_responsible_interval(&self, idx: usize) -> (u64, u64, bool) {
        Network::node_responsible_interval(self, idx)
    }

    #[inline]
    fn insert(&mut self, entry_node: usize, seg: Segment) -> usize {
        Network::insert(self, entry_node, seg)
    }

    #[inline]
    fn query_range(&self, entry_node: usize, key_range: (u64, u64)) -> (Vec<&Segment>, usize) {
        Network::query_range(self, entry_node, key_range)
    }

    #[inline]
    fn query_range_with_nodes(
        &self,
        entry_node: usize,
        key_range: (u64, u64),
    ) -> (Vec<(usize, &Segment)>, usize, Vec<usize>) {
        Network::query_range_with_nodes(self, entry_node, key_range)
    }

    fn as_any(&self) -> &dyn std::any::Any { self }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }

    #[inline]
    fn node_distribution_rows(&self) -> Vec<NodeDistRow> {
        Network::node_distribution_rows(self)
    }

    #[inline]
    fn print_node_distribution(&self) {
        Network::print_node_distribution(self)
    }
    fn export_node_ranges(&self) -> Vec<crate::placement::NodeRangeRow> {
        // Avoid recursion by explicitly calling the inherent method
        Network::export_node_ranges(self)
            .into_iter()
            .map(|(i,id,rs,re,wrapped,total,mn,mx)| (i,id,rs,re,wrapped,total,mn,mx))
            .collect()
    }

    fn export_node_data<'a>(&'a self, idx: usize) -> Vec<&'a crate::node::Segment> {
        Network::export_node_data(self, idx).collect()
    }

    fn export_pnode_vnode_details(&self) -> Vec<crate::placement::PNodeVNodeDetailRow> {
        // In baseline mode, the pnode and its vnode-equivalent view are 1:1
        let mut out = Vec::new();
        for (i, node) in self.nodes.iter().enumerate() {
            let (rs, re, wrapped) = self.node_responsible_interval(i);
            let (total, mn, mx) = node.stats_range();
            out.push((
                i, node.node_id,   // pnode view
                i, node.node_id,   // vnode-equivalent view
                rs, re, wrapped,
                total, mn, mx
            ));
        }
        out
    }
}
