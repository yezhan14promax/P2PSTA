// Unified abstraction for placement/routing strategies: implemented by Baseline DHT / VNode / SmartVNode.

use crate::node::Segment;

/// Node distribution row: (node_idx, node_id, total_count, min_key, max_key)
pub type NodeDistRow = (usize, u64, usize, Option<u64>, Option<u64>);

pub trait Placement {
    /// Insert: starting from the entry node (implementations may use a finger table / vnode mapping)
    fn insert(&mut self, entry_node: usize, seg: Segment) -> usize;

    /// Query the range [s, e]: returns (hit list, hops)
    fn query_range(&self, entry_node: usize, key_range: (u64, u64)) -> (Vec<&Segment>, usize);

    /// Query the range [s, e] (including the node index of each hit):
    /// Returns (list of (node_idx, &Segment), hops, distinct list of nodes reached)
    fn query_range_with_nodes(
        &self,
        entry_node: usize,
        key_range: (u64, u64),
    ) -> (Vec<(usize, &Segment)>, usize, Vec<usize>);

    /// Node distribution statistics (for persistence)
    fn node_distribution_rows(&self) -> Vec<NodeDistRow>;

    /// Print node distribution (optional)
    fn print_node_distribution(&self);
}

// ========== Baseline: Using the existing Network implementation directly ==========
use crate::network::Network;

impl Placement for Network {
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

    #[inline]
    fn node_distribution_rows(&self) -> Vec<NodeDistRow> {
        Network::node_distribution_rows(self)
    }

    #[inline]
    fn print_node_distribution(&self) {
        Network::print_node_distribution(self)
    }
}
