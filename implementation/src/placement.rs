// src/placement.rs
// 放置/路由策略统一抽象：Baseline DHT / VNode / SmartVNode 都实现它。

use crate::node::Segment;

/// 节点分布统计行：node_idx, node_id, total_count, min_key, max_key
pub type NodeDistRow = (usize, u64, usize, Option<u64>, Option<u64>);

pub trait Placement {
    /// 插入：从入口节点出发（实现可利用 finger table / vnode 映射）
    fn insert(&mut self, entry_node: usize, seg: Segment) -> usize;

    /// 查询区间 [s,e]：返回 (命中列表, hops)
    fn query_range(&self, entry_node: usize, key_range: (u64, u64)) -> (Vec<&Segment>, usize);

    /// 查询区间 [s,e]（携带命中所在的节点索引）：
    /// 返回 ( (node_idx, &Segment) 列表, hops, 触达的去重节点列表 )
    fn query_range_with_nodes(
        &self,
        entry_node: usize,
        key_range: (u64, u64),
    ) -> (Vec<(usize, &Segment)>, usize, Vec<usize>);

    /// 节点分布统计（便于落盘）
    fn node_distribution_rows(&self) -> Vec<NodeDistRow>;

    /// 打印节点分布（可选）
    fn print_node_distribution(&self);
}

// ========== Baseline：直接用现有 Network 实现 ==========
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
