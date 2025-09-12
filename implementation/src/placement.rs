// 统一放置接口：Baseline / VNode / SmartVNode 都实现这个 Trait
use crate::node::Segment;

/// 节点分布行：(node_idx, node_id, total_count, min_key, max_key)
pub type NodeDistRow = (usize, u64, usize, Option<u64>, Option<u64>);

pub trait Placement {
    fn node_id(&self, idx: usize) -> u64;
    fn node_responsible_interval(&self, idx: usize) -> (u64, u64, bool);

    /// 插入：返回路由跳数
    fn insert(&mut self, entry_node: usize, seg: Segment) -> usize;

    /// 区间查询（不带节点上下文）
    fn query_range(&self, entry_node: usize, key_range: (u64, u64)) -> (Vec<&Segment>, usize);

    /// 区间查询（带节点上下文）
    fn query_range_with_nodes(
        &self,
        entry_node: usize,
        key_range: (u64, u64),
    ) -> (Vec<(usize, &Segment)>, usize, Vec<usize>);

    /// 节点分布行
    fn node_distribution_rows(&self) -> Vec<NodeDistRow>;

    fn print_node_distribution(&self);
}

// ====== Baseline：Network 直接作为 Placement 实现 ======
use crate::network::Network;

impl Placement for Network {
    #[inline]
    fn node_id(&self, idx: usize) -> u64 {
        // 调用 Network 的固有方法（不同名，避免递归）
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

    #[inline]
    fn node_distribution_rows(&self) -> Vec<NodeDistRow> {
        Network::node_distribution_rows(self)
    }

    #[inline]
    fn print_node_distribution(&self) {
        Network::print_node_distribution(self)
    }
}
