// 统一放置接口：Baseline / VNode / SmartVNode 都实现这个 Trait
use crate::node::Segment;


/// 节点分布行：(pnode_idx, node_id, total_count, min_key, max_key)
pub type NodeDistRow = (usize, u64, usize, Option<u64>, Option<u64>);
// ===== 新增类型别名（统一 CSV 行的含义） =====
pub type NodeRangeRow = (
    usize,         // node_idx —— 对 baseline = 物理节点；对 vnode = 物理节点（已聚合）
    u64,           // node_id  —— 对 vnode = 代表性 id（取旗下第一个 vnode 的 id）
    u64, u64,      // resp_start, resp_end  —— 负责区间；对 vnode 聚合仅为粗略范围
    bool,          // wrapped   —— 区间是否跨越 2^m
    usize,         // stored_total
    Option<u64>,   // stored_min
    Option<u64>,   // stored_max
);

// 每条行代表“某 pnode 下的某个 vnode 的细节”
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
    fn export_node_ranges(&self) -> Vec<NodeRangeRow>;
    fn export_node_data<'a>(&'a self, idx: usize) -> Vec<&'a crate::node::Segment>;
    fn export_pnode_vnode_details(&self) -> Vec<PNodeVNodeDetailRow>;

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
    fn export_node_ranges(&self) -> Vec<crate::placement::NodeRangeRow> {
        // 注意避免递归，显式调用固有方法
        Network::export_node_ranges(self)
            .into_iter()
            .map(|(i,id,rs,re,wrapped,total,mn,mx)| (i,id,rs,re,wrapped,total,mn,mx))
            .collect()
    }

    fn export_node_data<'a>(&'a self, idx: usize) -> Vec<&'a crate::node::Segment> {
        Network::export_node_data(self, idx).collect()
    }

    fn export_pnode_vnode_details(&self) -> Vec<crate::placement::PNodeVNodeDetailRow> {
        // baseline：pnode 与“vnode 等价项”是 1:1
        let mut out = Vec::new();
        for (i, node) in self.nodes.iter().enumerate() {
            let (rs, re, wrapped) = self.node_responsible_interval(i);
            let (total, mn, mx) = node.stats_range();
            out.push((
                i, node.node_id,   // pnode 视角
                i, node.node_id,   // vnode 等价视角
                rs, re, wrapped,
                total, mn, mx
            ));
        }
        out
    }
}
