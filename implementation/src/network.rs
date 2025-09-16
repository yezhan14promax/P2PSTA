use crate::node::{Node, Segment};

/// Chord 风格的 DHT 网络
#[derive(Debug)]
pub struct Network {
    pub nodes: Vec<Node>,
    pub m: usize,                 // 环位宽
    pub node_ids: Vec<u64>,       // 升序排列的节点 ID（环上位置）
    pub fingers: Vec<Vec<usize>>, // fingers[i][k] = 节点 i 在 2^k 跳后的后继节点下标
    pub total_inserts: usize,
}

impl Network {
    /// 构建一个等距分布在环上的网络
    pub fn new(num_nodes: usize, m: usize, tail_bits: u8) -> Self {
        let n = num_nodes.max(1);
        let ring: u128 = if m >= 64 { u128::MAX } else { 1u128 << m };
        let step: u128 = ring / (n as u128);

        let mut nodes = Vec::with_capacity(n);
        let mut node_ids = Vec::with_capacity(n);
        for i in 0..n {
            let id = (step * i as u128) as u64;
            node_ids.push(id);
            nodes.push(Node::new(id, m, tail_bits));
        }

        let mut net = Self {
            nodes,
            m,
            node_ids,
            fingers: Vec::new(),
            total_inserts: 0,
        };
        net.rebuild_fingers();
        net
    }

    /// 重新构建 finger table
    pub fn rebuild_fingers(&mut self) {
        let n = self.nodes.len().max(1);
        self.fingers = vec![Vec::new(); n];

        for i in 0..n {
            let mut tbl = Vec::new();
            // 防止 m>63 导致移位 UB
            let max_k = self.m.min(63);
            for k in 0..max_k {
                let target = self.nodes[i].node_id.wrapping_add(1u64 << k);
                let succ = self.successor_index(target);
                tbl.push(succ);
            }
            self.fingers[i] = tbl;
        }
    }

    /// 在 node_ids 中二分，找 key 的后继下标
    #[inline]
    pub fn successor_index(&self, key: u64) -> usize {
        match self.node_ids.binary_search(&key) {
            Ok(i) => i,
            Err(i) => {
                if i >= self.node_ids.len() { 0 } else { i }
            }
        }
    }

    #[inline]
    fn in_interval_open_closed(&self, x: u64, a: u64, b: u64) -> bool {
        // 环上的 (a, b]
        if a < b {
            x > a && x <= b
        } else if a > b {
            x > a || x <= b
        } else {
            true
        }
    }

    fn closest_preceding_finger(&self, idx: usize, key: u64) -> usize {
        let table = &self.fingers[idx];
        let my_id = self.nodes[idx].node_id;
        for &j in table.iter().rev() {
            let cand = self.nodes[j].node_id;
            if self.in_interval_open_closed(cand, my_id, key.wrapping_sub(1)) {
                return j;
            }
        }
        idx
    }

    /// 从入口节点出发，找到 key 的后继（返回：(后继下标, 跳数)）
    pub fn find_successor_from(&self, mut idx: usize, key: u64) -> (usize, usize) {
        let mut hops = 0usize;

        // 先看自己与后继能否直接覆盖
        let succ0 = self.fingers[idx][0];
        let my0 = self.nodes[idx].node_id;
        if self.in_interval_open_closed(key, my0, self.nodes[succ0].node_id) {
            return (succ0, 1);
        }

        loop {
            let next = self.closest_preceding_finger(idx, key);
            idx = if next == idx { self.fingers[idx][0] } else { next };
            hops += 1;

            let succ = self.fingers[idx][0];
            let my = self.nodes[idx].node_id;
            if self.in_interval_open_closed(key, my, self.nodes[succ].node_id) {
                return (succ, hops + 1);
            }
            if hops > self.nodes.len() + 2 { break; }
        }
        (self.fingers[idx][0], hops.max(1))
    }

    /// 插入：从 entry_node 出发找到 key 的负责节点并写入；返回路由跳数
    pub fn insert(&mut self, entry_node: usize, seg: Segment) -> usize {
        let (idx, hops) = self.find_successor_from(entry_node % self.nodes.len().max(1), seg.sfc_key);
        self.nodes[idx].insert(seg);
        self.total_inserts += 1;
        hops
    }

    /// 计算某个节点在环上的**负责区间**：(prev(node).id + 1 ..= node.id)，返回 (start, end, wrapped)
    fn node_interval(&self, idx: usize) -> (u64, u64, bool) {
        let n = self.nodes.len();
        if n == 0 { return (0, 0, false); }
        let prev = if idx == 0 { n - 1 } else { idx - 1 };
        let prev_id = self.nodes[prev].node_id;
        let id = self.nodes[idx].node_id;
        let start = prev_id.wrapping_add(1);
        let end = id;
        let wrapped = start > end;
        (start, end, wrapped)
    }

    /// 公开的负责区间（便于统计/校验）
    pub fn node_responsible_interval(&self, idx: usize) -> (u64, u64, bool) {
        self.node_interval(idx)
    }

    /// 简化：只取 (start, end)
    pub fn node_key_range(&self, idx: usize) -> (u64, u64) {
        let (s, e, _) = self.node_interval(idx);
        (s, e)
    }

    /// 返回某节点的 ID（环位置）
    pub fn node_id_of(&self, idx: usize) -> u64 {
        self.nodes[idx].node_id
    }

    /// 基本区间查询（不返回节点信息）：返回 (命中 segment 列表, 跳数)
    pub fn query_range(&self, entry_node: usize, key_range: (u64, u64)) -> (Vec<&Segment>, usize) {
        let (s, e) = key_range;
        let n = self.nodes.len();
        if n == 0 { return (Vec::new(), 0); }
        if n == 1 {
            let (local, _) = self.nodes[0].query_range((s, e));
            return (local, 1);
        }

        let (mut idx, mut hops) = self.find_successor_from(entry_node % n, s);
        let start_idx = idx;
        let mut hits: Vec<&Segment> = Vec::new();
        let mut touched = 0usize;

        loop {
            let (start, end, wrapped) = self.node_interval(idx);
            if !wrapped {
                let sub_s = s.max(start);
                let sub_e = e.min(end);
                if sub_s <= sub_e {
                    let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                    hits.extend(local);
                }
            } else {
                // wrap 情况拆两段
                if e >= start {
                    let a_s = s.max(start);
                    let a_e = e;
                    if a_s <= a_e {
                        let (local, _) = self.nodes[idx].query_range((a_s, a_e));
                        hits.extend(local);
                    }
                }
                if s <= end {
                    let b_s = s;
                    let b_e = e.min(end);
                    if b_s <= b_e {
                        let (local, _) = self.nodes[idx].query_range((b_s, b_e));
                        hits.extend(local);
                    }
                }
            }

            touched += 1;
            if touched > n + 1 { break; } // 保险
            let succ = self.fingers[idx][0];
            idx = succ;
            hops += 1;

            if idx == start_idx { break; }
            let (_, end_cur, wrapped_cur) = self.node_interval(idx);
            if !wrapped_cur && end_cur > e { break; }
        }

        (hits, hops.max(1))
    }

    /// 区间查询（返回节点上下文）：( (node_idx, &Segment) 列表, 跳数, 命中节点集合 )
    pub fn query_range_with_nodes(
        &self,
        entry_node: usize,
        key_range: (u64, u64),
    ) -> (Vec<(usize, &Segment)>, usize, Vec<usize>) {
        let (s, e) = key_range;
        let n = self.nodes.len();
        if n == 0 { return (Vec::new(), 0, Vec::new()); }
        if n == 1 {
            let (local, _) = self.nodes[0].query_range((s, e));
            let hits = local.into_iter().map(|seg| (0usize, seg)).collect();
            return (hits, 1, vec![0]);
        }

        // hops 只取 finger 路由步数
        let (mut idx, route_hops) = self.find_successor_from(entry_node % n, s);
        let mut hits: Vec<(usize, &Segment)> = Vec::new();
        let mut touched_nodes: Vec<usize> = Vec::new();
        let mut visits = 0usize;

        let query_wrapped = s > e;
        let mut crossed_zero = false;

        loop {
            let (start, end, wrapped_node) = self.node_interval(idx);

            // 与查询区间求交并查本地
            if !query_wrapped {
                // 查询不 wrap：与当前节点（可能 wrap 或不 wrap）求交
                if !wrapped_node {
                    let sub_s = s.max(start);
                    let sub_e = e.min(end);
                    if sub_s <= sub_e {
                        let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                        for seg in local { hits.push((idx, seg)); }
                    }
                    // 关键：一旦该节点的 end 覆盖到 e，则早停
                    if end >= e {
                        if touched_nodes.last().copied() != Some(idx) {
                            touched_nodes.push(idx);
                        }
                        break;
                    }
                } else {
                    // 节点自身 wrap：分两段与 [s,e] 求交
                    if e >= start {
                        let sub_s = s.max(start);
                        let sub_e = e;
                        if sub_s <= sub_e {
                            let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                            for seg in local { hits.push((idx, seg)); }
                        }
                    }
                    if s <= end {
                        let sub_s = s;
                        let sub_e = e.min(end);
                        if sub_s <= sub_e {
                            let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                            for seg in local { hits.push((idx, seg)); }
                        }
                    }
                    // 节点 wrap 时不一定已经覆盖到 e，继续推进到下一个非 wrap 节点，届时检查 end>=e 再停
                }
            } else {
                // 查询 wrap：覆盖 [s..MAX] ∪ [0..e]
                // 第一段：[s..MAX]
                if !crossed_zero {
                    if !wrapped_node {
                        let sub_s = s.max(start);
                        let sub_e = u64::MAX.min(end);
                        if sub_s <= sub_e {
                            let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                            for seg in local { hits.push((idx, seg)); }
                        }
                        // 如果当前节点 end == u64::MAX，下一跳必跨零
                        if end == u64::MAX { crossed_zero = true; }
                    } else {
                        // 节点 wrap：也会覆盖到 MAX，下一跳跨零
                        let sub_s = s.max(start);
                        let sub_e = u64::MAX;
                        if sub_s <= sub_e {
                            let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                            for seg in local { hits.push((idx, seg)); }
                        }
                        crossed_zero = true;
                    }
                } else {
                    // 第二段：[0..e]
                    if !wrapped_node {
                        let sub_s = 0u64.max(start);
                        let sub_e = e.min(end);
                        if sub_s <= sub_e {
                            let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                            for seg in local { hits.push((idx, seg)); }
                        }
                        // 一旦 end >= e，第二段覆盖完成，早停
                        if end >= e {
                            if touched_nodes.last().copied() != Some(idx) {
                                touched_nodes.push(idx);
                            }
                            break;
                        }
                    } else {
                        // 节点 wrap：把 [0..end] 这段与 [0..e] 求交
                        if e <= end {
                            let (local, _) = self.nodes[idx].query_range((0, e));
                            for seg in local { hits.push((idx, seg)); }
                            if touched_nodes.last().copied() != Some(idx) {
                                touched_nodes.push(idx);
                            }
                            break;
                        } else {
                            let (local, _) = self.nodes[idx].query_range((0, end));
                            for seg in local { hits.push((idx, seg)); }
                        }
                    }
                }
            }

            if touched_nodes.last().copied() != Some(idx) {
                touched_nodes.push(idx);
            }

            // 推进到后继（线性推进，仅用于覆盖；不再计入 hops）
            visits += 1;
            if visits > n + 1 { break; } // 保险
            let succ = self.fingers[idx][0];
            if succ == idx { break; } // 退化
            idx = succ;
        }

        (hits, route_hops.max(1), touched_nodes)
    }



    /// 导出节点负责区间 + 存储数据范围（用于核对）
    /// (node_idx, node_id, resp_start, resp_end, wrapped, stored_total, stored_min, stored_max)
    pub fn export_node_ranges(&self) -> Vec<(usize, u64, u64, u64, bool, usize, Option<u64>, Option<u64>)> {
        let mut rows = Vec::new();
        for (i, node) in self.nodes.iter().enumerate() {
            let (rs, re, wrapped) = self.node_interval(i);
            let (total, mn, mx) = node.stats_range();
            rows.push((i, node.node_id, rs, re, wrapped, total, mn, mx));
        }
        rows
    }

    /// 导出某节点内**真实存储**的 Segment（迭代器）
    pub fn export_node_data(&self, idx: usize) -> impl Iterator<Item = &Segment> {
        self.nodes[idx].iter_segments()
    }

    /// 节点分布（原有 node_distribution 的结构化版本）
    pub fn node_distribution_rows(&self) -> Vec<(usize, u64, usize, Option<u64>, Option<u64>)> {
        let mut out = Vec::new();
        for (i, node) in self.nodes.iter().enumerate() {
            let (total, mn, mx) = node.stats_range();
            out.push((i, node.node_id, total, mn, mx));
        }
        out
    }

    /// 打印节点分布（调试用）
    pub fn print_node_distribution(&self) {
        println!("--- Node data distribution ---");
        for (i, node) in self.nodes.iter().enumerate() {
            let total = node.store_len();
            if total == 0 {
                println!("Node {} (ID={}): 0 records, key range [-, -]", i, node.node_id);
                continue;
            }
            let ( _t, mn, mx ) = node.stats_range();
            println!(
                "Node {} (ID={}): {} records, key range [{}, {}]",
                i, node.node_id, total,
                mn.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
                mx.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
            );
        }
    }
}
