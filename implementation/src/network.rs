use crate::node::{Node, Segment};

/// Chord 风格 DHT（指针路由 + finger table）
/// - m: 环/键位宽；节点均匀铺在 2^m 环上；fingers[i][k] 指向 successor(id_i + 2^k)
#[derive(Debug)]
pub struct Network {
    pub nodes: Vec<Node>,
    pub m: usize,
    pub total_inserts: usize,
    pub node_ids: Vec<u64>,        // 升序环序
    pub fingers: Vec<Vec<usize>>,  // fingers[i][k] = 节点索引
}

impl Network {
    /// 新建网络：把 stop_tail_bits 传给每个 Node（用于按桶存储/查询）
    pub fn new(num_nodes: usize, m: usize, tail_bits: u8) -> Self {
        let n = num_nodes.max(1);
        let mut nodes = Vec::with_capacity(n);
        let ring: u128 = 1u128 << m.min(63);
        let step: u128 = ring / (n as u128);
        let mut node_ids: Vec<u64> = Vec::with_capacity(n);
        for i in 0..n {
            let id = (step * (i as u128)) as u64;
            node_ids.push(id);
            nodes.push(Node::new(id, m, tail_bits));
        }
        let mut net = Self {
            nodes,
            m,
            total_inserts: 0,
            node_ids,
            fingers: Vec::new(),
        };
        net.rebuild_fingers();
        net
    }

    /// 重建 finger 表
    pub fn rebuild_fingers(&mut self) {
        let n = self.nodes.len().max(1);
        self.fingers = vec![Vec::new(); n];
        for i in 0..n {
            let mut table: Vec<usize> = Vec::new();
            for k in 0..self.m.min(63) {
                let key = self.nodes[i].node_id.wrapping_add(1u64 << k);
                let succ = self.successor_index(key);
                table.push(succ);
            }
            self.fingers[i] = table;
        }
        // 同步 finger 到 Node
        for (i, node) in self.nodes.iter_mut().enumerate() {
            node.finger = self.fingers[i].clone();
        }
    }

    /// key 的后继节点索引
    pub fn successor_index(&self, key: u64) -> usize {
        let n = self.node_ids.len();
        if n == 1 { return 0; }
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.node_ids[mid] >= key { hi = mid; } else { lo = mid + 1; }
        }
        if lo < n { lo } else { 0 }
    }

    /// x ∈ (a, b]（顺时针，含 b）
    #[inline]
    fn in_interval_open_closed(&self, x: u64, a: u64, b: u64) -> bool {
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
        let my = self.nodes[idx].node_id;
        for &j in table.iter().rev() {
            let cand = self.nodes[j].node_id;
            if self.in_interval_open_closed(cand, my, key.wrapping_sub(1)) {
                return j;
            }
        }
        idx
    }

    /// 从入口节点出发查找 key 的后继；返回(索引, hops)
    pub fn find_successor_from(&self, mut idx: usize, key: u64) -> (usize, usize) {
        let mut hops = 0usize;
        let succ0 = self.fingers[idx][0];
        let my0 = self.nodes[idx].node_id;
        if self.in_interval_open_closed(key, my0, self.nodes[succ0].node_id) {
            return (succ0, 1);
        }
        loop {
            let next = self.closest_preceding_finger(idx, key);
            if next == idx {
                idx = self.fingers[idx][0];
            } else {
                idx = next;
            }
            hops += 1;
            let succ = self.fingers[idx][0];
            let my = self.nodes[idx].node_id;
            if self.in_interval_open_closed(key, my, self.nodes[succ].node_id) {
                hops += 1;
                return (succ, hops);
            }
            if hops > self.nodes.len() * 4 {
                // fallback：避免极端情况下不收敛
                return (self.successor_index(key), hops);
            }
        }
    }

    /// 插入
    pub fn insert(&mut self, entry_node: usize, seg: Segment) -> usize {
        let (target, _hops) = self.find_successor_from(entry_node % self.nodes.len(), seg.hilbert_key);
        self.nodes[target].insert(seg);
        self.total_inserts += 1;
        target
    }

    /// 本节点负责的闭区间 [start, end] 以及是否跨零（Chord: (prev, self] -> [prev+1, self]）
    #[inline]
    fn node_interval(&self, idx: usize) -> (u64, u64, bool) {
        let n = self.nodes.len();
        if n == 1 {
            return (0, u64::MAX, false);
        }
        let n_id = self.nodes[idx].node_id;
        let prev_idx = if idx == 0 { n - 1 } else { idx - 1 };
        let prev_id = self.nodes[prev_idx].node_id;
        let start = prev_id.wrapping_add(1);
        let end = n_id;
        let wrapped = start > end; // 例如 prev=MAX-10, end=100
        (start, end, wrapped)
    }

    /// 基础查询（不返回节点信息）—— 正确使用 (prev, self] 作为节点负责区间；支持跨零
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
                // 节点负责 [start, end]
                let sub_s = s.max(start);
                let sub_e = e.min(end);
                if sub_s <= sub_e {
                    let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                    hits.extend(local);
                }
            } else {
                // 节点负责 [start..=MAX] ∪ [0..=end]
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

            // 是否已覆盖到 e？
            let done = if !wrapped {
                e <= end
            } else {
                // 跨零：只要 e 落在 [start..=MAX] 或 [0..=end] 任一侧
                e >= start || e <= end
            };
            if done { break; }

            // 前进到后继
            let succ = self.fingers[idx][0];
            idx = succ;
            hops += 1;

            if idx == start_idx || touched > n + 1 {
                break;
            }
        }

        (hits, hops.max(1))
    }

    /// 查询（返回节点信息 & 触达节点集合）—— 同上，且记录命中的节点索引
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

        let (mut idx, mut hops) = self.find_successor_from(entry_node % n, s);
        let start_idx = idx;
        let mut hits: Vec<(usize, &Segment)> = Vec::new();
        let mut touched_nodes: Vec<usize> = Vec::new();
        let mut touched = 0usize;

        loop {
            let (start, end, wrapped) = self.node_interval(idx);

            if !wrapped {
                let sub_s = s.max(start);
                let sub_e = e.min(end);
                if sub_s <= sub_e {
                    let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                    for seg in local { hits.push((idx, seg)); }
                }
            } else {
                if e >= start {
                    let a_s = s.max(start);
                    let a_e = e;
                    if a_s <= a_e {
                        let (local, _) = self.nodes[idx].query_range((a_s, a_e));
                        for seg in local { hits.push((idx, seg)); }
                    }
                }
                if s <= end {
                    let b_s = s;
                    let b_e = e.min(end);
                    if b_s <= b_e {
                        let (local, _) = self.nodes[idx].query_range((b_s, b_e));
                        for seg in local { hits.push((idx, seg)); }
                    }
                }
            }

            if touched_nodes.last().copied() != Some(idx) {
                touched_nodes.push(idx);
            }
            touched += 1;

            let done = if !wrapped {
                e <= end
            } else {
                e >= start || e <= end
            };
            if done { break; }

            let succ = self.fingers[idx][0];
            idx = succ;
            hops += 1;

            if idx == start_idx || touched > n + 1 {
                break;
            }
        }

        (hits, hops.max(1), touched_nodes)
    }

    /// 全网键空间包络（按“桶起点键”统计）：(global_min, global_max)。若无数据返回 None
    pub fn global_key_envelope(&self) -> Option<(u64, u64)> {
        let mut gmin = u64::MAX;
        let mut gmax = 0u64;
        let mut any = false;
        for node in &self.nodes {
            if let Some((&mn, _)) = node.storage.first_key_value() {
                any = true;
                if mn < gmin { gmin = mn; }
            }
            if let Some((&mx, _)) = node.storage.last_key_value() {
                any = true;
                if mx > gmax { gmax = mx; }
            }
        }
        if any { Some((gmin, gmax)) } else { None }
    }

    /// 导出节点分布
    pub fn node_distribution_rows(&self) -> Vec<(usize, u64, usize, Option<u64>, Option<u64>)> {
        let mut rows = Vec::with_capacity(self.nodes.len());
        for (i, node) in self.nodes.iter().enumerate() {
            let total_count: usize = node.storage.values().map(|v| v.len()).sum();
            if total_count == 0 {
                rows.push((i, node.node_id, 0, None, None));
                continue;
            }
            let mut mn = u64::MAX;
            let mut mx = 0u64;
            for (&k, _) in node.storage.iter() {
                if k < mn { mn = k; }
                if k > mx { mx = k; }
            }
            rows.push((i, node.node_id, total_count, Some(mn), Some(mx)));
        }
        rows
    }

    pub fn print_node_distribution(&self) {
        println!("--- Node data distribution ---");
        for (i, node) in self.nodes.iter().enumerate() {
            let total_count: usize = node.storage.values().map(|v| v.len()).sum();
            if total_count == 0 {
                println!("Node {} (ID={}): 0 records, key range [-, -]", i, node.node_id);
                continue;
            }
            let mut mn = u64::MAX;
            let mut mx = 0u64;
            for (&k, _) in node.storage.iter() {
                if k < mn { mn = k; }
                if k > mx { mx = k; }
            }
            println!(
                "Node {} (ID={}): {} records, key range [{}, {}]",
                i, node.node_id, total_count, mn, mx
            );
        }
    }
}
