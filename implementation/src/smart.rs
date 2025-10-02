// src/smart.rs
use std::cmp::{max, min};
use std::any::Any;

use crate::node::Segment;
use crate::network::Network;
use crate::placement::{Placement, NodeRangeRow, PNodeVNodeDetailRow};

#[derive(Debug)]
pub struct SmartDirect {
    // 路由器：Chord on pNode ring（仅用于路由/跳数）
    inner: Network,

    // pNode 数、位宽
    pnodes: usize,
    m: usize,

    // 导入阶段：先缓存在内存，finalize 后再分桶
    buffer: Vec<Segment>,

    // finalize 之后：
    buckets: Vec<Vec<Segment>>,     // 每个 pnode 一个桶（连续装箱后的数据）
    ranges:  Vec<(u64, u64)>,       // 每个 pnode 的 [start_key, end_key]（闭区间）
    node_ids: Vec<u64>,             // 每个 pnode 的 node_id（= end_key）
    finalized: bool,
}

impl SmartDirect {
    pub fn new(num_pnodes: usize, m: usize, tail_bits: u8) -> Self {
        let p = num_pnodes.max(1);
        // inner 用于 Chord 路由，初始化后会在 finalize() 里重写 node_id 并 rebuild fingers
        let inner = Network::new(p, m, tail_bits);
        Self {
            inner,
            pnodes: p,
            m,
            buffer: Vec::new(),
            buckets: vec![Vec::new(); p],
            ranges: vec![(0, 0); p],
            node_ids: vec![0; p],
            finalized: false,
        }
    }

    /// 两阈值连续装箱（L/H 比例来自配置）
    pub fn finalize(&mut self, low_ratio: f64, high_ratio: f64) {
        if self.finalized { return; }

        // 1) SFC 升序稳定排序
        self.buffer.sort_by_key(|s| s.sfc_key);

        let total = self.buffer.len();
        if total == 0 {
            // 空数据：node_id/range 保持 0，重建 finger 以免崩
            self.rebuild_ring_and_fingers();
            self.finalized = true;
            return;
        }

        let p = self.pnodes;
        let mu = (total as f64) / (p as f64);
        let low = (mu * low_ratio).floor() as usize;
        let high = (mu * high_ratio).ceil() as usize;
        let low = low.max(1); // 保底

        // 2) 以 “编码段”（同 sfc_key 的连续 run）为单位遍历
        let mut buckets: Vec<Vec<Segment>> = vec![Vec::new(); p];
        let mut end_keys: Vec<u64> = vec![0; p];

        let first_key = self.buffer[0].sfc_key;
        let mut start_keys: Vec<u64> = vec![first_key; p];

        let mut pi: usize = 0;     // 当前 pnode
        let mut acc: usize = 0;    // 当前 pnode 已装条数

        let mut i = 0;
        while i < total {
            let key = self.buffer[i].sfc_key;
            // 找到该 key 的 run 末尾 [i, j)
            let mut j = i + 1;
            while j < total && self.buffer[j].sfc_key == key { j += 1; }
            let mut remain = j - i;

            // 可能需要把一个巨大的 run 拆到多个 pnode
            let mut cursor = i;
            while remain > 0 {
                // 如果已用尽 pnode，就全塞最后一个
                if pi >= p {
                    let last = p - 1;
                    buckets[last].extend_from_slice(&self.buffer[cursor..j]);
                    end_keys[last] = key;
                    acc = 0;
                    remain = 0;
                    break;
                }

                // 当前 pnode 还没达到 low：倾向整段放入，但不能超过 high
                if acc < low {
                    let cap = high.saturating_sub(acc); // 最大还能放多少
                    if cap == 0 {
                        // 已经卡在 high，切到下一个 pnode
                        pi += 1;
                        if pi < p { start_keys[pi] = end_keys[pi - 1]; }
                        acc = 0;
                        continue;
                    }
                    let take = min(cap, remain);
                    if take == 0 {
                        // 理论上不会到这
                        pi += 1;
                        if pi < p { start_keys[pi] = end_keys[pi - 1]; }
                        acc = 0;
                        continue;
                    }
                    buckets[pi].extend_from_slice(&self.buffer[cursor..(cursor + take)]);
                    end_keys[pi] = key;
                    acc += take;
                    cursor += take;
                    remain -= take;

                    // 如果这一段被截断（acc 达到 high），切 pnode
                    if remain > 0 && acc >= high {
                        pi += 1;
                        if pi < p { start_keys[pi] = end_keys[pi - 1]; }
                        acc = 0;
                    }
                } else {
                    // acc >= low：优先“把整个编码段吃完”
                    let will = acc + remain;
                    if will <= high {
                        // 整段吃完，然后切 pnode
                        buckets[pi].extend_from_slice(&self.buffer[cursor..j]);
                        end_keys[pi] = key;
                        acc = will;
                        cursor = j;
                        remain = 0;

                        pi += 1;
                        if pi < p { start_keys[pi] = end_keys[pi - 1]; }
                        acc = 0;
                    } else {
                        // 超过 high：在本 key 处“立即截断”
                        let cap = high.saturating_sub(acc);
                        let take = cap.max(1); // 至少拿 1 条，避免死循环
                        let take = min(take, remain);
                        buckets[pi].extend_from_slice(&self.buffer[cursor..(cursor + take)]);
                        end_keys[pi] = key;
                        acc += take;
                        cursor += take;
                        remain -= take;

                        // 切 pnode，把该 key 的剩余留给下一个 pnode
                        pi += 1;
                        if pi < p { start_keys[pi] = end_keys[pi - 1]; }
                        acc = 0;
                    }
                }
            }

            i = j;
        }

        // 若还有未用 pnode，按照“start=前一 end”的定义把 ranges 铺满
        for k in 0..p {
            if buckets[k].is_empty() {
                // 空桶：沿用前一 end 作为 start/end
                if k > 0 { start_keys[k] = end_keys[k - 1]; }
                end_keys[k] = start_keys[k];
            }
        }

        // 写入成员字段
        self.buckets = buckets;
        self.ranges = (0..p)
            .map(|k| (start_keys[k], end_keys[k]))
            .collect();
        self.node_ids = end_keys.clone();

        // 3) 重写 inner 的 node_id 并重建 finger table（Chord on pNode ring）
        self.rebuild_ring_and_fingers();

        // buffer 可释放以节省内存
        self.buffer.clear();
        self.finalized = true;
    }

    fn rebuild_ring_and_fingers(&mut self) {
        // 将 inner 的 node_id 改为 end_key，使 (prev+1..=id) 正好是我们分配的区间
        for (i, id) in self.node_ids.iter().copied().enumerate() {
            // inner.nodes[i].node_id, inner.node_ids[i]
            if i < self.inner.nodes.len() {
                self.inner.nodes[i].node_id = id;
            }
            if i < self.inner.node_ids.len() {
                self.inner.node_ids[i] = id;
            }
        }
        self.inner.rebuild_fingers();
    }

    #[inline]
    fn pnode_count(&self) -> usize { self.pnodes }

    #[inline]
    fn stats_of_bucket(&self, idx: usize) -> (usize, Option<u64>, Option<u64>) {
        if idx >= self.buckets.len() { return (0, None, None); }
        let b = &self.buckets[idx];
        if b.is_empty() { return (0, None, None); }
        let mut mn = u64::MAX;
        let mut mx = 0u64;
        for s in b.iter() {
            mn = min(mn, s.sfc_key);
            mx = max(mx, s.sfc_key);
        }
        (b.len(), Some(mn), Some(mx))
    }
}

impl Placement for SmartDirect {
    // --- 基本信息 ---
    fn node_id(&self, idx: usize) -> u64 {
        self.node_ids.get(idx).copied().unwrap_or(0)
    }

    fn node_responsible_interval(&self, idx: usize) -> (u64, u64, bool) {
        if idx >= self.ranges.len() { return (0, 0, false); }
        let (s, e) = self.ranges[idx];
        (s, e, false) // 非 wrap
    }

    fn node_distribution_rows(&self) -> Vec<(usize, u64, usize, Option<u64>, Option<u64>)> {
        let mut out = Vec::with_capacity(self.pnode_count());
        for i in 0..self.pnode_count() {
            let (t, mn, mx) = self.stats_of_bucket(i);
            out.push((i, self.node_id(i), t, mn, mx));
        }
        out
    }

    fn insert(&mut self, _entry_node: usize, seg: Segment) -> usize {
        self.buffer.push(seg);
        1
    }

    fn query_range(&self, entry_node: usize, key_range: (u64, u64)) -> (Vec<&Segment>, usize) {
        let (pairs, hops, _touched) = self.query_range_with_nodes(entry_node, key_range);
        let out = pairs.into_iter().map(|(_pi, s)| s).collect();
        (out, hops)
    }

    fn print_node_distribution(&self) {
        println!("--- SmartDirect pNode data distribution ---");
        for i in 0..self.pnode_count() {
            let (t, mn, mx) = self.stats_of_bucket(i);
            if t == 0 {
                println!("pNode {} (ID={}): 0 records, key range [-, -]", i, self.node_id(i));
            } else {
                println!(
                    "pNode {} (ID={}): {} records, key range [{}, {}]",
                    i, self.node_id(i), t,
                    mn.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
                    mx.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
                );
            }
        }
    }

    // --- 查询：用于统计 hops + 诊断（真实取数仍然“每 pnode 一次拉取”） ---
    fn query_range_with_nodes(
        &self,
        entry_node: usize,
        key_range: (u64, u64),
    ) -> (Vec<(usize, &Segment)>, usize, Vec<usize>) {
        let (s, e) = key_range;
        let n = self.pnode_count();
        if n == 0 { return (Vec::new(), 0, Vec::new()); }

        // 用 inner（Chord on pNode ring）路由到 s 的后继，拿到 hops
        let (mut idx, hops) = self.inner.find_successor_from(entry_node % n, s);

        // 收集触达的 pnode，并写出 (idx,&Segment)（仅用于诊断）
        let mut touched: Vec<usize> = Vec::new();
        let mut out: Vec<(usize, &Segment)> = Vec::new();
        let mut visits = 0usize;

        loop {
            if !touched.contains(&idx) { touched.push(idx); }
            let (rs, re, _w) = self.node_responsible_interval(idx);

            // 与当前 pnode 区间求交
            let (ia, ib);
            if s <= e {
                // 查询不 wrap
                ia = max(s, rs);
                ib = min(e, re);
                if ia <= ib {
                    for seg in self.buckets[idx].iter() {
                        if seg.sfc_key >= ia && seg.sfc_key <= ib {
                            out.push((idx, seg));
                        }
                    }
                }
                if re >= e { break; }
            } else {
                // 查询 wrap： [s..MAX] ∪ [0..e]
                // 第一段
                let a1 = max(s, rs);
                let b1 = min(u64::MAX, re);
                if a1 <= b1 {
                    for seg in self.buckets[idx].iter() {
                        if seg.sfc_key >= a1 && seg.sfc_key <= b1 {
                            out.push((idx, seg));
                        }
                    }
                }
                // 第二段
                let a2 = max(0, rs);
                let b2 = min(e, re);
                if a2 <= b2 {
                    for seg in self.buckets[idx].iter() {
                        if seg.sfc_key >= a2 && seg.sfc_key <= b2 {
                            out.push((idx, seg));
                        }
                    }
                }
                // 早停：若第二段已覆盖到 e
                if re >= e { break; }
            }

            visits += 1;
            if visits > n + 1 { break; }
            let succ = self.inner.fingers[idx][0];
            if succ == idx { break; }
            idx = succ;
        }

        (out, hops.max(1), touched)
    }

    // --- 导出 ---
    fn export_node_ranges(&self) -> Vec<NodeRangeRow> {
        // (pnode_idx, node_id, resp_start, resp_end, wrapped, stored_total, stored_min, stored_max)
        let mut rows = Vec::with_capacity(self.pnode_count());
        for i in 0..self.pnode_count() {
            let (rs, re, w) = self.node_responsible_interval(i);
            let (t, mn, mx) = self.stats_of_bucket(i);
            rows.push((i, self.node_id(i), rs, re, w, t, mn, mx));
        }
        rows
    }

    fn export_node_data<'a>(&'a self, idx: usize) -> Vec<&'a Segment> {
        if idx >= self.buckets.len() { return Vec::new(); }
        self.buckets[idx].iter().collect()
    }

    fn export_pnode_vnode_details(&self) -> Vec<PNodeVNodeDetailRow> {
        // 为了兼容目录法：一行一“伪 vnode”，vi=idx，vid=node_id[idx]
        let mut v = Vec::with_capacity(self.pnode_count());
        for i in 0..self.pnode_count() {
            let (rs, re, _w) = self.node_responsible_interval(i);
            let (t, mn, mx) = self.stats_of_bucket(i);
            v.push((i, self.node_id(i), i, self.node_id(i), rs, re, false, t, mn, mx));
        }
        v
    }

    // --- Downcast 支持 ---
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}
