// src/smart.rs
use std::cmp::{max, min};
use std::any::Any;

use crate::node::Segment;
use crate::network::Network;
use crate::placement::{Placement, NodeRangeRow, PNodeVNodeDetailRow};

#[derive(Debug)]
pub struct SmartDirect {
    // Router: Chord on the pNode ring (used only for routing / hop counting)
    inner: Network,

    // pNode count and bit width
    pnodes: usize,
    m: usize,

    // Ingest stage: buffer everything in memory, then partition during finalize
    buffer: Vec<Segment>,

    // After finalize:
    buckets: Vec<Vec<Segment>>,     // one bucket per pnode after contiguous packing
    ranges:  Vec<(u64, u64)>,       // [start_key, end_key] for each pnode (inclusive)
    node_ids: Vec<u64>,             // node_id for each pnode (= end_key)
    finalized: bool,
}

impl SmartDirect {
    pub fn new(num_pnodes: usize, m: usize, tail_bits: u8) -> Self {
        let p = num_pnodes.max(1);
        // inner is used for Chord routing; finalize() rewrites node_id values and rebuilds fingers
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

    /// Two-threshold contiguous packing (L/H ratios come from config)
    pub fn finalize(&mut self, low_ratio: f64, high_ratio: f64) {
        if self.finalized { return; }

        // 1) Stable sort by ascending SFC key
        self.buffer.sort_by_key(|s| s.sfc_key);

        let total = self.buffer.len();
        if total == 0 {
            // Empty input: keep node_id/range at 0 and rebuild fingers to avoid crashes
            self.rebuild_ring_and_fingers();
            self.finalized = true;
            return;
        }

        let p = self.pnodes;
        let mu = (total as f64) / (p as f64);
        let low = (mu * low_ratio).floor() as usize;
        let high = (mu * high_ratio).ceil() as usize;
        let low = low.max(1); // safety floor

        // 2) Iterate over encoded runs (contiguous records sharing the same sfc_key)
        let mut buckets: Vec<Vec<Segment>> = vec![Vec::new(); p];
        let mut end_keys: Vec<u64> = vec![0; p];

        let first_key = self.buffer[0].sfc_key;
        let mut start_keys: Vec<u64> = vec![first_key; p];

        let mut pi: usize = 0;     // current pnode
        let mut acc: usize = 0;    // records already packed into the current pnode

        let mut i = 0;
        while i < total {
            let key = self.buffer[i].sfc_key;
            // Find the end of the current key run [i, j)
            let mut j = i + 1;
            while j < total && self.buffer[j].sfc_key == key { j += 1; }
            let mut remain = j - i;

            // A very large run may need to be split across multiple pnodes
            let mut cursor = i;
            while remain > 0 {
                // If all pnodes are already used, push the remainder into the last one
                if pi >= p {
                    let last = p - 1;
                    buckets[last].extend_from_slice(&self.buffer[cursor..j]);
                    end_keys[last] = key;
                    acc = 0;
                    remain = 0;
                    break;
                }

                // If the current pnode is still below low, prefer taking the whole run but never exceed high
                if acc < low {
                    let cap = high.saturating_sub(acc); // remaining capacity before hitting high
                    if cap == 0 {
                        // Already at high, move to the next pnode
                        pi += 1;
                        if pi < p { start_keys[pi] = end_keys[pi - 1]; }
                        acc = 0;
                        continue;
                    }
                    let take = min(cap, remain);
                    if take == 0 {
                        // This path should be unreachable in practice
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

                    // If this run was truncated because acc reached high, move to the next pnode
                    if remain > 0 && acc >= high {
                        pi += 1;
                        if pi < p { start_keys[pi] = end_keys[pi - 1]; }
                        acc = 0;
                    }
                } else {
                    // Once acc >= low, prefer consuming the entire encoded run
                    let will = acc + remain;
                    if will <= high {
                        // Consume the whole run, then move to the next pnode
                        buckets[pi].extend_from_slice(&self.buffer[cursor..j]);
                        end_keys[pi] = key;
                        acc = will;
                        cursor = j;
                        remain = 0;

                        pi += 1;
                        if pi < p { start_keys[pi] = end_keys[pi - 1]; }
                        acc = 0;
                    } else {
                        // Exceeds high: cut immediately within this key run
                        let cap = high.saturating_sub(acc);
                        let take = cap.max(1); // take at least one record to avoid an infinite loop
                        let take = min(take, remain);
                        buckets[pi].extend_from_slice(&self.buffer[cursor..(cursor + take)]);
                        end_keys[pi] = key;
                        acc += take;
                        cursor += take;
                        remain -= take;

                        // Move to the next pnode and leave the rest of this key for it
                        pi += 1;
                        if pi < p { start_keys[pi] = end_keys[pi - 1]; }
                        acc = 0;
                    }
                }
            }

            i = j;
        }

        // If some pnodes remain unused, extend ranges using the start=previous-end convention
        for k in 0..p {
            if buckets[k].is_empty() {
                // Empty bucket: reuse the previous end as both start and end
                if k > 0 { start_keys[k] = end_keys[k - 1]; }
                end_keys[k] = start_keys[k];
            }
        }

        // Persist the computed state into the struct fields
        self.buckets = buckets;
        self.ranges = (0..p)
            .map(|k| (start_keys[k], end_keys[k]))
            .collect();
        self.node_ids = end_keys.clone();

        // 3) Rewrite inner node_id values and rebuild the finger table (Chord on the pNode ring)
        self.rebuild_ring_and_fingers();

        // The ingest buffer can be released to save memory
        self.buffer.clear();
        self.finalized = true;
    }

    fn rebuild_ring_and_fingers(&mut self) {
        // Set inner node_id to end_key so that (prev+1..=id) matches the assigned interval exactly
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
    // --- Basic information ---
    fn node_id(&self, idx: usize) -> u64 {
        self.node_ids.get(idx).copied().unwrap_or(0)
    }

    fn node_responsible_interval(&self, idx: usize) -> (u64, u64, bool) {
        if idx >= self.ranges.len() { return (0, 0, false); }
        let (s, e) = self.ranges[idx];
        (s, e, false) // non-wrapping
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

    // --- Query path: used for hop statistics and diagnostics (real fetching still happens once per pnode) ---
    fn query_range_with_nodes(
        &self,
        entry_node: usize,
        key_range: (u64, u64),
    ) -> (Vec<(usize, &Segment)>, usize, Vec<usize>) {
        let (s, e) = key_range;
        let n = self.pnode_count();
        if n == 0 { return (Vec::new(), 0, Vec::new()); }

        // Route to the successor of s via inner (Chord on the pNode ring) to get hop count
        let (mut idx, hops) = self.inner.find_successor_from(entry_node % n, s);

        // Collect touched pnodes and emit (idx, &Segment) pairs for diagnostics only
        let mut touched: Vec<usize> = Vec::new();
        let mut out: Vec<(usize, &Segment)> = Vec::new();
        let mut visits = 0usize;

        loop {
            if !touched.contains(&idx) { touched.push(idx); }
            let (rs, re, _w) = self.node_responsible_interval(idx);

            // Intersect with the current pnode interval
            let (ia, ib);
            if s <= e {
                // Non-wrapping query
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
                // Wrapping query: [s..MAX] union [0..e]
                // First segment
                let a1 = max(s, rs);
                let b1 = min(u64::MAX, re);
                if a1 <= b1 {
                    for seg in self.buckets[idx].iter() {
                        if seg.sfc_key >= a1 && seg.sfc_key <= b1 {
                            out.push((idx, seg));
                        }
                    }
                }
                // Second segment
                let a2 = max(0, rs);
                let b2 = min(e, re);
                if a2 <= b2 {
                    for seg in self.buckets[idx].iter() {
                        if seg.sfc_key >= a2 && seg.sfc_key <= b2 {
                            out.push((idx, seg));
                        }
                    }
                }
                // Early stop once the second segment already covers e
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

    // --- Export helpers ---
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
        // For compatibility with the directory-based planner: one pseudo-vnode per row, vi=idx and vid=node_id[idx]
        let mut v = Vec::with_capacity(self.pnode_count());
        for i in 0..self.pnode_count() {
            let (rs, re, _w) = self.node_responsible_interval(i);
            let (t, mn, mx) = self.stats_of_bucket(i);
            v.push((i, self.node_id(i), i, self.node_id(i), rs, re, false, t, mn, mx));
        }
        v
    }

    // --- Downcast support ---
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}
