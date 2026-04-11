use crate::node::{Node, Segment};

/// Chord-style DHT network
#[derive(Debug)]
pub struct Network {
    pub nodes: Vec<Node>,
    pub m: usize,                 // Ring bit width
    pub node_ids: Vec<u64>,       // Sorted node IDs (positions on ring)
    pub fingers: Vec<Vec<usize>>, // fingers[i][k] = successor index of node i after 2^k hops
    pub total_inserts: usize,
}

impl Network {
    /// Build a network with evenly distributed nodes on ring
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

    /// Rebuild finger table
    pub fn rebuild_fingers(&mut self) {
        let n = self.nodes.len().max(1);
        self.fingers = vec![Vec::new(); n];

        for i in 0..n {
            let mut tbl = Vec::new();
            // Prevent shift UB when m>63
            let max_k = self.m.min(63);
            for k in 0..max_k {
                let target = self.nodes[i].node_id.wrapping_add(1u64 << k);
                let succ = self.successor_index(target);
                tbl.push(succ);
            }
            self.fingers[i] = tbl;
        }
    }

    /// Binary search in node_ids to find successor index of key
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
        // On ring, interval (a, b]
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

    /// Find successor of key starting from entry node, return (successor_index, hop_count)
    pub fn find_successor_from(&self, mut idx: usize, key: u64) -> (usize, usize) {
        let mut hops = 0usize;

        // Check if self and successor can directly cover key
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

    /// Insert by routing from entry_node to the node responsible for the key; returns hop count
    pub fn insert(&mut self, entry_node: usize, seg: Segment) -> usize {
        let (idx, hops) = self.find_successor_from(entry_node % self.nodes.len().max(1), seg.sfc_key);
        self.nodes[idx].insert(seg);
        self.total_inserts += 1;
        hops
    }

    /// Compute a node's responsible interval on the ring: (prev(node).id + 1 ..= node.id), returning (start, end, wrapped)
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

    /// Public accessor for the responsible interval, used by statistics and validation
    pub fn node_responsible_interval(&self, idx: usize) -> (u64, u64, bool) {
        self.node_interval(idx)
    }

    /// Convenience helper returning only (start, end)
    pub fn node_key_range(&self, idx: usize) -> (u64, u64) {
        let (s, e, _) = self.node_interval(idx);
        (s, e)
    }

    /// Return the node ID (its ring position) for a given node
    pub fn node_id_of(&self, idx: usize) -> u64 {
        self.nodes[idx].node_id
    }

    /// Basic range query without node context; returns (matched segments, hops)
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
                // Split wrapping queries into two segments
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
            if touched > n + 1 { break; } // safety guard
            let succ = self.fingers[idx][0];
            idx = succ;
            hops += 1;

            if idx == start_idx { break; }
            let (_, end_cur, wrapped_cur) = self.node_interval(idx);
            if !wrapped_cur && end_cur > e { break; }
        }

        (hits, hops.max(1))
    }

    /// Range query with node context: returns ((pnode_idx, &Segment) list, hops, touched node set)
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

        // Count only finger-routing steps as hops
        let (mut idx, route_hops) = self.find_successor_from(entry_node % n, s);
        let mut hits: Vec<(usize, &Segment)> = Vec::new();
        let mut touched_nodes: Vec<usize> = Vec::new();
        let mut visits = 0usize;

        let query_wrapped = s > e;
        let mut crossed_zero = false;

        loop {
            let (start, end, wrapped_node) = self.node_interval(idx);

            // Intersect with the query interval and scan local data
            if !query_wrapped {
                // Non-wrapping query: intersect with the current node, which may itself wrap or not
                if !wrapped_node {
                    let sub_s = s.max(start);
                    let sub_e = e.min(end);
                    if sub_s <= sub_e {
                        let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                        for seg in local { hits.push((idx, seg)); }
                    }
                    // Key early-stop condition: once this node's end covers e, stop
                    if end >= e {
                        if touched_nodes.last().copied() != Some(idx) {
                            touched_nodes.push(idx);
                        }
                        break;
                    }
                } else {
                    // If the node itself wraps, intersect [s, e] with its two segments
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
                    // A wrapping node may still not cover e; advance to the next non-wrapping node and stop only when end >= e
                }
            } else {
                // Wrapping query: cover [s..MAX] union [0..e]
                // First segment: [s..MAX]
                if !crossed_zero {
                    if !wrapped_node {
                        let sub_s = s.max(start);
                        let sub_e = u64::MAX.min(end);
                        if sub_s <= sub_e {
                            let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                            for seg in local { hits.push((idx, seg)); }
                        }
                        // If the current node ends at u64::MAX, the next hop must cross zero
                        if end == u64::MAX { crossed_zero = true; }
                    } else {
                        // A wrapping node also covers MAX, so the next hop crosses zero
                        let sub_s = s.max(start);
                        let sub_e = u64::MAX;
                        if sub_s <= sub_e {
                            let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                            for seg in local { hits.push((idx, seg)); }
                        }
                        crossed_zero = true;
                    }
                } else {
                    // Second segment: [0..e]
                    if !wrapped_node {
                        let sub_s = 0u64.max(start);
                        let sub_e = e.min(end);
                        if sub_s <= sub_e {
                            let (local, _) = self.nodes[idx].query_range((sub_s, sub_e));
                            for seg in local { hits.push((idx, seg)); }
                        }
                        // Once end >= e, the second segment is fully covered, so stop early
                        if end >= e {
                            if touched_nodes.last().copied() != Some(idx) {
                                touched_nodes.push(idx);
                            }
                            break;
                        }
                    } else {
                        // Wrapping node: intersect its [0..end] segment with [0..e]
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

            // Advance to the successor linearly for coverage only; no extra hops are counted
            visits += 1;
            if visits > n + 1 { break; } // safety guard
            let succ = self.fingers[idx][0];
            if succ == idx { break; } // degenerate case
            idx = succ;
        }

        (hits, route_hops.max(1), touched_nodes)
    }



    /// Export node responsible intervals plus stored-data ranges for verification
    /// (pnode_idx, node_id, resp_start, resp_end, wrapped, stored_total, stored_min, stored_max)
    pub fn export_node_ranges(&self) -> Vec<(usize, u64, u64, u64, bool, usize, Option<u64>, Option<u64>)> {
        let mut rows = Vec::new();
        for (i, node) in self.nodes.iter().enumerate() {
            let (rs, re, wrapped) = self.node_interval(i);
            let (total, mn, mx) = node.stats_range();
            rows.push((i, node.node_id, rs, re, wrapped, total, mn, mx));
        }
        rows
    }

    /// Export the segments actually stored on a node (iterator)
    pub fn export_node_data(&self, idx: usize) -> impl Iterator<Item = &Segment> {
        self.nodes[idx].iter_segments()
    }

    /// Structured node distribution data (replacement for the old node_distribution output)
    pub fn node_distribution_rows(&self) -> Vec<(usize, u64, usize, Option<u64>, Option<u64>)> {
        let mut out = Vec::new();
        for (i, node) in self.nodes.iter().enumerate() {
            let (total, mn, mx) = node.stats_range();
            out.push((i, node.node_id, total, mn, mx));
        }
        out
    }

    #[inline] pub fn node_count(&self) -> usize { self.nodes.len() }
    #[inline] pub fn node_stats_range(&self, idx: usize) -> (usize, Option<u64>, Option<u64>) { self.nodes[idx].stats_range() }
    #[inline] pub fn node_id(&self, idx: usize) -> u64 { self.node_id_of(idx) }

    /// Print node distribution for debugging
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
