use crate::node::{Node, Segment};
use rand::Rng;

pub struct Network {
    pub nodes: Vec<Node>,
    pub m: usize, // ID 空间大小 log2(2^m)
    pub max_id: u64,
}

impl Network {
    pub fn new(num_nodes: usize, m: usize) -> Self {
        let mut rng = rand::rng();
        let max_id = if m >= 64 {
            u64::MAX
        } else {
            1u64 << m
        };


        // 随机生成节点
        let mut ids: Vec<u64> = (0..num_nodes)
            .map(|_| rng.random_range(0..max_id))
            .collect();
        ids.sort();

        let mut nodes = Vec::new();
        for id in ids {
            nodes.push(Node::new(id, m));
        }

        let mut net = Network { nodes, m, max_id };
        net.build_finger_tables();
        net
    }

    /// 建立 finger table 和 successor / predecessor
    fn build_finger_tables(&mut self) {
        let n = self.nodes.len();
        for i in 0..n {
            let id = self.nodes[i].node_id;
            let mut finger = Vec::new();

            for k in 0..self.m {
                let offset = if k >= 63 { u64::MAX } else { 1u64 << k };
                let start = id.wrapping_add(offset) % self.max_id;
                let succ = self.find_successor_id(start);
                let idx = self.nodes.iter().position(|n| n.node_id == succ).unwrap();
                finger.push(idx);
            }

            self.nodes[i].finger = finger;
            self.nodes[i].successor = Some((i + 1) % n);
            self.nodes[i].predecessor = Some((i + n - 1) % n);
        }
    }

    /// 查找 key 的后继节点 ID
    fn find_successor_id(&self, key: u64) -> u64 {
        for node in &self.nodes {
            if node.node_id >= key {
                return node.node_id;
            }
        }
        self.nodes[0].node_id
    }

    /// 从入口节点查找 key，返回节点索引
    pub fn lookup(&self, start_idx: usize, key: u64) -> usize {
        let mut idx = start_idx;

        loop {
            let node_id = self.nodes[idx].node_id;
            let succ_idx = self.nodes[idx].successor.unwrap();
            let succ_id = self.nodes[succ_idx].node_id;

            // 如果 key 在 [node_id, succ_id] 范围内，就找到了
            if in_range(key, node_id, succ_id, self.max_id) {
                return succ_idx;
            } else {
                // 向 finger 表里最接近 key 的节点跳
                let mut next_idx = idx;
                for &f in self.nodes[idx].finger.iter().rev() {
                    let finger_id = self.nodes[f].node_id;
                    if in_range(finger_id, node_id, key, self.max_id) {
                        next_idx = f;
                        break;
                    }
                }
                if next_idx == idx {
                    return succ_idx;
                }
                idx = next_idx;
            }
        }
    }

    pub fn insert(&mut self, entry_node: usize, seg: Segment) {
        let idx = self.lookup(entry_node, seg.hilbert_key);
        self.nodes[idx].insert(seg);
    }

    pub fn query_range(&self, entry_node: usize, key_range: (u64, u64)) -> (Vec<&Segment>, usize) {
        let (low, high) = key_range;

        // 找到范围边界
        let start_idx = self.lookup(entry_node, low);
        let end_idx = self.lookup(entry_node, high);

        let mut results = Vec::new();
        let mut visited = 0;

        let mut idx = start_idx;
        loop {
            visited += 1;
            results.extend(self.nodes[idx].query(key_range));

            if idx == end_idx {
                break;
            }
            idx = self.nodes[idx].successor.unwrap();
        }

        (results, visited)
    }
}


impl Network {
    pub fn query(&self, entry_node: usize, key_range: (u64, u64)) -> (Vec<&Segment>, usize) {
        self.query_range(entry_node, key_range)
    }
}


/// 判断 key 是否在 (start, end] 区间（环形 ID 空间）
fn in_range(key: u64, start: u64, end: u64, max_id: u64) -> bool {
    if start < end {
        key > start && key <= end
    } else {
        key > start || key <= end
    }
}
