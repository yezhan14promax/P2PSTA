use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Segment {
    pub traj_id: u64,
    pub segment_id: u32,
    pub hilbert_key: u64,
    pub payload: String,
}

#[derive(Debug)]
pub struct Node {
    pub node_id: u64,                    // 节点 ID
    pub m: usize,                        // ID 空间大小 (2^m)
    pub finger: Vec<usize>,              // finger table 存节点索引 (不是 node_id，方便在 Network 中跳转)
    pub predecessor: Option<usize>,      // 前驱节点索引
    pub successor: Option<usize>,        // 后继节点索引
    pub storage: HashMap<u64, Vec<Segment>>, // 存储 key -> 数据段
}

impl Node {
    pub fn new(node_id: u64, m: usize) -> Self {
        Node {
            node_id,
            m,
            finger: vec![],
            predecessor: None,
            successor: None,
            storage: HashMap::new(),
        }
    }

    pub fn insert(&mut self, seg: Segment) {
        self.storage.entry(seg.hilbert_key)
            .or_insert_with(Vec::new)
            .push(seg);
    }

    pub fn query(&self, key_range: (u64, u64)) -> Vec<&Segment> {
        let mut results = Vec::new();
        for (key, segs) in &self.storage {
            if *key >= key_range.0 && *key <= key_range.1 {
                results.extend(segs.iter());
            }
        }
        results
    }

    pub fn stats(&self) -> (usize, Option<u64>, Option<u64>) {
        let mut count = 0;
        let mut min_key = u64::MAX;
        let mut max_key = u64::MIN;

        for (key, segs) in &self.storage {
            count += segs.len();
            if *key < min_key {
                min_key = *key;
            }
            if *key > max_key {
                max_key = *key;
            }
        }

        if count == 0 {
            (0, None, None)
        } else {
            (count, Some(min_key), Some(max_key))
        }
    }
}
