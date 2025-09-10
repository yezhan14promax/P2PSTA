use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Segment {
    pub traj_id: u64,
    pub segment_id: u32,
    pub hilbert_key: u64,
    pub payload: String,
    pub lat: f64,           
    pub lon: f64,           
    pub ts:  u64,           
}

impl Segment {
    pub fn new(
        traj_id: u64,
        segment_id: u32,
        hilbert_key: u64,
        lat: f64,
        lon: f64,
        ts: u64,
        payload: impl Into<String>,
    ) -> Self {
        Self {
            traj_id,
            segment_id,
            hilbert_key,
            lat,
            lon,
            ts,
            payload: payload.into(),
        }
    }
}

#[derive(Debug)]
pub struct Node {
    pub node_id: u64,               // 节点 ID（环上位置）
    pub m: usize,                   // 键位宽
    pub tail_bits: u8,              // 桶尾位数（stop_tail_bits）
    pub finger: Vec<usize>,         // finger table（由 Network 填充）
    pub predecessor: Option<usize>, // 前驱索引（可选）

    // 存储：按“桶起点键”聚合
    pub storage: BTreeMap<u64, Vec<Segment>>,
}

impl Node {
    pub fn new(node_id: u64, m: usize, tail_bits: u8) -> Self {
        Self {
            node_id,
            m,
            tail_bits,
            finger: Vec::new(),
            predecessor: None,
            storage: BTreeMap::new(),
        }
    }

    #[inline]
    fn bucket_span(&self) -> u64 {
        if self.tail_bits >= 63 { u64::MAX } else { 1u64 << self.tail_bits }
    }

    #[inline]
    fn bucket_start(&self, key: u64) -> u64 {
        let span = self.bucket_span();
        if span == u64::MAX { 0 } else { key & !(span - 1) }
    }

    /// 插入：把段落写到其桶起点键下
    pub fn insert(&mut self, seg: Segment) {
        let b = self.bucket_start(seg.hilbert_key);
        self.storage.entry(b).or_default().push(seg);
    }

    /// 本地范围查询：[s,e] 与各桶区间 [K, K+span-1] 相交即命中
    /// 返回 (命中引用, 本地遍历步数=1)
    pub fn query_range(&self, range: (u64, u64)) -> (Vec<&Segment>, usize) {
        let (s, e) = range;
        let span = self.bucket_span();
        let mut out: Vec<&Segment> = Vec::new();

        // 朴素遍历（BTreeMap 可改进为按 key 上界/下界范围，但这里先保证正确性）
        for (&k, vec_seg) in self.storage.iter() {
            let k_end = if span == u64::MAX { u64::MAX } else { k.saturating_add(span - 1) };
            // 判断相交：K <= e && k_end >= s
            if k <= e && k_end >= s {
                for seg in vec_seg {
                    out.push(seg);
                }
            }
        }
        (out, 1)
    }

    /// 统计：总条数
    pub fn data_len(&self) -> usize {
        self.storage.values().map(|v| v.len()).sum()
    }

    /// 统计：键范围（桶起点 min/max）
    pub fn stats_range(&self) -> (usize, Option<u64>, Option<u64>) {
        let total = self.data_len();
        if let (Some((&mn, _)), Some((&mx, _))) = (self.storage.first_key_value(), self.storage.last_key_value()) {
            (total, Some(mn), Some(mx))
        } else {
            (total, None, None)
        }
    }
}
