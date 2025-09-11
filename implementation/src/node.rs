use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Segment {
    pub traj_id: u64,
    pub segment_id: u32,
    pub hilbert_key: u64,
    pub payload: String,
    pub lat: f64,           // Latitude
    pub lon: f64,           // Longitude
    pub ts:  u64,           // Timestamp
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
    pub node_id: u64,               // Node ID (position on the ring)
    pub m: usize,                   // Key width
    pub tail_bits: u8,              // Bucket tail bits (stop_tail_bits)
    pub finger: Vec<usize>,         // Finger table (populated by Network)
    pub predecessor: Option<usize>, // Predecessor index (optional)

    // Storage: aggregated by bucket starting key
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

    /// Insert: writes a segment under its bucket starting key
    pub fn insert(&mut self, seg: Segment) {
        let b = self.bucket_start(seg.hilbert_key);
        self.storage.entry(b).or_default().push(seg);
    }

    /// Local range query: returns segments whose bucket range [K, K+span-1] intersects with [s, e]
    /// Returns (vector of hit references, local traversal step count = 1)
    pub fn query_range(&self, range: (u64, u64)) -> (Vec<&Segment>, usize) {
        let (s, e) = range;
        let span = self.bucket_span();
        let mut out: Vec<&Segment> = Vec::new();

        // Naive traversal (BTreeMap can be improved using range queries by key, but correctness is prioritized here)
        for (&k, vec_seg) in self.storage.iter() {
            let k_end = if span == u64::MAX { u64::MAX } else { k.saturating_add(span - 1) };
            // Check for intersection: K <= e && k_end >= s
            if k <= e && k_end >= s {
                for seg in vec_seg {
                    out.push(seg);
                }
            }
        }
        (out, 1)
    }

    /// Statistics: total count of segments
    pub fn data_len(&self) -> usize {
        self.storage.values().map(|v| v.len()).sum()
    }

    /// Statistics: key range (minimum/maximum bucket starting keys)
    pub fn stats_range(&self) -> (usize, Option<u64>, Option<u64>) {
        let total = self.data_len();
        if let (Some((&mn, _)), Some((&mx, _))) = (self.storage.first_key_value(), self.storage.last_key_value()) {
            (total, Some(mn), Some(mx))
        } else {
            (total, None, None)
        }
    }
}
