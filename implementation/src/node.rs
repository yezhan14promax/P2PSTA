use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Segment {
    pub user: String,         // User ID
    pub traj_id: u64,
    pub segment_id: u32,
    pub sfc_key: u64,
    pub payload: String,
    pub lat: f64,           // Latitude
    pub lon: f64,           // Longitude
    pub ts:  u64,           // Timestamp
}

impl Segment {
    pub fn new(
        user: String,
        traj_id: u64,
        segment_id: u32,
        sfc_key: u64,
        lat: f64,
        lon: f64,
        ts: u64,
        payload: impl Into<String>,
    ) -> Self {
        Self {
            user:user.to_string(),
            traj_id,
            segment_id,
            sfc_key,
            payload: payload.into(),
            lat,
            lon,
            ts,
        }
    }
}

/// One physical node on the ring
#[derive(Debug)]
pub struct Node {
    pub node_id: u64,               // Position on the ring (MSB-first 0..2^m-1)
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
        let b = self.bucket_start(seg.sfc_key);
        self.storage.entry(b).or_default().push(seg);
    }

    /// Local range query: returns segments whose bucket range [K, K+span-1] intersects with [s, e]
    /// Returns (vector of hit references, local traversal step count = 1)
    pub fn query_range(&self, range: (u64, u64)) -> (Vec<&Segment>, usize) {
        let (s, e) = range;
        let span = self.bucket_span();
        if self.storage.is_empty() {
            return (Vec::new(), 1);
        }
        // locate first bucket whose end >= s  => bucket_start <= s <= bucket_end
        // i.e., bucket_start in [s-span+1, e]
        let start_key = s.saturating_sub(span - 1);
        let mut out: Vec<&Segment> = Vec::new();
        for (&b, v) in self.storage.range(start_key..) {
            let b_end = if span == u64::MAX { u64::MAX } else { b.saturating_add(span - 1) };
            if b > e { break; }
            if !(e < b || b_end < s) {
                // intersects
                for seg in v { out.push(seg); }
            }
        }
        (out, 1)
    }

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

    /// Return total number of stored segments
    pub fn store_len(&self) -> usize {
        self.data_len()
    }

    /// Iterate over all stored segments
    pub fn iter_segments(&self) -> impl Iterator<Item=&Segment> {
        self.storage.values().flat_map(|v| v.iter())
    }
}
