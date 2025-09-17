// src/config.rs
use serde::Deserialize;
use std::fs::File;
use std::io::Read;

#[derive(Debug, Deserialize)]
pub struct Config {
    // Required for sfc.rs
    pub dataset: DatasetConfig,
    pub sfc: SfcConfig,

    // Used by other modules
    pub data: DataConfig,
    pub output: OutputConfig,
    pub network: NetworkConfig,
    pub experiment: ExperimentConfig,
}

impl Config {
    pub fn from_yaml(path: &str) -> Self {
        let mut f = File::open(path).expect("failed to open config yaml");
        let mut s = String::new();
        f.read_to_string(&mut s)
            .expect("failed to read config yaml");
        serde_yaml::from_str::<Config>(&s).expect("failed to parse config yaml")
    }
}

/* ---------- Blocks ---------- */

#[derive(Debug, Deserialize)]
pub struct DataConfig {
    pub csv_path: String,
    pub max_ingest: Option<usize>,   // 0 or None means no limitation
}

#[derive(Debug, Deserialize)]
pub struct OutputConfig {
    pub result_dir: String,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub num_nodes: usize,
}

/// Global dataset boundaries used by sfc.rs
#[derive(Debug, Deserialize)]
pub struct DatasetConfig {
    // In YAML, this field is still written as [min, max]; if not provided, sfc.rs uses its own unwrap_or default value
    pub lat_range: (f64, f64),
    pub lon_range: (f64, f64),
    pub time_range: Option<(u64, u64)>,  // <-- Changed from (u64, u64) to Option<(u64, u64)>
}

/// SFC control parameters used by sfc.rs
#[derive(Debug, Deserialize)]
pub struct SfcConfig {
    pub algorithm: String,     // "z3" | "h3" | "z2t" | "h2t"
    pub center_lat: f64,
    pub x_precision_m: f64,
    pub y_precision_m: f64,
    pub t_precision_s: u64,

    // Optional parameters used by sfc.rs
    pub time_bucket_s: Option<u64>,
    pub max_ranges: Option<usize>,
}

/* ---------- Experiment (Query, Plan Switching, Metric Toggles) ---------- */

#[derive(Debug, Deserialize)]
pub struct ExperimentConfig {
    pub print_first: Option<usize>,

    // Retains algorithm and merge controls for display in window.txt
    pub algorithm: String,
    pub center_lat: f64,
    pub x_precision_m: f64,
    pub y_precision_m: f64,
    pub t_precision_s: u64,
    pub stop_tail_bits: u8,
    pub merge_gap_keys: usize,
    pub max_ranges: Option<usize>,
    pub debug: Option<bool>,
    pub placement: PlacementConfig,
    pub metrics: MetricsConfig,
    pub queries: Vec<QueryWindow>,
}

#[derive(Debug, Deserialize)]
pub struct PlacementConfig {
    pub mode: String,               // "baseline" | "vnode" | "smart_vnode"
    pub per_node: Option<usize>,    // for vnode
    pub vnode_bits: Option<usize>,  // for vnode
    pub smart: Option<SmartConfig>, // for smart vnode
}

#[derive(Debug, Deserialize)]
pub struct SmartConfig {
    pub hot_prefix_top_k: Option<f64>,
    pub split_factor_bits: Option<u8>,
    pub rebalance_cooldown_s: Option<u64>,
    pub co_placement_jaccard: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct MetricsConfig {
    pub compute_node_cover: Option<bool>, // default true
    pub save_with_nodes: Option<bool>,    // default true
    pub precise_hits: Option<bool>,       // reserved
}

#[derive(Debug, Deserialize, Clone)]
pub struct QueryWindow {
    pub name: Option<String>,
    pub lat_min: f64,
    pub lon_min: f64,
    pub lat_max: f64,
    pub lon_max: f64,
    pub t_start: String,  // ISO format or epoch seconds
    pub t_end: String,
}
