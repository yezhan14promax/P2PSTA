// src/config.rs
use serde::Deserialize;
use std::fs::File;
use std::io::Read;

#[derive(Debug, Deserialize)]
pub struct Config {
    // sfc.rs 需要
    pub dataset: DatasetConfig,
    pub sfc: SfcConfig,

    // 其余模块使用
    pub data: DataConfig,
    pub output: OutputConfig,
    pub network: NetworkConfig,
    pub experiment: ExperimentConfig,
}

impl Config {
    pub fn from_yaml(path: &str) -> Self {
        let mut f = File::open(path).expect("open config yaml failed");
        let mut s = String::new();
        f.read_to_string(&mut s).expect("read config yaml failed");
        serde_yaml::from_str::<Config>(&s).expect("parse config yaml failed")
    }
}

/* ---------- Blocks ---------- */

#[derive(Debug, Deserialize)]
pub struct DataConfig {
    pub csv_path: String,
    pub max_ingest: Option<usize>,   // 0 或 None 表示不限制
}

#[derive(Debug, Deserialize)]
pub struct OutputConfig {
    pub result_dir: String,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub num_nodes: usize,
}

/// 供 sfc.rs 使用的“数据全域范围”
#[derive(Debug, Deserialize)]
pub struct DatasetConfig {
    // YAML 里仍写成 [min, max]；如果不写该字段，sfc.rs 会用它自己的 unwrap_or 默认值
    pub lat_range: (f64, f64),
    pub lon_range: (f64, f64),
    pub time_range: Option<(u64, u64)>,  // <-- 这里从 (u64,u64) 改为 Option<(u64,u64)>
}

/// 供 sfc.rs 使用的 SFC 控制参数
#[derive(Debug, Deserialize)]
pub struct SfcConfig {
    pub algorithm: String,     // "z3" | "h3" | "z2t" | "h2t"
    pub center_lat: f64,
    pub x_precision_m: f64,
    pub y_precision_m: f64,
    pub t_precision_s: u64,

    // sfc.rs 中使用到的可选参数
    pub time_bucket_s: Option<u64>,
    pub max_ranges: Option<usize>,
}

/* ---------- Experiment (查询/方案切换/指标开关) ---------- */

#[derive(Debug, Deserialize)]
pub struct ExperimentConfig {
    pub print_first: Option<usize>,

    // 仍保留一份算法与合并控制，方便 window.txt 写入展示
    pub algorithm: String,
    pub center_lat: f64,
    pub x_precision_m: f64,
    pub y_precision_m: f64,
    pub t_precision_s: u64,
    pub stop_tail_bits: u8,
    pub merge_gap_keys: usize,
    pub max_ranges: Option<usize>,

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
    pub precise_hits: Option<bool>,       // 预留
}

#[derive(Debug, Deserialize, Clone)]
pub struct QueryWindow {
    pub name: Option<String>,
    pub lat_min: f64,
    pub lon_min: f64,
    pub lat_max: f64,
    pub lon_max: f64,
    pub t_start: String,  // ISO 或 epoch 秒
    pub t_end: String,
}
