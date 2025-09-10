mod config;
mod network;
mod node;
mod sfc;
mod experiment;
mod placement;
mod planner;
mod query;

use std::env;

fn main() {
    // 读取配置路径：优先 CLI 第一个参数，其次环境变量 CONFIG，最后默认 "config.yaml"
    let cfg_path = env::args().nth(1)
        .or_else(|| env::var("CONFIG").ok())
        .unwrap_or_else(|| "config.yaml".to_string());

    println!("CWD = {}", std::env::current_dir().unwrap().display());
    println!("Using config: {}", cfg_path);

    let cfg = config::Config::from_yaml(&cfg_path);
    experiment::run_experiment(&cfg);
}
