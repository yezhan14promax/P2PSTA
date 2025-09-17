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
    // Read configuration path: prioritize the first CLI argument, then the CONFIG environment variable, and finally default to "config.yaml"
    let cfg_path = env::args().nth(1)
        .or_else(|| env::var("CONFIG").ok())
        .unwrap_or_else(|| "config.yaml".to_string());

    println!("CWD = {}", std::env::current_dir().unwrap().display());
    println!("Using config: {}", cfg_path);
    let cfg = config::Config::from_yaml(&cfg_path);
    if cfg.experiment.debug.unwrap_or(false) {
    println!(">>> [debug] YAML debug mode is ON");
}   else {
    println!(">>> [debug] YAML debug mode is OFF");
}
   
    experiment::run_experiment(&cfg);

}
