use crate::{network::Network, node::Segment, hilbert::encode_hilbert, dataset};
use rand::Rng;
use std::time::Instant;

pub fn run_experiment(net: &mut Network, entry_node: usize, num_inserts: usize, num_queries: usize) {
    let mut rng = rand::rng();

    // 插入阶段：加载 Geolife 数据集
    // Insertion phase: Load Geolife dataset
    println!("Loading Geolife dataset...");
    let dataset = dataset::load_geolife_dataset(
        "geolife/Geolife Trajectories 1.3/Data"
    );

    // 只取前 num_inserts 条数据
    // Only take the first num_inserts records
    let points: Vec<_> = dataset.into_iter().take(num_inserts).collect();

    // 记录 key 范围
    // Record the key range
    let mut min_key = u64::MAX;
    let mut max_key = u64::MIN;

    let start = Instant::now();
    for (i, (lat, lon, time)) in points.into_iter().enumerate() {
        let key = encode_hilbert(lat, lon, time);

        // 更新 key 范围
        // Update key range
        if key < min_key {
            min_key = key;
        }
        if key > max_key {
            max_key = key;
        }

        let seg = Segment {
            traj_id: i as u64,
            segment_id: 0,
            hilbert_key: key,
            payload: format!("point({}, {}, {})", lat, lon, time),
        };
        net.insert(entry_node, seg);
    }

    // 避免溢出：只有当 max_key > min_key 时才计算跨度
    // Avoid overflow: Only calculate span when max_key > min_key
    let key_span = if max_key > min_key { max_key - min_key } else { 0 };

    println!(
        "Inserted {} records in {:?}, key range [{}, {}], span {}",
        num_inserts,
        start.elapsed(),
        min_key,
        max_key,
        key_span
    );

    // 打印节点分布情况
    // Print node data distribution
    println!("--- Node data distribution ---");
    for (i, node) in net.nodes.iter().enumerate() {
        let (count, min, max) = node.stats();
        match (min, max) {
            (Some(min), Some(max)) => {
                println!("Node {} (ID={}): {} records, key range [{}, {}]",
                         i, node.node_id, count, min, max);
            }
            _ => {
                println!("Node {} (ID={}): empty", i, node.node_id);
            }
        }
    }

    // 查询阶段
    // Query phase
    println!("--- Query---");
    let query_window = if key_span > 0 { key_span / 100 } else { 1 };

    let start = Instant::now();
    let mut total_hits = 0;
    let mut total_visited = 0;

    for _ in 0..num_queries {
        // 在真实 key 范围内随机选择查询区间
        // Randomly select query interval within the real key range
        let k1 = rng.random_range(min_key..max_key);
        let k2 = k1.saturating_add(query_window); // 使用 saturating_add 避免溢出 // Use saturating_add to avoid overflow
        let (results, visited) = net.query_range(entry_node, (k1, k2));

        total_hits += results.len();
        total_visited += visited;

        println!(
            "Query range ({} ~ {}), matched {} records, visited {} nodes",
            k1, k2, results.len(), visited
        );
    }

    println!(
        "{} queries in {:?}, avg matched {:.2}, avg visited nodes {:.2}",
        num_queries,
        start.elapsed(),
        total_hits as f64 / num_queries as f64,
        total_visited as f64 / num_queries as f64
    );
}
