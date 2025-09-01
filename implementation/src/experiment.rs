use crate::{network::Network, node::Segment, hilbert::encode_hilbert};
use rand::Rng;
use std::time::Instant;

pub fn run_experiment(net: &mut Network, entry_node: usize, num_inserts: usize, num_queries: usize) {
    let mut rng = rand::rng();

    // 记录插入数据的 key 范围
    let mut min_key = u64::MAX;
    let mut max_key = u64::MIN;

    // 插入
    let start = Instant::now();
    for i in 0..num_inserts {
        let lat = rng.random_range(-90.0..90.0);
        let lon = rng.random_range(-180.0..180.0);
        let time = rng.random_range(0..1_000_000);
        let key = encode_hilbert(lat, lon, time);

        // 更新 key 范围
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
    let key_span = max_key - min_key;
    println!(
        "插入 {} 条数据耗时: {:?}, key 范围 [{}, {}], 总跨度 {}",
        num_inserts,
        start.elapsed(),
        min_key,
        max_key,
        key_span
    );
    
    println!("--- 节点数据分布 ---");
    for (i, node) in net.nodes.iter().enumerate() {
        let (count, min_key, max_key) = node.stats();
        match (min_key, max_key) {
            (Some(min), Some(max)) => {
                println!("节点 {} (ID={}): {} 条数据, key范围 [{}, {}]", 
                        i, node.node_id, count, min, max);
            }
            _ => {
                println!("节点 {} (ID={}): 空", i, node.node_id);
            }
        }
    }

    // 动态查询窗口 = 总跨度的 1%
    let query_window = std::cmp::max(1, key_span / 100);

    // 查询
    let start = Instant::now();
    let mut total_hits = 0;
    let mut total_visited = 0;

    for _ in 0..num_queries {
        let k1 = rng.random_range(min_key..max_key);
        let k2 = k1.saturating_add(query_window);
        let (results, visited) = net.query_range(entry_node, (k1, k2));

        total_hits += results.len();
        total_visited += visited;

        println!(
            "查询范围({}~{}), 命中 {} 条, 访问 {} 个节点",
            k1, k2, results.len(), visited
        );
    }

    println!(
        "{} 次查询耗时: {:?}, 平均命中 {:.2}, 平均访问节点 {:.2}",
        num_queries,
        start.elapsed(),
        total_hits as f64 / num_queries as f64,
        total_visited as f64 / num_queries as f64
    );
}
