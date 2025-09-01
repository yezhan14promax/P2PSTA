mod node;
mod network;
mod hilbert;
mod experiment;

fn main() {
    // 创建一个 64 节点的 Chord 环，ID 空间大小 2^10 = 1024
    let mut net = network::Network::new(64, 10);

    // 从节点 0 作为入口，插入 1000000 条，查询 10 次
    experiment::run_experiment(&mut net, 0, 1_000_000, 10);
}
