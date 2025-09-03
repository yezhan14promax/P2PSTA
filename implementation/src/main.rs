mod node;
mod network;
mod hilbert;
mod experiment;
mod dataset;

fn main() {
    // 创建一个 x个节点的 Chord 环，id 空间大小为 2^m
    // Create a Chord ring with x nodes, and an ID space of size 2^m
    let mut net = network::Network::new(512, 64);

    // 从节点 0 作为入口，插入 1000000 条，查询 10 次
    // Use node 0 as the entry point, insert 1,000,000 records, and perform 10 queries
    experiment::run_experiment(&mut net, 0, 1_000_000, 10);
}
