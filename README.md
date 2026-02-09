# p2pSTA

====================
English
====================

A P2P/Chord experimental framework for spatio-temporal trajectory data. It evaluates data placement strategies on a DHT in terms of load balance, routing cost, and query effectiveness. Pipeline: encode points into SFC keys -> place/partition on the ring -> run window queries and collect hops/hits.

## Highlights

- SFC encoding & window coverage: `z3/h3` (3D) and `z2t/h2t` (2D + time buckets).
- Placement modes: `baseline` (physical nodes), `vnode` (virtual nodes), `snode` (SmartDirect adaptive split).
- Query planning: window -> key ranges -> prefix merge + boundary injection to reduce ranges without misses.
- Outputs: node distribution, responsibility ranges, query hits, routing hops, and summaries.

## Layout

```
p2psta/
├─ implementation/          Rust core and experiment entry
│  ├─ src/                  core implementation
│  ├─ config.yaml           experiment config
│  ├─ py/                   preprocessing & plotting scripts
│  ├─ results/              outputs (generated)
│  └─ geolife_clean.csv     cleaned data (generated)
├─ docs/                    notes / reading
└─ chord/                   early Chord prototypes (Go/Rust)
```

## Quick start

### 1) Prepare data (optional)

Preprocess Geolife data to create `geolife_clean.csv`:

```bash
cd implementation
python py/preprocess.py --root "D:/.../Geolife Trajectories 1.3/Data" --out "geolife_clean.csv"
```

The preprocessing normalizes timezone to UTC, filters by time window, clips to Beijing bounding box, removes speed outliers and jitter, and deduplicates same-second points.

### 2) Run experiments

```bash
cd implementation
cargo run --release -- config.yaml
```

Or via env var:

```bash
set CONFIG=config.yaml
cargo run --release
```

## Configuration (`implementation/config.yaml`)

Key fields (aligned with `src/config.rs`):

- `dataset.lat_range / lon_range / time_range`: global bounds used by SFC quantization.
- `sfc.algorithm`: `z3 | h3 | z2t | h2t`.
- `sfc.time_bucket_s`: time bucket size for z2t/h2t.
- `sfc.max_depth / max_nodes / tail_bits_guard`: window coverage recursion caps and coarse-accept control.
- `data.csv_path`: input CSV; `data.max_ingest` limits ingest for quick tests.
- `network.num_nodes`: number of physical nodes.
- `placement.mode`: `baseline | vnode | snode`.
  - `vnode` also needs `vnodes_per_node`.
  - `snode` can tune `smart.low_ratio / high_ratio` for balancing.
- `experiment.stop_tail_bits`: minimum bucket granularity for boundary injection.
- `experiment.prefix_bits`: prefix merge bits (smaller -> coarser ranges).
- `experiment.queries`: query windows (space + time).

## Input CSV format

Recognized columns (case-insensitive):

- `lat` / `latitude`
- `lon` / `lng` / `longitude`
- `datetime` / `time`
- `user` / `uid` / `user_id` (optional)
- `traj_id` / `trajectory_id` / `tid` (optional)

Time supports ISO 8601 (e.g., `2008-08-01T03:00:00Z`) or epoch seconds.

## Outputs

Generated under the current working directory:

```
results/run_YYYYMMDD_HHMMSS/
├─ timings.txt
├─ ingest_summary.txt
├─ params.txt
├─ node_distribution.csv
├─ node_ranges.csv
├─ node_dump.csv
└─ query_XX_name/
   ├─ window.txt
   ├─ query_results.csv
   ├─ pnode_report.csv
   └─ summary.txt
```

Notes:
- `node_distribution.csv`/`node_ranges.csv`: load & responsibility ranges per node.
- `query_results.csv`: final hits.
- `summary.txt`: hit counts, touched pnodes, total/avg hops.

## Core modules

- `implementation/src/main.rs`: entrypoint.
- `implementation/src/experiment.rs`: workflow orchestration.
- `implementation/src/sfc.rs` + `implementation/src/sfc/*.rs`: SFC encoding & range coverage.
- `implementation/src/network.rs`: Chord-style DHT.
- `implementation/src/node.rs`: storage & bucketed queries.
- `implementation/src/placement.rs`: placement interface.
- `implementation/src/vnode.rs`: vnode mode.
- `implementation/src/smart.rs`: SmartDirect mode.
- `implementation/src/planner.rs`: query planning.
- `implementation/src/query.rs`: directory-style querying + metrics.

## Scripts & visualization

- `implementation/py/preprocess.py`: Geolife cleaning and CSV generation.
- `implementation/py/summary.py`: aggregate summaries and plot metrics.
- `implementation/py/record_per_pnode.py`: pnode load & ECDF plots.
- `implementation/py/*.ipynb`: notebooks for visualization.

## Notes

- The dataset is large (tens of millions of rows). Use `data.max_ingest` for quick tests.
- Output directory is fixed to `results/` (relative to CWD).

====================
Français
====================

Un cadre expérimental P2P/Chord pour des trajectoires spatio-temporelles. Il évalue les stratégies de placement de données sur un DHT en termes d'équilibrage de charge, de coût de routage et d'efficacité des requêtes. Chaîne principale : encoder les points en clés SFC -> placer/partitionner sur l'anneau -> exécuter des requêtes fenêtre et collecter hops/hits.

## Points clés

- Encodage SFC et couverture de fenêtres : `z3/h3` (3D) et `z2t/h2t` (2D + buckets temporels).
- Modes de placement : `baseline` (nœuds physiques), `vnode` (nœuds virtuels), `snode` (SmartDirect adaptatif).
- Planification de requêtes : fenêtre -> intervalles de clés -> fusion par préfixe + injection de bord pour réduire le nombre d'intervalles sans perte.
- Résultats : distribution des nœuds, plages de responsabilité, hits, hops et résumés.

## Arborescence

```
p2psta/
├─ implementation/          cœur Rust et entrée des expériences
│  ├─ src/                  implémentation principale
│  ├─ config.yaml           configuration d'expérience
│  ├─ py/                   prétraitement & scripts de plots
│  ├─ results/              sorties (générées)
│  └─ geolife_clean.csv     données nettoyées (générées)
├─ docs/                    notes / lectures
└─ chord/                   prototypes Chord (Go/Rust)
```

## Démarrage rapide

### 1) Préparer les données (optionnel)

Prétraiter Geolife pour générer `geolife_clean.csv` :

```bash
cd implementation
python py/preprocess.py --root "D:/.../Geolife Trajectories 1.3/Data" --out "geolife_clean.csv"
```

Le prétraitement normalise en UTC, filtre la fenêtre temporelle, découpe sur la zone de Pékin, supprime les outliers de vitesse, élimine les doublons à la seconde.

### 2) Lancer l'expérience

```bash
cd implementation
cargo run --release -- config.yaml
```

Ou via variable d'environnement :

```bash
set CONFIG=config.yaml
cargo run --release
```

## Configuration (`implementation/config.yaml`)

Champs principaux (alignés sur `src/config.rs`) :

- `dataset.lat_range / lon_range / time_range` : bornes globales pour la quantification SFC.
- `sfc.algorithm` : `z3 | h3 | z2t | h2t`.
- `sfc.time_bucket_s` : taille des buckets temporels pour z2t/h2t.
- `sfc.max_depth / max_nodes / tail_bits_guard` : limites de récursion et contrôle d'acceptation grossière.
- `data.csv_path` : CSV d'entrée ; `data.max_ingest` limite l'ingest pour les tests rapides.
- `network.num_nodes` : nombre de nœuds physiques.
- `placement.mode` : `baseline | vnode | snode`.
  - `vnode` nécessite aussi `vnodes_per_node`.
  - `snode` peut ajuster `smart.low_ratio / high_ratio`.
- `experiment.stop_tail_bits` : granularité minimale pour l'injection de bord.
- `experiment.prefix_bits` : bits de fusion par préfixe (plus petit -> intervalles plus grossiers).
- `experiment.queries` : fenêtres de requête (espace + temps).

## Format CSV d'entrée

Colonnes reconnues (insensible à la casse) :

- `lat` / `latitude`
- `lon` / `lng` / `longitude`
- `datetime` / `time`
- `user` / `uid` / `user_id` (optionnel)
- `traj_id` / `trajectory_id` / `tid` (optionnel)

Le temps accepte ISO 8601 (ex. `2008-08-01T03:00:00Z`) ou epoch secondes.

## Sorties

Générées dans le répertoire courant :

```
results/run_YYYYMMDD_HHMMSS/
├─ timings.txt
├─ ingest_summary.txt
├─ params.txt
├─ node_distribution.csv
├─ node_ranges.csv
├─ node_dump.csv
└─ query_XX_name/
   ├─ window.txt
   ├─ query_results.csv
   ├─ pnode_report.csv
   └─ summary.txt
```

Notes :
- `node_distribution.csv`/`node_ranges.csv` : charge et plages de responsabilité.
- `query_results.csv` : points réellement trouvés.
- `summary.txt` : hits, pnodes touchés, hops total/moyen.

## Modules principaux

- `implementation/src/main.rs` : point d'entrée.
- `implementation/src/experiment.rs` : orchestration du workflow.
- `implementation/src/sfc.rs` + `implementation/src/sfc/*.rs` : encodage SFC & couverture.
- `implementation/src/network.rs` : DHT de style Chord.
- `implementation/src/node.rs` : stockage et requêtes par buckets.
- `implementation/src/placement.rs` : interface de placement.
- `implementation/src/vnode.rs` : mode vnode.
- `implementation/src/smart.rs` : mode SmartDirect.
- `implementation/src/planner.rs` : planification des requêtes.
- `implementation/src/query.rs` : exécution des requêtes + métriques.

## Scripts et visualisation

- `implementation/py/preprocess.py` : nettoyage Geolife + CSV.
- `implementation/py/summary.py` : agrégation des summaries et plots.
- `implementation/py/record_per_pnode.py` : charges pnode et ECDF.
- `implementation/py/*.ipynb` : notebooks de visualisation.

## Notes

- Le dataset est volumineux. Utiliser `data.max_ingest` pour des tests rapides.
- Le dossier de sortie est fixé à `results/` (relatif au CWD).

====================
中文说明
====================

面向空间-时间轨迹数据的 P2P/Chord 实验框架，用于评估不同数据放置策略在 DHT 上的负载均衡、路由开销与查询效果。核心流程是：将轨迹点编码为空间填充曲线（SFC）键 -> 在环上放置/分片 -> 执行窗口查询并统计 hops 与命中情况。

## 功能概览

- SFC 编码与窗口覆盖：支持 `z3/h3`（3D）与 `z2t/h2t`（2D+时间桶）。
- 放置策略：`baseline`（物理节点直接分片）、`vnode`（虚拟节点聚合）、`snode`（SmartDirect，自适应划分）。
- 查询规划：窗口 -> 键区间 -> 前缀合并 + 边界注入，降低区间数量且避免漏检。
- 输出结果：节点分布、区间覆盖、查询命中与路由 hops 等指标。

## 目录结构

```
p2psta/
├─ implementation/          Rust 主程序与实验入口
│  ├─ src/                  核心实现
│  ├─ config.yaml           实验配置
│  ├─ py/                   数据预处理与可视化脚本
│  ├─ results/              运行输出（自动生成）
│  └─ geolife_clean.csv     预处理后的数据（可生成）
├─ docs/                    研读/笔记文档
└─ chord/                   早期 Chord 原型（Go/Rust）
```

## 快速开始

### 1) 准备数据（可选）

预处理 Geolife 数据，生成 `geolife_clean.csv`：

```bash
cd implementation
python py/preprocess.py --root "D:/.../Geolife Trajectories 1.3/Data" --out "geolife_clean.csv"
```

默认预处理会做：时区统一（UTC）、时间窗过滤、北京范围裁剪、速度异常/抖动过滤、同秒去重等。

### 2) 运行实验

```bash
cd implementation
cargo run --release -- config.yaml
```

也可以用环境变量指定配置文件：

```bash
set CONFIG=config.yaml
cargo run --release
```

## 配置说明（`implementation/config.yaml`）

核心字段如下（与 `src/config.rs` 对齐）：

- `dataset.lat_range / lon_range / time_range`：全局数据边界（SFC 量化使用）。
- `sfc.algorithm`：`z3 | h3 | z2t | h2t`。
- `sfc.time_bucket_s`：z2t/h2t 的时间桶大小（秒）。
- `sfc.max_depth / max_nodes / tail_bits_guard`：窗口覆盖递归上限与“粗接收”控制。
- `data.csv_path`：输入 CSV 路径；`data.max_ingest` 可限量导入用于小规模测试。
- `network.num_nodes`：物理节点数。
- `placement.mode`：`baseline | vnode | snode`。
  - `vnode` 还需 `vnodes_per_node`。
  - `snode` 可配置 `smart.low_ratio / high_ratio` 控制负载均衡阈值。
- `experiment.stop_tail_bits`：最小桶粒度（用于查询边界注入）。
- `experiment.prefix_bits`：前缀合并位数（越小区间越粗）。
- `experiment.queries`：查询窗口（空间范围 + 时间范围）。

## 输入 CSV 格式

程序会识别以下列名（大小写不敏感）：

- `lat` / `latitude`
- `lon` / `lng` / `longitude`
- `datetime` / `time`
- `user` / `uid` / `user_id`（可选）
- `traj_id` / `trajectory_id` / `tid`（可选）

时间支持 ISO 8601（如 `2008-08-01T03:00:00Z`）或 epoch 秒。

## 输出结构

运行后在当前工作目录下生成：

```
results/run_YYYYMMDD_HHMMSS/
├─ timings.txt
├─ ingest_summary.txt
├─ params.txt
├─ node_distribution.csv
├─ node_ranges.csv
├─ node_dump.csv
└─ query_XX_name/
   ├─ window.txt
   ├─ query_results.csv
   ├─ pnode_report.csv
   └─ summary.txt
```

其中：
- `node_distribution.csv`/`node_ranges.csv`：节点负载与责任区间；
- `query_results.csv`：最终命中点；
- `summary.txt`：命中数、触达节点数、总 hops/平均 hops。

## 代码导览（核心模块）

- `implementation/src/main.rs`：入口，读取配置并启动实验。
- `implementation/src/experiment.rs`：全流程组织（建网、导入、查询、导出）。
- `implementation/src/sfc.rs` + `implementation/src/sfc/*.rs`：SFC 编码与窗口覆盖。
- `implementation/src/network.rs`：Chord 风格 DHT（finger table、路由、范围查询）。
- `implementation/src/node.rs`：节点存储与桶化查询。
- `implementation/src/placement.rs`：放置策略统一接口。
- `implementation/src/vnode.rs`：虚拟节点模式。
- `implementation/src/smart.rs`：SmartDirect 模式（导入后重划分/重建 ring）。
- `implementation/src/planner.rs`：查询规划（区间合并与边界注入）。
- `implementation/src/query.rs`：目录法执行查询 + 指标统计。

## 相关脚本与可视化

- `implementation/py/preprocess.py`：Geolife 清洗与生成 CSV。
- `implementation/py/summary.py`：汇总多个 run 的 summary 指标并绘图。
- `implementation/py/record_per_pnode.py`：按 pnode 负载与 ECDF 分析。
- `implementation/py/*.ipynb`：可视化与对比笔记本。

## 备注

- 数据量非常大（`geolife_clean.csv` 可达千万级），建议先用 `data.max_ingest` 小规模验证。
- 输出目录固定为 `results/`（相对当前工作目录）。
