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

