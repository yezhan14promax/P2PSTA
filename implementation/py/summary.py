# -*- coding: utf-8 -*-
"""
plot_summary_metrics.py

Summary script:
- read summary.txt from the three query directories under each scheme directory
- metrics: Total precise hits, Hit pnodes, Total route hops, Avg route hops
- draw one comparison line chart per metric (x-axis = scheme, three lines = three queries)

Dependencies: matplotlib, pandas, numpy
"""

from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ==================== Configuration ====================
BASE_DIR = Path("D:/implementation/p2psta/implementation/results")
RUNS_ORDER = [
    "op128","snode128","p128v10","p128v100","p128v1000","p128v10000",
    "op1024","snode1024","p1024v10","p1024v100","p1024v1000","p1024v10000",
    "op8192","snode8192","p8192v10","p8192v100","p8192v1000",
]
QUERIES = [
    ("forbidden_city", "query_00_forbidden_city"),
    ("Guomao_CBD", "query_01_Guomao_CBD"),
    ("Changping_District", "query_02_Changping_District"),
]

OUT_DIR = Path("/implementation/p2psta/implementation/results/figs/summary"); OUT_DIR.mkdir(parents=True, exist_ok=True)
FIGSIZE = (22, 12)   # one figure holding four subplots
DPI = 400
LW = 1.2
MS = 4
ALPHA = 0.9

# ========= Parse summary.txt =========
METRIC_KEYS = {
    "Total precise hits": "Number of data hits",
    "Hit pnodes": "Number of pnode hits",
    "Total route hops": "total_hops",
    "Avg route hops": "avg_hops",
}

def parse_summary_file(path: Path) -> dict:
    vals = {}
    if not path.exists():
        return vals
    text = path.read_text(encoding="utf-8", errors="ignore")
    for line in text.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        k, v = line.split(":", 1)
        k, v = k.strip(), v.strip()
        if k in METRIC_KEYS:
            # Extract numeric values (integer / float)
            m = re.search(r"-?\d+(?:\.\d+)?", v)
            if not m:
                continue
            num = float(m.group(0))
            if k != "Avg route hops":
                num = int(round(num))
            vals[METRIC_KEYS[k]] = num
    return vals

def collect_long_df() -> pd.DataFrame:
    rows = []
    for run in RUNS_ORDER:
        for qname, qdir in QUERIES:
            sfile = BASE_DIR / run / qdir / "summary.txt"
            vals = parse_summary_file(sfile)
            if not vals:
                vals = {k: np.nan for k in METRIC_KEYS.values()}
            rows.append({"run": run, "query": qname, **vals})
    return pd.DataFrame(rows)

# ========= Draw the 2x2 overview =========
def plot_overview(df: pd.DataFrame, savepath: Path):
    metrics = [
        ("Number of data hits", "Number of data hits"),
        ("Number of pnode hits",   "Number of pnode hits"),
        ("total_hops",   "Total route hops"),
        ("avg_hops",     "Avg route hops"),
    ]

    fig, axes = plt.subplots(2, 2, figsize=FIGSIZE, dpi=DPI)
    axes = axes.ravel()
    x = np.arange(len(RUNS_ORDER))

    # Collect one set of handles/labels for a shared legend
    handles_labels_collected = None

    for ax, (col, title) in zip(axes, metrics):
        for qname, _qdir in QUERIES:
            sub = df[df["query"] == qname].set_index("run").reindex(RUNS_ORDER)
            y = sub[col].to_numpy(dtype=float)
            h, = ax.plot(x, y, marker="o", lw=LW, ms=MS, alpha=ALPHA, label=qname)
        ax.set_title(title)
        ax.set_xlabel("scheme")
        ax.set_ylabel(title)
        ax.set_xticks(x)
        ax.set_xticklabels(RUNS_ORDER, rotation=45, ha="right")
        ax.grid(True, alpha=0.3)

        # Collect legend entries once
        if handles_labels_collected is None:
            handles_labels_collected = ax.get_legend_handles_labels()

    # Shared legend at the top
    if handles_labels_collected is not None:
        handles, labels = handles_labels_collected
        fig.legend(handles, labels, loc="upper center", ncol=len(QUERIES),
                   bbox_to_anchor=(0.5, 1.02), frameon=True)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(savepath, dpi=DPI)
    # For vector output, use: fig.savefig(savepath.with_suffix(".svg"))
    plt.close(fig)
    print(f"[Saved] {savepath}")

def main():
    df = collect_long_df()
    plot_overview(df, OUT_DIR / "summary_metrics_overview.png")

if __name__ == "__main__":
    main()
