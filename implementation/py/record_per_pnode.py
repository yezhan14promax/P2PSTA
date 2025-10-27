# -*- coding: utf-8 -*-
"""
make_figs_teacher_style_v4_zeros.py

Fixes:
- Preserve zero-load pnodes by reindexing to expected P from run name.
- Show zeros explicitly in ECDF (log-x) with a '0' tick at the left edge.
- Show zeros in per-pnode (log-y) by pinning them to a floor line with a '0' y-tick.

Keeps:
- All figure styles from previous version (v3), including metrics sorting/grouping.
"""

from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib import cm, colors
from matplotlib.patches import Patch

# ================== CONFIG ==================
BASE_DIR = Path("D:/implementation/p2psta/implementation/results")
RUNS = [
    "op128","op1024","op8192",
    "p128v10","p128v100","p128v1000","p128v10000",
    "p1024v10","p1024v100","p1024v1000","p1024v10000",
    "p8192v10","p8192v100","p8192v1000",
    "snode128","snode1024","snode8192",
]
CSV_NAME = "node_distribution.csv"
OUT_DIR = BASE_DIR / "figs" / "teacher_style"; OUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNKSIZE = 2_000_000
DPI = 400
LINEWIDTH = 0.6
ALPHA = 0.85
MAX_POINTS = 12000

# ECDF / per-pnode zeros handling
ZERO_FLOOR_FACTOR = 0.08   # per-pnode(log-y): floor = min_positive * ZERO_FLOOR_FACTOR
ZERO_X_FRACTION   = 0.6    # ECDF(log-x): zero tick placed at (min_positive * ZERO_X_FRACTION)

# Query windows
QUERY_DIRS = [
    "query_00_forbidden_city",
    "query_01_Guomao_CBD",
    "query_02_Changping_District",
]
DENSITY_LABEL = {
    "query_00_forbidden_city":     "High-density window",
    "query_01_Guomao_CBD":         "Medium-density window",
    "query_02_Changping_District": "Low-density window",
}
DENSITY_ORDER = ["High-density window", "Medium-density window", "Low-density window"]
DENSITY_COLORS = {
    "High-density window":   "#1f77b4",
    "Medium-density window": "#ff7f0e",
    "Low-density window":    "#2ca02c",
}

# ================== helpers ==================
def parse_pcount(name: str) -> int | None:
    m = re.match(r"^p(\d+)", name)
    if m: return int(m.group(1))
    m = re.match(r"^op(\d+)$", name)
    if m: return int(m.group(1))
    m = re.match(r"^snode(\d+)$", name)
    if m: return int(m.group(1))
    return None

def parse_scheme_key(name: str):
    # for earlier metrics sorting (op -> snode -> p...v...)
    if name.startswith("op"):
        p = int(re.findall(r"\d+", name)[0]); return (0, p, -1)
    if name.startswith("snode"):
        p = int(re.findall(r"\d+", name)[0]); return (1, p, -1)
    m = re.match(r"^p(\d+)v(\d+)$", name)
    if m:
        return (2, int(m.group(1)), int(m.group(2)))
    return (9, 1_000_000, 1_000_000)

def sort_runs_scheme_order_grouped(run_names):
    def normalize_p(p): return 8192 if p in (8192, 8196) else p
    buckets = {}
    for name in run_names:
        p = parse_pcount(name)
        p = normalize_p(p) if p is not None else 10**9
        buckets.setdefault(p, []).append(name)
    ordered_p_list = [128, 1024, 8192] + sorted([p for p in buckets.keys()
                                                 if p not in (128,1024,8192)])
    out = []
    for p in ordered_p_list:
        if p not in buckets: continue
        group = buckets[p]
        out += sorted([r for r in group if r.startswith("op")], key=lambda r: parse_pcount(r) or 0)
        out += sorted([r for r in group if r.startswith("snode")],    key=lambda r: parse_pcount(r) or 0)
        pv = []
        for r in group:
            m = re.match(rf"^p{p}v(\d+)$", r)
            if m: pv.append((int(m.group(1)), r))
        out += [r for _, r in sorted(pv, key=lambda t: t[0])]
        leftovers = [r for r in group if r not in out]
        out += sorted(leftovers)
    return out

def load_pnode_vector(csv_path: Path, expected_pcount: int | None) -> pd.DataFrame:
    """
    Read node_distribution.csv and aggregate per pnode_idx -> total_count,
    then reindex to [0..expected_pcount-1] and fill missing with 0.
    """
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)
    agg = {}
    for chunk in pd.read_csv(
        csv_path, usecols=["pnode_idx","total_count"],
        dtype=str, keep_default_na=False, chunksize=CHUNKSIZE
    ):
        p = pd.to_numeric(chunk["pnode_idx"], errors="coerce")
        v = pd.to_numeric(chunk["total_count"], errors="coerce").fillna(0)
        mask = ~p.isna()
        if not mask.any(): 
            continue
        sub = pd.DataFrame({"pnode_idx": p[mask].astype(int), "total_count": v[mask]})
        g = sub.groupby("pnode_idx")["total_count"].sum()
        for idx, val in g.items():
            agg[idx] = agg.get(idx, 0.0) + float(val)

    # build df
    if agg:
        df = pd.DataFrame(sorted(agg.items(), key=lambda kv: kv[0]),
                          columns=["pnode_idx","total_count"])
    else:
        df = pd.DataFrame(columns=["pnode_idx","total_count"])

    # reindex to include zero-load pnodes
    if expected_pcount is not None and expected_pcount > 0:
        full_index = pd.Index(range(expected_pcount), name="pnode_idx")
        df = df.set_index("pnode_idx").reindex(full_index, fill_value=0).reset_index()
    else:
        # still ensure int index order
        if not df.empty:
            df = df.sort_values("pnode_idx").reset_index(drop=True)
    return df

def build_rank_xy(pnode_df: pd.DataFrame):
    y = pnode_df["total_count"].to_numpy(dtype=float)
    x = np.arange(len(y))
    return x, y

def downsample_xy(x: np.ndarray, y: np.ndarray, max_points: int):
    n = len(x)
    if n <= max_points: return x, y
    step = int(np.ceil(n / max_points))
    return x[::step], y[::step]

def human_fmt(val, pos):
    v = float(val)
    for unit in ["","K","M","B","T"]:
        if abs(v) < 1000:
            return f"{v:.0f}{unit}"
        v /= 1000.0
    return f"{v:.0f}P"

def ecdf_xy(arr: np.ndarray):
    x = np.sort(arr)
    n = len(x)
    if n == 0:
        return np.array([]), np.array([])
    y = np.arange(1, n+1) / n
    return x, y

# ================== per-pnode & CDF plotting (with zeros visible) ==================
def plot_per_pnode_linear(pcount_label: str, lines, savepath: Path):
    plt.figure(figsize=(14, 6), dpi=DPI)
    for label, x, y in lines:
        x1, y1 = downsample_xy(x, y, MAX_POINTS)
        plt.plot(x1, y1, label=label, lw=LINEWIDTH, alpha=ALPHA)
    plt.title(f"Records per pnode (linear) — P={pcount_label}")
    plt.xlabel("pnode rank (sorted by pnode_idx)")
    plt.ylabel("records per pnode")
    plt.gca().yaxis.set_major_formatter(FuncFormatter(human_fmt))
    plt.grid(True, alpha=0.3)
    leg = plt.legend(title="Scheme", frameon=True)
    leg.get_frame().set_alpha(0.85); leg.get_frame().set_facecolor("white")
    plt.tight_layout(); plt.savefig(savepath, dpi=DPI); plt.close()

def plot_per_pnode_logy(pcount_label: str, lines, savepath: Path):
    # Compute global min positive for consistent zero floor
    min_pos_global = None
    for _, _x, y in lines:
        pos = y[y > 0]
        if len(pos) > 0:
            m = float(pos.min())
            min_pos_global = m if min_pos_global is None else min(min_pos_global, m)
    if min_pos_global is None:
        min_pos_global = 1.0

    zero_floor = max(min_pos_global * ZERO_FLOOR_FACTOR, 1e-12)

    plt.figure(figsize=(14, 6), dpi=DPI)
    for label, x, y in lines:
        y = y.astype(float)
        # positives
        mask_pos = y > 0
        xp, yp = downsample_xy(x[mask_pos], y[mask_pos], MAX_POINTS)
        plt.plot(xp, yp, label=label, lw=LINEWIDTH, alpha=ALPHA)
        # zeros pinned to floor
        mask_zero = ~mask_pos
        if mask_zero.any():
            xz = x[mask_zero]
            # downsample zeros too if needed
            xz = xz[::max(1, len(xz)//5000)]
            plt.scatter(xz, np.full_like(xz, zero_floor, dtype=float),
                        s=3, alpha=0.35, linewidths=0, label=None)

    plt.yscale("log")
    # add a '0' y tick at the floor
    yticks = plt.gca().get_yticks()
    yticks = np.append([zero_floor], yticks[yticks > zero_floor])
    plt.gca().set_yticks(yticks)
    plt.gca().set_yticklabels(["0"] + [f"{t:.0e}" for t in yticks[1:]])

    plt.ylim(bottom=zero_floor*0.9)
    plt.title(f"Records per pnode (log scale; zeros shown at floor) — P={pcount_label}")
    plt.xlabel("pnode rank (sorted by pnode_idx)")
    plt.ylabel("records per pnode (log scale, 0 at floor)")
    plt.grid(True, which="both", alpha=0.3)
    leg = plt.legend(title="Scheme", frameon=True)
    leg.get_frame().set_alpha(0.85); leg.get_frame().set_facecolor("white")
    plt.tight_layout(); plt.savefig(savepath, dpi=DPI); plt.close()

def plot_cdf_logx(pcount_label: str, lines, savepath: Path):
    # Find global min positive for consistent '0' x placement
    min_pos_global = None
    for _, _x, y in lines:
        pos = y[y > 0]
        if len(pos) > 0:
            m = float(pos.min())
            min_pos_global = m if min_pos_global is None else min(min_pos_global, m)
    if min_pos_global is None:
        min_pos_global = 1.0
    x_zero = min_pos_global * ZERO_X_FRACTION

    plt.figure(figsize=(12, 6), dpi=DPI)
    ax = plt.gca()

    # draw ECDF of positives and zero-mass as a vertical segment at x_zero
    for label, _x, y in lines:
        y = y.astype(float)
        pos = y[y > 0]
        zero_ratio = (y <= 0).mean() * 100.0

        if len(pos) > 0:
            xs, ec = ecdf_xy(pos)
            ax.step(xs, ec*100.0, where="post", label=label, lw=1.2)

        if zero_ratio > 0:
            # vertical segment at x_zero from 0 to zero_ratio
            ax.vlines(x_zero, 0, zero_ratio, colors=ax._get_lines.get_next_color(),
                      linestyles="-", alpha=0.75, lw=2)
            # small dot at top of the zero mass
            ax.scatter([x_zero], [zero_ratio], s=18, alpha=0.9)

    ax.set_xscale("log")
    # add custom x tick for '0'
    xticks = list(ax.get_xticks())
    xticks = [t for t in xticks if t > x_zero]  # keep positive ticks to the right
    ax.set_xticks([x_zero] + xticks)
    ax.set_xticklabels(["0"] + [f"{int(t):,}" for t in xticks])

    ax.set_title(f"ECDF of per-pnode records (log-x; zeros included) — P={pcount_label}")
    ax.set_xlabel("records per pnode (log scale, leftmost is 0)")
    ax.set_ylabel("Percentage of nodes with this load (%)")
    ax.grid(True, which="both", alpha=0.3)
    leg = ax.legend(title="Scheme", frameon=True)
    leg.get_frame().set_alpha(0.85); leg.get_frame().set_facecolor("white")
    plt.tight_layout(); plt.savefig(savepath, dpi=DPI); plt.close()

# ================== METRICS (same as v3) ==================
def parse_summary_text(txt_path: Path):
    if not txt_path.exists(): return None
    data = {}
    with txt_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if ":" not in line: continue
            k, v = line.split(":", 1)
            k = k.strip()
            try:
                data[k] = float(v.strip())
            except:
                pass
    if not data: return None
    return {
        "Total precise hits": data.get("Total precise hits", np.nan),
        "Hit pnodes": data.get("Hit pnodes", np.nan),
        "Total route hops": data.get("Total route hops", np.nan),
        "Avg route hops": data.get("Avg route hops", np.nan),
    }

def collect_metrics_all_runs():
    metrics_by_run = {}
    valid_runs = []
    for run in RUNS:
        run_dir = BASE_DIR / run
        items = {}
        for qdir in QUERY_DIRS:
            smry = parse_summary_text(run_dir / qdir / "summary.txt")
            if smry is not None:
                items[DENSITY_LABEL[qdir]] = smry
        if items:
            metrics_by_run[run] = items
            valid_runs.append(run)
    return metrics_by_run, valid_runs

def plot_hits_vs_hops_mirrored(metrics_by_run, run_names, savepath: Path):
    runs_sorted = sort_runs_scheme_order_grouped(run_names)
    dens = [d for d in DENSITY_ORDER if any(d in metrics_by_run[r] for r in runs_sorted)]
    if not dens: return
    hits = np.zeros((len(dens), len(runs_sorted))) * np.nan
    hops = np.zeros_like(hits)
    for j, r in enumerate(runs_sorted):
        for i, d in enumerate(dens):
            m = metrics_by_run[r].get(d)
            if m:
                hits[i, j] = m["Hit pnodes"]
                hops[i, j] = m["Total route hops"]
    max_hits = np.nanmax(hits)
    max_hops = np.nanmax(hops)
    scale = 1.0 if (not np.isfinite(max_hits) or not np.isfinite(max_hops) or max_hops == 0) else (max_hits / max_hops)

    fig, ax = plt.subplots(figsize=(max(12, 0.6*len(runs_sorted)+8), 10), dpi=DPI)

    y = np.arange(len(runs_sorted))
    group_h = 0.7
    bar_h = group_h / (len(dens) + 0.2)

    # 用来收集图例句柄
    legend_handles, legend_labels = [], []

    for i, d in enumerate(dens):
        color = DENSITY_COLORS.get(d, None)
        offset = (i - (len(dens)-1)/2) * bar_h

        # 左侧：Hit pnodes（负轴）
        ax.barh(y + offset, -hits[i, :], height=bar_h*0.9,
                color=color, alpha=0.95)

        # 右侧：Total route hops（正轴，缩放）
        ax.barh(y + offset, hops[i, :] * scale, height=bar_h*0.9,
                color=color, alpha=0.45, edgecolor="none")

        # 为图例添加一个代理方块（每个密度一个）
        legend_handles.append(Patch(facecolor=color, edgecolor="none"))
        legend_labels.append(d)

    # 中线
    ax.axvline(0, color="#333", lw=1.0)

    # 轴标签等保持不变……
    ax.set_yticks(y); ax.set_yticklabels(runs_sorted, fontsize=10)
    ax.set_xlabel("Hit pnodes (left; count)")
    xmax = np.nanmax([np.abs(ax.get_xlim()[0]), np.abs(ax.get_xlim()[1])])
    xmax = max(xmax, max_hits) * 1.15
    ax.set_xlim(-xmax, xmax)

    def fwd(x): return np.where(x>=0, x/scale, x) if scale!=0 else x
    def inv(x): return np.where(x>=0, x*scale, x) if scale!=0 else x
    secax = ax.secondary_xaxis('top', functions=(fwd, inv))
    secax.set_xlabel("Total route hops (right; true scale)")

    ax.grid(axis="x", alpha=0.25)

    # ✅ 完整图例（上移一点避免靠底）
    leg = ax.legend(legend_handles, legend_labels,
                    title="Density window",
                    loc="lower right", bbox_to_anchor=(1.0, 0.26),  # 调高 y 值可继续上移
                    frameon=True)
    leg.get_frame().set_alpha(0.9)
    leg.get_frame().set_facecolor("white")

    plt.tight_layout(rect=[0, 0.04, 1, 0.97])
    fig.suptitle("Hit pnodes (left) vs Total route hops (right) — mirrored grouped bars",
                y=0.995, fontsize=16)
    fig.savefig(savepath, dpi=DPI)  # 如有裁切可加 bbox_inches="tight"
    plt.close(fig)
    print(f"[Saved] {savepath}")

def plot_avg_hops_grouped(metrics_by_run, run_names, savepath: Path):
    runs_sorted = sort_runs_scheme_order_grouped(run_names)
    dens = [d for d in DENSITY_ORDER if any(d in metrics_by_run[r] for r in runs_sorted)]
    if not dens: return
    x = np.arange(len(runs_sorted)); group_w = 0.75; bar_w = group_w / len(dens)

    fig, ax = plt.subplots(figsize=(max(12, 0.6*len(runs_sorted)+8), 6.5), dpi=DPI)
    for i, d in enumerate(dens):
        yvals = []
        for r in runs_sorted:
            m = metrics_by_run[r].get(d)
            yvals.append(np.nan if m is None else m["Avg route hops"])
        yvals = np.array(yvals, dtype=float)
        ax.bar(x + (i - (len(dens)-1)/2)*bar_w, yvals, width=bar_w*0.95,
               color=DENSITY_COLORS.get(d, None), alpha=0.9, label=d)
        for xi, val in zip(x, yvals):
            if np.isfinite(val):
                ax.text(xi + (i - (len(dens)-1)/2)*bar_w, val, f"{val:.2f}",
                        ha="center", va="bottom", fontsize=9)

    ax.set_xticks(x); ax.set_xticklabels(runs_sorted, rotation=0, fontsize=10)
    ax.set_ylabel("Average route hops")
    ax.set_title("Average route hops by scheme (grouped by density)")
    ax.grid(axis="y", alpha=0.25)
    leg = ax.legend(title="Density window", frameon=True)
    leg.get_frame().set_alpha(0.9); leg.get_frame().set_facecolor("white")

    plt.tight_layout()
    fig.savefig(savepath, dpi=DPI); plt.close(fig)
    print(f"[Saved] {savepath}")

# ================== main ==================
def main():
    # —— 1) 读取每个 run 的 pnode 向量，按方案名推断期望 P 并补零 —— 
    valid = []
    run_info = {}
    for name in RUNS:
        csv_path = BASE_DIR / name / CSV_NAME
        if not csv_path.exists():
            print(f"[Skip] {name}: {csv_path} not found")
            continue

        expected_p = parse_pcount(name)  # op8192 / snode1024 / p128v100 -> 8192/1024/128
        df = load_pnode_vector(csv_path, expected_pcount=expected_p)  # <<< 会补齐 0 负载 pnode
        if df.empty:
            print(f"[Warn] {name} empty after aggregation")
            continue

        x, y = build_rank_xy(df)
        run_info[name] = {
            "pcount": expected_p if expected_p else int(df["pnode_idx"].nunique()),
            "x": x, "y": y
        }
        valid.append(name)

    if not valid:
        print("No valid runs. Nothing to plot.")
        return

    # —— 2) 按 pnode 数分组并输出三张图（linear / log-y / ECDF）——
    # 组内排序：op{P} -> snode{P} -> p{P}v{...}（v 升序）
    groups = {}
    for name, info in run_info.items():
        groups.setdefault(info["pcount"], []).append(name)

    for pcount in sorted(groups.keys()):
        # 组内排序
        names = sort_runs_scheme_order_grouped(groups[pcount])

        # 组装折线数据（label, x, y）
        lines = [(name, run_info[name]["x"], run_info[name]["y"]) for name in names]

        # 输出三张图（文件名含 P）
        out_linear = OUT_DIR / f"records_per_pnode_LINEAR_P{pcount}.png"
        out_logy   = OUT_DIR / f"records_per_pnode_LOG_P{pcount}.png"
        out_ecdf   = OUT_DIR / f"ecdf_records_LOGX_P{pcount}.png"

        plot_per_pnode_linear(str(pcount), lines, out_linear)
        plot_per_pnode_logy (str(pcount), lines, out_logy)
        plot_cdf_logx       (str(pcount), lines, out_ecdf)

    # —— 3) 指标图：保持现有逻辑（镜像 hits vs total hops + avg hops 分组柱）——
    metrics_by_run, runs_with_metrics = collect_metrics_all_runs()
    if runs_with_metrics:
        plot_hits_vs_hops_mirrored(
            metrics_by_run, runs_with_metrics,
            OUT_DIR / "metrics_hits_vs_total_hops__mirrored.png"
        )
        plot_avg_hops_grouped(
            metrics_by_run, runs_with_metrics,
            OUT_DIR / "metrics_avg_route_hops__grouped.png"
        )
    else:
        print("[Metrics] No summary.txt found in any run.")

    print(f"Done. Figures in: {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
