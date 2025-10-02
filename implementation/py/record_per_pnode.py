# -*- coding: utf-8 -*-
"""
plot_pnode_records_v3.py

变化：
1) 仅绘制“平滑曲线”（rolling median），不再叠加原始折线；
2) 线性坐标图对 y==0 的点改为 NaN，从而消除底部那几条彩色直线；
   对数坐标图直接丢弃 y<=0 数据；
3) 图例只展示方案名（颜色=方案），放在图外右侧；
4) STD 柱状图在柱顶标注“精准数值”（千位分隔，保留两位小数）。
"""

from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# =============== 配置 ===============
BASE_DIR = Path("D:/implementation/p2psta/implementation/results")
RUNS = [
    "baseline128","baseline1024","baseline8192",
    "p128v10","p128v100","p128v1000","p128v10000",
    "p1024v10","p1024v100","p1024v1000","p1024v10000",
    "p8192v10","p8192v100","p8192v1000",
    "snode128","snode1024","snode8192",
]
CSV_NAME = "node_distribution.csv"
OUT_DIR = Path("D:/implementation/p2psta/implementation/results/figs/pnode"); OUT_DIR.mkdir(parents=True, exist_ok=True)

# 大文件友好
CHUNKSIZE = 2_000_000

# 折线观感
FIGSIZE = (22, 7)     # 单张（三组）图尺寸
DPI = 400
LINEWIDTH = 0.5
ALPHA = 0.8
LEGEND_FONTSIZE = 10
LEGEND_OUTSIDE = False 
MAX_POINTS = 12000     # 轻度降采样

# 总览图观感（三行两列）
OVERVIEW_FIGSIZE = (22, 18)  # 一页容纳 3 组 × {linear, log}
OVERVIEW_DPI = 400

# ===================== 工具函数 =====================
def parse_pcount(name: str) -> int | None:
    m = re.match(r"^p(\d+)", name)
    if m: return int(m.group(1))
    m = re.match(r"^baseline(\d+)$", name)
    if m: return int(m.group(1))
    m = re.match(r"^snode(\d+)$", name)
    if m: return int(m.group(1))
    return None

def load_pnode_vector(csv_path: Path) -> pd.DataFrame:
    """读取 node_distribution.csv 的 pnode_idx,total_count 两列，按 pnode_idx 汇总"""
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
        if not mask.any(): continue
        sub = pd.DataFrame({"pnode_idx": p[mask].astype(int), "total_count": v[mask]})
        g = sub.groupby("pnode_idx")["total_count"].sum()
        for idx, val in g.items():
            agg[idx] = agg.get(idx, 0.0) + float(val)
    if not agg:
        return pd.DataFrame(columns=["pnode_idx","total_count"])
    df = pd.DataFrame(sorted(agg.items(), key=lambda kv: kv[0]),
                      columns=["pnode_idx","total_count"]).reset_index(drop=True)
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
    """K/M/B 简写"""
    v = float(val)
    for unit in ["","K","M","B","T"]:
        if abs(v) < 1000:
            return f"{v:.0f}{unit}"
        v /= 1000.0
    return f"{v:.0f}P"

def add_legend(ax, loc_inside="upper right"):
    """添加半透明白底图例。LEGEND_OUTSIDE 控制是否放图外右侧。"""
    if LEGEND_OUTSIDE:
        leg = ax.legend(loc="center left", bbox_to_anchor=(1.01, 0.5),
                        frameon=True, fontsize=LEGEND_FONTSIZE)
    else:
        leg = ax.legend(loc=loc_inside, frameon=True, fontsize=LEGEND_FONTSIZE)
    leg.get_frame().set_alpha(0.85)
    leg.get_frame().set_facecolor("white")

def ecdf_xy(arr: np.ndarray):
    """返回 ECDF 的 (x,y)：x 升序，y 为累计比例"""
    x = np.sort(arr)
    n = len(x)
    if n == 0:
        return np.array([]), np.array([])
    y = np.arange(1, n+1) / n
    return x, y

# ===================== 绘图函数 =====================
def plot_group_records(group_name: str, lines, savepath: Path):
    """每个 pnode 数量画一张图：左线性，右 log1p；经典图例。"""
    if not lines: return
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=FIGSIZE, dpi=DPI)

    # 线性坐标：total_count
    for label, x, y in lines:
        x1, y1 = downsample_xy(x, y, MAX_POINTS)
        ax1.plot(x1, y1, label=label, lw=LINEWIDTH, alpha=ALPHA)
    ax1.set_title(f"Records per pnode (linear) — P={group_name}")
    ax1.set_xlabel("pnode rank (sorted by pnode_idx)")
    ax1.set_ylabel("total_count")
    ax1.yaxis.set_major_formatter(FuncFormatter(human_fmt))
    ax1.grid(True, alpha=0.3)
    add_legend(ax1, loc_inside="upper right")

    # log1p 坐标：log10(total_count + 1)
    for label, x, y in lines:
        y_log1p = np.log10(y.astype(float) + 1.0)
        x2, y2 = downsample_xy(x, y_log1p, MAX_POINTS)
        ax2.plot(x2, y2, label=label, lw=LINEWIDTH, alpha=ALPHA)
    ax2.set_title(f"Records per pnode (log1p) — P={group_name}")
    ax2.set_xlabel("pnode rank (sorted by pnode_idx)")
    ax2.set_ylabel("log10")
    ax2.grid(True, alpha=0.3)
    add_legend(ax2, loc_inside="upper right")

    if LEGEND_OUTSIDE:
        plt.tight_layout(rect=[0,0,0.86,1])
    else:
        plt.tight_layout()

    fig.savefig(savepath, dpi=DPI)
    plt.close(fig)
    print(f"[Saved] {savepath}")

def plot_overview(groups_ordered, lines_by_group, savepath: Path):
    """三行两列总览图：左 linear，右 log1p。"""
    nrows = len(groups_ordered)
    fig, axes = plt.subplots(nrows, 2, figsize=OVERVIEW_FIGSIZE, dpi=OVERVIEW_DPI, squeeze=False)

    for r, gname in enumerate(groups_ordered):
        lines = lines_by_group[gname]

        # linear
        ax1 = axes[r, 0]
        for label, x, y in lines:
            x1, y1 = downsample_xy(x, y, MAX_POINTS)
            ax1.plot(x1, y1, label=label, lw=LINEWIDTH, alpha=ALPHA)
        ax1.set_title(f"P={gname} (linear)")
        ax1.set_xlabel("pnode rank")
        ax1.set_ylabel("total_count")
        ax1.yaxis.set_major_formatter(FuncFormatter(human_fmt))
        ax1.grid(True, alpha=0.3)
        add_legend(ax1, loc_inside="upper right")

        # log1p
        ax2 = axes[r, 1]
        for label, x, y in lines:
            y_log1p = np.log10(y.astype(float) + 1.0)
            x2, y2 = downsample_xy(x, y_log1p, MAX_POINTS)
            ax2.plot(x2, y2, label=label, lw=LINEWIDTH, alpha=ALPHA)
        ax2.set_title(f"P={gname} (log1p)")
        ax2.set_xlabel("pnode rank")
        ax2.set_ylabel("log10")
        ax2.grid(True, alpha=0.3)
        add_legend(ax2, loc_inside="upper right")

    if LEGEND_OUTSIDE:
        plt.tight_layout(rect=[0,0,0.86,1])
    else:
        plt.tight_layout()

    fig.savefig(savepath, dpi=OVERVIEW_DPI)
    plt.close(fig)
    print(f"[Saved] {savepath}")

def plot_std_bars(group_name: str, stats, savepath: Path):
    """柱状图：std 值并标注具体数值"""
    if not stats: return
    stats = sorted(stats, key=lambda t: t[1], reverse=True)
    labels = [s[0] for s in stats]
    vals = [s[1] for s in stats]

    fig, ax = plt.subplots(figsize=(max(12, 1.1*len(labels)), 6), dpi=DPI)
    xs = np.arange(len(labels))
    bars = ax.bar(xs, vals)

    ax.set_title(f"STD of total_count per pnode — P={group_name}")
    ax.set_ylabel("std(total_count)")
    ax.yaxis.set_major_formatter(FuncFormatter(human_fmt))
    ax.set_xticks(xs); ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.3)

    for rect, v in zip(bars, vals):
        ax.text(rect.get_x()+rect.get_width()/2,
                rect.get_height(),
                human_fmt(v, None),
                ha="center", va="bottom", fontsize=9)

    plt.tight_layout()
    fig.savefig(savepath, dpi=DPI)
    plt.close(fig)
    print(f"[Saved] {savepath}")

def plot_zero_ratio_bars(group_name: str, zero_stats, savepath: Path):
    """空载节点比例（total_count==0）柱状图，标注百分比"""
    if not zero_stats: return
    zero_stats = sorted(zero_stats, key=lambda t: t[1], reverse=True)
    labels = [s[0] for s in zero_stats]
    vals = [s[1] for s in zero_stats]  # ratio in [0,1]

    fig, ax = plt.subplots(figsize=(max(12, 1.1*len(labels)), 6), dpi=DPI)
    xs = np.arange(len(labels))
    bars = ax.bar(xs, vals)

    ax.set_title(f"Zero-load pnode ratio — P={group_name}")
    ax.set_ylabel("ratio of pnodes with total_count == 0")
    ax.set_ylim(0, 1)
    ax.set_xticks(xs); ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.3)

    for rect, v in zip(bars, vals):
        ax.text(rect.get_x()+rect.get_width()/2,
                rect.get_height(),
                f"{v*100:.1f}%",
                ha="center", va="bottom", fontsize=9)

    plt.tight_layout()
    fig.savefig(savepath, dpi=DPI)
    plt.close(fig)
    print(f"[Saved] {savepath}")

def plot_cdf_log1p(group_name: str, lines, savepath: Path):
    """ECDF 对比：x = log10"""
    if not lines: return
    plt.figure(figsize=(12, 7), dpi=DPI)
    for label, _x, y in lines:
        xvals = np.log10(y.astype(float) + 1.0)
        x_sorted, ecdf_y = ecdf_xy(xvals)
        plt.step(x_sorted, ecdf_y, where="post", label=label)
    plt.title(f"ECDF of log10 — P={group_name}")
    plt.xlabel("log10")
    plt.ylabel("CDF")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(savepath, dpi=DPI)
    plt.close()
    print(f"[Saved] {savepath}")

# ===================== 主流程 =====================
def main():
    # 过滤实际存在的目录
    valid = []
    for name in RUNS:
        if (BASE_DIR / name / CSV_NAME).exists():
            valid.append(name)
        else:
            print(f"[Skip] {name}: {(BASE_DIR / name / CSV_NAME)} not found")

    if not valid:
        print("No valid runs. Check BASE_DIR & RUNS."); return

    # 读取并聚合每个方案
    run_info = {}
    for name in valid:
        csv_path = BASE_DIR / name / CSV_NAME
        print(f"[Load] {name}")
        df = load_pnode_vector(csv_path)
        if df.empty:
            print(f"[Warn] {name} empty after aggregation"); continue
        pcount_file = int(df["pnode_idx"].nunique())
        pcount_name = parse_pcount(name)
        if pcount_name is not None and pcount_name != pcount_file:
            print(f"[Note] {name}: pnodes(file)={pcount_file}, (name)={pcount_name} -> using file value")
        x, y = build_rank_xy(df)
        run_info[name] = {"pcount": pcount_file, "x": x, "y": y}

    if not run_info:
        print("No data parsed."); return

    # 按 pnode 数量分组
    groups = {}
    for name, info in run_info.items():
        groups.setdefault(info["pcount"], []).append(name)

    # —— 单组图 + STD + 诊断 —— #
    for pcount in sorted(groups.keys()):
        names = sorted(groups[pcount])
        lines = [(name, run_info[name]["x"], run_info[name]["y"]) for name in names]

        # 折线：线性 + log1p
        plot_group_records(str(pcount), lines, OUT_DIR / f"records_per_pnode_P{pcount}.png")

        # STD 柱状
        stats = []
        zero_stats = []
        for name in names:
            y = run_info[name]["y"]
            stdv = float(np.std(y, ddof=1)) if len(y) > 1 else 0.0
            stats.append((name, stdv))
            zero_ratio = float((y <= 0).sum()) / float(len(y))
            zero_stats.append((name, zero_ratio))
        plot_std_bars(str(pcount), stats, OUT_DIR / f"std_total_count_P{pcount}.png")

        # 诊断 1：Zero-load ratio
        plot_zero_ratio_bars(str(pcount), zero_stats, OUT_DIR / f"zero_ratio_P{pcount}.png")

        # 诊断 2：ECDF of log10(total_count+1)
        plot_cdf_log1p(str(pcount), lines, OUT_DIR / f"cdf_log1p_P{pcount}.png")

    # —— 生成总览图（三行两列，log 列为 log1p） —— #
    ordered_groups = [g for g in [128, 1024, 8192] if g in groups]
    if ordered_groups:
        lines_by_group = {}
        for pcount in ordered_groups:
            names = sorted(groups[pcount])
            lines_by_group[str(pcount)] = [(name, run_info[name]["x"], run_info[name]["y"]) for name in names]
        plot_overview([str(g) for g in ordered_groups], lines_by_group, OUT_DIR / "records_per_pnode_OVERVIEW.png")

    print(f"Done. Figures in: {OUT_DIR.resolve()}")

if __name__ == "__main__":
    main()