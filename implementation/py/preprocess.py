# -*- coding: utf-8 -*-
"""
Geolife 严格数据清洗脚本（无外部配置）
- 读取 ROOT 下每个用户目录的 Trajectory/*.plt
- 统一时区到 UTC（末尾 Z）
- 全局时间窗过滤（默认：2007-04-01 ~ 2012-08-31）
- 轨迹内排序、同秒去重
- 速度/距离异常过滤、近距离抖动过滤
- 北京范围裁剪
- 轨迹最小点数过滤
- 产出 geolife_clean.csv 与 preprocess_summary.txt

用法：
  python preprocess.py --root "D:/GeolifeTrajectories/Data" --out "geolife_clean.csv" [--verbose]

仅需 pandas。Python 3.9+（使用 zoneinfo）。
"""

from __future__ import annotations
import os
import sys
import math
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional

import pandas as pd
from zoneinfo import ZoneInfo  # Python 3.9+

# =========================
# 配置常量（可按需直接改动）
# =========================

# 数据根目录（包含 user 目录，每个 user 下有 Trajectory/*.plt）
DEFAULT_ROOT = r"D:\implementation\p2psta\implementation\geolife\Geolife Trajectories 1.3\Data"

# 输出 CSV 文件名
DEFAULT_OUT_CSV = "geolife_clean.csv"

# 是否额外输出 Pickle（空字符串则不输出）
DEFAULT_OUT_PKL = ""   # 例如 "geolife_clean.pkl"

# 原始记录的当地时区（Geolife 在北京采集，通常用 Asia/Shanghai）
LOCAL_TZ = "Asia/Shanghai"

# 是否转换到 UTC（窗口通常用 Z/UTC，建议 True）
TO_UTC = True

# —— 全局时间窗（UTC，左闭右开）——
# 与官方数据期一致：2007-04 ~ 2012-08
TIME_START_UTC = "2007-04-01T00:00:00Z"
TIME_END_UTC_EXCL = "2012-09-01T00:00:00Z" 

# 空间裁剪范围（北京大致范围，可按需缩小/扩大）
LAT_MIN, LAT_MAX = 39.0, 41.0
LON_MIN, LON_MAX = 115.0, 118.0

# 清洗规则
MAX_SPEED_KMH = 200.0   # 相邻点速度超过此阈值判为异常，丢弃后点
MIN_TIME_DIFF_S = 1     # 相邻点时间差小于该值时，触发“近距离抖动”规则
MIN_MOVE_M = 1.0        # 相邻点距离小于该值且时间也很小 → 认为抖动，丢弃后点
MIN_POINTS_PER_TRAJ = 5 # 轨迹最少点数
DROP_ZERO_COORD = True  # 丢弃 (0,0) 点

# 日志详细程度
VERBOSE = False


# =========================
# 工具函数
# =========================

def log(msg: str):
    if VERBOSE:
        print(msg)

def safe_parse_datetime(date_str: str, time_str: str, local_tz: ZoneInfo) -> Optional[datetime]:
    """
    Geolife PLT: 'date','time' 两列，多为当地时间。
    返回 tz-aware datetime（若 TO_UTC 则转 UTC）。
    """
    s = f"{(date_str or '').strip()} {(time_str or '').strip()}"
    fmts = ["%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"]
    for fmt in fmts:
        try:
            dt_naive = datetime.strptime(s, fmt)
            dt_local = dt_naive.replace(tzinfo=local_tz)
            return dt_local.astimezone(ZoneInfo("UTC")) if TO_UTC else dt_local
        except Exception:
            continue
    return None

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    p = math.pi / 180.0
    dlat = (lat2 - lat1) * p
    dlon = (lon2 - lon1) * p
    a = (math.sin(dlat/2) ** 2 +
         math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin(dlon/2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# =========================
# 主逻辑
# =========================

def preprocess(root: Path, out_csv: Path, out_pkl: Optional[Path]) -> None:
    local_tz = ZoneInfo(LOCAL_TZ)
    t_start = pd.to_datetime(TIME_START_UTC, utc=True)
    t_end_ex = pd.to_datetime(TIME_END_UTC_EXCL, utc=True)  # exclusive

    users = [d for d in root.iterdir() if d.is_dir()]
    users.sort(key=lambda p: p.name)

    all_rows = []
    stats = {
        "files_total": 0,
        "rows_total": 0,

        "rows_bad_format": 0,         # 缺列/NaN
        "rows_bad_time": 0,           # 时间解析失败
        "rows_zero_coord": 0,         # (0,0)
        "rows_out_of_bbox": 0,        # 超出范围
        "rows_out_of_timerange": 0,   # 不在全局时间窗

        "rows_same_second_merged": 0, # 同秒去重
        "rows_speed_outlier": 0,      # 速度异常
        "rows_too_close": 0,          # 近距离抖动

        "traj_total": 0,
        "traj_too_short": 0,
    }

    for user_dir in users:
        user = user_dir.name
        traj_dir = user_dir / "Trajectory"
        if not traj_dir.exists():
            continue

        for fname in os.listdir(traj_dir):
            if not fname.endswith(".plt"):
                continue
            fpath = traj_dir / fname
            stats["files_total"] += 1

            try:
                # Geolife 标准：前 6 行是 header，从第 7 行开始是数据
                df = pd.read_csv(
                    fpath, skiprows=6, header=None,
                    names=["lat", "lon", "unused", "alt_ft", "days", "date", "time"],
                    dtype={"lat":"float64","lon":"float64","unused":"float64","alt_ft":"float64","days":"float64","date":"string","time":"string"},
                    na_filter=True
                )
            except Exception:
                # 文件坏掉就跳过
                continue

            if df.empty:
                continue

            stats["rows_total"] += len(df)

            # 1) 丢弃坏行
            before = len(df)
            df = df.dropna(subset=["lat", "lon", "date", "time"])
            stats["rows_bad_format"] += (before - len(df))

            # 2) 丢弃 (0,0)
            if DROP_ZERO_COORD:
                before = len(df)
                df = df[~((df["lat"] == 0.0) & (df["lon"] == 0.0))]
                stats["rows_zero_coord"] += (before - len(df))

            # 3) 解析时间（→ UTC tz-aware）
            dt_list = []
            bad_time = 0
            for d, t in zip(df["date"].tolist(), df["time"].tolist()):
                dt = safe_parse_datetime(d, t, local_tz)
                if dt is None:
                    dt_list.append(pd.NaT)
                    bad_time += 1
                else:
                    dt_list.append(dt)
            df["datetime"] = pd.to_datetime(dt_list, utc=True, errors="coerce")
            stats["rows_bad_time"] += bad_time
            df = df.dropna(subset=["datetime"])

            # 4) 全局时间窗过滤（UTC，左闭右开）
            before = len(df)
            df = df[(df["datetime"] >= t_start) & (df["datetime"] < t_end_ex)]
            stats["rows_out_of_timerange"] += (before - len(df))

            # 5) 北京范围裁剪
            before = len(df)
            df = df[
                (df["lat"] >= LAT_MIN) & (df["lat"] <= LAT_MAX) &
                (df["lon"] >= LON_MIN) & (df["lon"] <= LON_MAX)
            ]
            stats["rows_out_of_bbox"] += (before - len(df))

            if df.empty:
                continue

            # 6) 时间排序
            df = df.sort_values(["datetime"]).reset_index(drop=True)

            # 7) 同秒去重（保留首点）
            df["dt_s"] = df["datetime"].dt.floor("S")
            before = len(df)
            df = df.drop_duplicates(subset=["lat","lon","dt_s"], keep="first").reset_index(drop=True)
            stats["rows_same_second_merged"] += (before - len(df))

            # 8) 速度/抖动过滤（相邻点）
            lats = df["lat"].to_numpy()
            lons = df["lon"].to_numpy()
            times = df["dt_s"].astype("int64").to_numpy() // 10**9  # epoch 秒

            keep = [True]  # 首点保留
            rm_speed = 0
            rm_close = 0
            for i in range(1, len(df)):
                dt = max(1, int(times[i] - times[i-1]))
                dist_m = haversine_m(lats[i-1], lons[i-1], lats[i], lons[i])
                speed_kmh = (dist_m / dt) * 3.6
                if speed_kmh > MAX_SPEED_KMH:
                    keep.append(False)
                    rm_speed += 1
                    continue
                if dist_m < MIN_MOVE_M and dt < max(2, MIN_TIME_DIFF_S):
                    keep.append(False)
                    rm_close += 1
                    continue
                keep.append(True)

            df = df[pd.Series(keep, index=df.index)].reset_index(drop=True)
            stats["rows_speed_outlier"] += rm_speed
            stats["rows_too_close"] += rm_close

            # 9) 最小点数
            if len(df) < MIN_POINTS_PER_TRAJ:
                stats["traj_total"] += 1
                stats["traj_too_short"] += 1
                continue

            # 10) 产出列
            traj_id = f"{user}/{Path(fname).stem}"
            out = df[["lat","lon","datetime"]].copy()
            out["user"] = user
            out["traj_id"] = traj_id
            out = out[["user","traj_id","lat","lon","datetime"]]
            all_rows.append(out)
            stats["traj_total"] += 1

    if len(all_rows) == 0:
        print("No valid data found. Please check ROOT and ranges.")
        sys.exit(1)

    data = pd.concat(all_rows, ignore_index=True)

    # 统一为 ISO8601 Z（UTC）
    data["datetime"] = pd.to_datetime(data["datetime"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 输出边界
    lat_min, lat_max = float(data["lat"].min()), float(data["lat"].max())
    lon_min, lon_max = float(data["lon"].min()), float(data["lon"].max())
    ts_min = pd.to_datetime(data["datetime"]).min()
    ts_max = pd.to_datetime(data["datetime"]).max()

    # 写 CSV
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(out_csv, index=False, encoding="utf-8")

    # 可选写 PKL
    if out_pkl:
        out_pkl.parent.mkdir(parents=True, exist_ok=True)
        data.to_pickle(out_pkl)

    # 写 summary
    summary_path = out_csv.with_name("preprocess_summary.txt")
    with summary_path.open("w", encoding="utf-8") as f:
        def w(s: str): f.write(s + "\n")
        w("=== Geolife Preprocess Summary ===")
        w(f"source_root         : {root}")
        w(f"csv_out             : {out_csv}")
        if out_pkl: w(f"pickle_out          : {out_pkl}")
        w("")
        w(f"files_total         : {stats['files_total']}")
        w(f"rows_total          : {stats['rows_total']}")
        w("")
        w(f"rows_bad_format     : {stats['rows_bad_format']}")
        w(f"rows_bad_time       : {stats['rows_bad_time']}")
        w(f"rows_zero_coord     : {stats['rows_zero_coord']}")
        w(f"rows_out_of_bbox    : {stats['rows_out_of_bbox']}")
        w(f"rows_out_of_timerange : {stats['rows_out_of_timerange']}")
        w(f"rows_same_second_merged : {stats['rows_same_second_merged']}")
        w(f"rows_speed_outlier  : {stats['rows_speed_outlier']}")
        w(f"rows_too_close      : {stats['rows_too_close']}")
        w("")
        w(f"traj_total          : {stats['traj_total']}")
        w(f"traj_too_short      : {stats['traj_too_short']}")
        w("")
        w(f"lat_range(out)      : [{lat_min:.6f}, {lat_max:.6f}]")
        w(f"lon_range(out)      : [{lon_min:.6f}, {lon_max:.6f}]")
        w(f"ts_min(out UTC)     : {ts_min}")
        w(f"ts_max(out UTC)     : {ts_max}")
        w("")
        w(f"global_time_window  : [{TIME_START_UTC}, {TIME_END_UTC_EXCL})  # UTC 左闭右开")

    print(f"Saved:\n  CSV  -> {out_csv}\n  SUMM -> {summary_path}\nRows kept: {len(data)}")
    if out_pkl:
        print(f"  PKL  -> {out_pkl}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Geolife strict preprocessing (UTC, time-window, dedup, outlier removal).")
    ap.add_argument("--root", type=str, default=DEFAULT_ROOT, help="Geolife dataset root (contains user folders).")
    ap.add_argument("--out", type=str, default=DEFAULT_OUT_CSV, help="Output CSV path (geolife_clean.csv).")
    ap.add_argument("--pkl", type=str, default=DEFAULT_OUT_PKL, help="Optional Pickle output path.")
    ap.add_argument("--verbose", action="store_true", help="Verbose logs.")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    VERBOSE = bool(args.verbose) 
    root = Path(args.root)
    out_csv = Path(args.out)
    out_pkl = Path(args.pkl) if args.pkl else None

    if not root.exists():
        print(f"[ERROR] ROOT not found: {root}")
        sys.exit(2)

    preprocess(root, out_csv, out_pkl)

