# -*- coding: utf-8 -*-
"""
Strict Geolife data-cleaning script with no external configuration.
- Read Trajectory/*.plt from each user directory under ROOT
- Normalize timestamps to UTC (trailing Z)
- Filter by a global time window (default: 2007-04-01 to 2012-08-31)
- Sort points within each trajectory and deduplicate points within the same second
- Filter speed/distance outliers and near-duplicate jitter
- Clip to the Beijing bounding box
- Filter trajectories by minimum point count
- Produce geolife_clean.csv and preprocess_summary.txt

Usage:
  python preprocess.py --root "D:/GeolifeTrajectories/Data" --out "geolife_clean.csv" [--verbose]

Requires only pandas. Python 3.9+ is needed for zoneinfo.
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
# Configuration constants (edit directly if needed)
# =========================

# Root data directory containing user folders, each with Trajectory/*.plt
DEFAULT_ROOT = r"D:\implementation\p2psta\implementation\geolife\Geolife Trajectories 1.3\Data"

# Output CSV filename
DEFAULT_OUT_CSV = "geolife_clean.csv"

# Whether to additionally emit a Pickle file (empty string disables it)
DEFAULT_OUT_PKL = ""   # for example: "geolife_clean.pkl"

# Local timezone of the raw records (Geolife was collected in Beijing, so Asia/Shanghai is typical)
LOCAL_TZ = "Asia/Shanghai"

# Whether to convert to UTC (query windows usually use Z/UTC, so True is recommended)
TO_UTC = True

# --- Global time window in UTC [inclusive, exclusive) ---
# Matches the official dataset period: 2007-04 to 2012-08
TIME_START_UTC = "2007-04-01T00:00:00Z"
TIME_END_UTC_EXCL = "2012-09-01T00:00:00Z" 

# Spatial clipping range (rough Beijing bounding box; shrink or expand as needed)
LAT_MIN, LAT_MAX = 39.0, 41.0
LON_MIN, LON_MAX = 115.0, 118.0

# Cleaning rules
MAX_SPEED_KMH = 200.0   # if speed between adjacent points exceeds this threshold, drop the later point
MIN_TIME_DIFF_S = 1     # if adjacent points are closer than this in time, trigger the near-jitter rule
MIN_MOVE_M = 1.0        # if distance and time gap are both tiny, treat it as jitter and drop the later point
MIN_POINTS_PER_TRAJ = 5 # minimum number of points per trajectory
DROP_ZERO_COORD = True  # drop points at (0,0)

# Logging verbosity
VERBOSE = False


# =========================
# Helper functions
# =========================

def log(msg: str):
    if VERBOSE:
        print(msg)

def safe_parse_datetime(date_str: str, time_str: str, local_tz: ZoneInfo) -> Optional[datetime]:
    """
    Geolife PLT uses separate "date" and "time" columns, usually in local time.
    Return a timezone-aware datetime object (converted to UTC when TO_UTC is enabled).
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
# Main logic
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

        "rows_bad_format": 0,         # missing columns / NaN
        "rows_bad_time": 0,           # timestamp parse failure
        "rows_zero_coord": 0,         # (0,0)
        "rows_out_of_bbox": 0,        # outside bounding box
        "rows_out_of_timerange": 0,   # outside the global time window

        "rows_same_second_merged": 0, # same-second deduplication
        "rows_speed_outlier": 0,      # speed outlier
        "rows_too_close": 0,          # near-duplicate jitter

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
                # Geolife standard: the first six lines are headers; data starts from line seven
                df = pd.read_csv(
                    fpath, skiprows=6, header=None,
                    names=["lat", "lon", "unused", "alt_ft", "days", "date", "time"],
                    dtype={"lat":"float64","lon":"float64","unused":"float64","alt_ft":"float64","days":"float64","date":"string","time":"string"},
                    na_filter=True
                )
            except Exception:
                # Skip corrupted files
                continue

            if df.empty:
                continue

            stats["rows_total"] += len(df)

            # 1) Drop malformed rows
            before = len(df)
            df = df.dropna(subset=["lat", "lon", "date", "time"])
            stats["rows_bad_format"] += (before - len(df))

            # 2) Drop (0,0)
            if DROP_ZERO_COORD:
                before = len(df)
                df = df[~((df["lat"] == 0.0) & (df["lon"] == 0.0))]
                stats["rows_zero_coord"] += (before - len(df))

            # 3) Parse timestamps into UTC-aware datetimes
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

            # 4) Filter by the global UTC time window [inclusive, exclusive)
            before = len(df)
            df = df[(df["datetime"] >= t_start) & (df["datetime"] < t_end_ex)]
            stats["rows_out_of_timerange"] += (before - len(df))

            # 5) Clip to the Beijing bounding box
            before = len(df)
            df = df[
                (df["lat"] >= LAT_MIN) & (df["lat"] <= LAT_MAX) &
                (df["lon"] >= LON_MIN) & (df["lon"] <= LON_MAX)
            ]
            stats["rows_out_of_bbox"] += (before - len(df))

            if df.empty:
                continue

            # 6) Sort by time
            df = df.sort_values(["datetime"]).reset_index(drop=True)

            # 7) Deduplicate points within the same second (keep the first one)
            df["dt_s"] = df["datetime"].dt.floor("S")
            before = len(df)
            df = df.drop_duplicates(subset=["lat","lon","dt_s"], keep="first").reset_index(drop=True)
            stats["rows_same_second_merged"] += (before - len(df))

            # 8) Filter speed outliers / jitter between adjacent points
            lats = df["lat"].to_numpy()
            lons = df["lon"].to_numpy()
            times = df["dt_s"].astype("int64").to_numpy() // 10**9  # epoch seconds

            keep = [True]  # always keep the first point
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

            # 9) Enforce the minimum point count
            if len(df) < MIN_POINTS_PER_TRAJ:
                stats["traj_total"] += 1
                stats["traj_too_short"] += 1
                continue

            # 10) Emit output columns
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

    # Normalize to ISO8601 Z (UTC)
    data["datetime"] = pd.to_datetime(data["datetime"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Output bounds
    lat_min, lat_max = float(data["lat"].min()), float(data["lat"].max())
    lon_min, lon_max = float(data["lon"].min()), float(data["lon"].max())
    ts_min = pd.to_datetime(data["datetime"]).min()
    ts_max = pd.to_datetime(data["datetime"]).max()

    # Write CSV
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(out_csv, index=False, encoding="utf-8")

    # Optionally write PKL
    if out_pkl:
        out_pkl.parent.mkdir(parents=True, exist_ok=True)
        data.to_pickle(out_pkl)

    # Write summary
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
        w(f"global_time_window  : [{TIME_START_UTC}, {TIME_END_UTC_EXCL})  # UTC inclusive, exclusive")

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

