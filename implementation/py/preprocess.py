import os
import pandas as pd
import pickle
import yaml
import sys

# Read configuration
config_path = "config.yaml"
if len(sys.argv) > 1:
    config_path = sys.argv[1]

with open(config_path, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

root = cfg["dataset"]["geolife_path"]
csv_file = cfg["dataset"]["csv_path"]
pkl_file = cfg["dataset"]["pickle_path"]

all_points = []

for user in os.listdir(root):
    traj_dir = os.path.join(root, user, "Trajectory")
    if not os.path.exists(traj_dir):
        continue
    for file in os.listdir(traj_dir):
        if not file.endswith(".plt"):
            continue
        df = pd.read_csv(
            os.path.join(traj_dir, file),
            skiprows=6,
            names=["lat", "lon", "unused", "alt", "days", "date", "time"],
        )
        df["datetime"] = pd.to_datetime(df["date"] + " " + df["time"])
        df["user"] = user
        df["traj_id"] = file
        all_points.append(df[["user", "traj_id", "lat", "lon", "datetime"]])

data = pd.concat(all_points, ignore_index=True)

# Filter Beijing area
min_lat, max_lat = cfg["dataset"]["lat_range"]
min_lon, max_lon = cfg["dataset"]["lon_range"]
data = data[
    (data["lat"] >= min_lat) & (data["lat"] <= max_lat) &
    (data["lon"] >= min_lon) & (data["lon"] <= max_lon)
]

print("lat:", float(data["lat"].min()), float(data["lat"].max()))
print("lon:", float(data["lon"].min()), float(data["lon"].max()))

# Save Pickle and CSV
with open(pkl_file, "wb") as f:
    pickle.dump(data, f)
data.to_csv(csv_file, index=False, encoding="utf-8")
print(f"Saved to:\n  {pkl_file}\n  {csv_file}\nTotal rows: {len(data)}")
