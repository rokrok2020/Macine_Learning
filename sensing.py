"""Step 2 of the pipeline: merge the local raw JSON files into one table.

Input : data/raw/<city>/*.json   (written by data_collection.py)
Output: data/raw/sensed_data.csv  - one row per (location, date), columns =
        location, date + every daily variable returned by the API.

- No API calls are made here; only local files are read.
- The location is taken from the coordinates stored inside each JSON
  (nearest known city), not from the file name. This way the same code also
  works on the raw JSON test files of Part B, whatever their names are.
- Duplicates by (location, date) are removed, e.g. if two files overlap.

Run:  python sensing.py
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd

RAW_DIR = Path("data/raw")
OUTPUT = RAW_DIR / "sensed_data.csv"

LOCATIONS = {
    "london": (51.5074, -0.1278),
    "new_york": (40.7128, -74.0060),
    "sydney": (-33.8688, 151.2093),
    "berlin": (52.5200, 13.4050),
    "tokyo": (35.6762, 139.6503),
    "vancouver": (49.2827, -123.1207),
}


def nearest_location(lat: float, lon: float) -> str:
    """Return the known city closest to the given coordinates.

    The API returns grid-cell coordinates that differ slightly from the
    requested ones, so an exact match is not possible.
    """
    return min(LOCATIONS, key=lambda name: math.dist((lat, lon), LOCATIONS[name]))


def json_to_frame(path: Path) -> pd.DataFrame:
    """Convert one raw JSON file to a table: one row per day."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    # payload["daily"] is {variable: [value per day]}; "time" holds the dates.
    df = pd.DataFrame(payload["daily"]).rename(columns={"time": "date"})
    df.insert(0, "location", nearest_location(payload["latitude"], payload["longitude"]))
    return df


def build_sensed_table(raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    """Read all JSON files under raw_dir, merge, de-duplicate and sort."""
    files = sorted(raw_dir.rglob("*.json"))  # data/raw/<city>/*.json
    if not files:
        raise FileNotFoundError(f"No JSON files in {raw_dir}. Run data_collection.py first.")

    df = pd.concat([json_to_frame(f) for f in files], ignore_index=True)
    before = len(df)
    df = df.drop_duplicates(subset=["location", "date"], keep="first")
    df = df.sort_values(["location", "date"]).reset_index(drop=True)
    print(f"Read {len(files)} files, {before} rows, removed {before - len(df)} duplicates")
    return df


if __name__ == "__main__":
    table = build_sensed_table()
    table.to_csv(OUTPUT, index=False)
    print(f"Saved {OUTPUT}: {table.shape[0]} rows x {table.shape[1]} columns")
