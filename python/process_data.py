# process_data.py
# Takes the OPW and tide dataframes, matches them up, and outputs JSON files
# ready for the website to read.
#
# Outputs:
#   data/all_tides.json     — all historic low tides with matched OPW reading
#   data/future_tides.json  — upcoming low tides (no OPW data yet)
#   data/opw_data.json      — all raw OPW readings (for the river level chart)

import json
import os
from datetime import datetime


# ── THRESHOLD BANDS ──────────────────────────────────────────────────────────
# These match exactly what's in your spreadsheet screenshot.
# Easy to update here in one place.

OPW_BANDS = [
    {"min": 0.00, "max": 0.75, "label": "Too low",       "colour": "#d0ccc8"},
    {"min": 0.75, "max": 0.88, "label": "Low & shallow", "colour": "#f0c070"},
    {"min": 0.88, "max": 1.00, "label": "Good",          "colour": "#aed6a8"},
    {"min": 1.00, "max": 1.12, "label": "Good",          "colour": "#81c784"},
    {"min": 1.12, "max": 1.24, "label": "Good",          "colour": "#4caf50"},
    {"min": 1.24, "max": 1.37, "label": "Best",          "colour": "#2e7d32"},
    {"min": 1.37, "max": 1.49, "label": "High / good",   "colour": "#26a69a"},
    {"min": 1.49, "max": 1.73, "label": "High / surf",   "colour": "#64b5f6"},
    {"min": 1.73, "max": 1.98, "label": "High green",    "colour": "#1976d2"},
    {"min": 1.98, "max": 99.0, "label": "Flood",         "colour": "#7b1fa2"},
]

TIDE_BANDS = [
    {"min": 0.00, "max": 1.00, "label": "Good",     "colour": "#4caf50"},
    {"min": 1.00, "max": 1.15, "label": "Marginal", "colour": "#f0c070"},
    {"min": 1.15, "max": 99.0, "label": "Too high", "colour": "#e57373"},
]


def get_opw_band(level):
    """Returns the condition band dict for a given OPW level."""
    for band in OPW_BANDS:
        if band["min"] <= level < band["max"]:
            return band
    return OPW_BANDS[-1]


def get_tide_band(height):
    """Returns the condition band dict for a given tide height."""
    for band in TIDE_BANDS:
        if band["min"] <= height < band["max"]:
            return band
    return TIDE_BANDS[-1]


def get_overall(tide_height, opw_level):
    """
    Returns an overall condition label based on both tide and OPW level.
    Both must be good for the wave to run.
    """
    tide_band = get_tide_band(tide_height)
    opw_band  = get_opw_band(opw_level)

    if opw_level < 0.75:
        return {"label": "No wave",      "colour": "#d0ccc8"}
    if tide_band["label"] == "Too high":
        return {"label": "Tide too high","colour": "#e57373"}
    if tide_band["label"] == "Good" and opw_level >= 0.75:
        return {"label": "On",           "colour": "#2e7d32"}
    if tide_band["label"] == "Marginal":
        return {"label": "Marginal",     "colour": "#f0c070"}
    return     {"label": "No wave",      "colour": "#d0ccc8"}


def find_nearest_opw(tide_dt, opw_df):
    """
    Given a low tide datetime, finds the OPW row with the closest timestamp.
    Returns a dict with the OPW reading, or None if no data available.
    """
    if opw_df.empty:
        return None

    # Calculate the absolute time difference between this tide and every OPW reading
    time_diffs = (opw_df["datetime"] - tide_dt).abs()

    # Find the index of the smallest difference
    nearest_idx = time_diffs.idxmin()
    nearest_row = opw_df.loc[nearest_idx]

    # Only accept if within 2 hours — otherwise the match is too loose
    diff_minutes = time_diffs[nearest_idx].total_seconds() / 60
    if diff_minutes > 120:
        return None

    return {
        "datetime":     nearest_row["datetime"].strftime("%Y-%m-%d %H:%M"),
        "level":        round(float(nearest_row["level"]), 3),
        "diff_minutes": round(diff_minutes, 1),
    }


def process(opw_df, tides_df):
    """
    Main processing function.

    Takes:
        opw_df   — DataFrame with columns: datetime, level
        tides_df — DataFrame with columns: datetime, tide_height, type, is_future

    Returns:
        all_tides       — all low tides (past + future), no OPW data
        future_tides    — upcoming low tides only, no OPW data
        tides_and_flow  — past low tides with matched OPW reading
        opw_at_low_tide — just the OPW readings nearest to each low tide
    """

    # ── 1. All tides (past + future, no OPW) ─────────────────────────────────
    print("Processing all tides...")
    all_tides = []
    for _, row in tides_df.iterrows():
        tide_dt     = row["datetime"]
        tide_height = round(float(row["tide_height"]), 2)
        all_tides.append({
            "datetime":    tide_dt.strftime("%Y-%m-%d %H:%M"),
            "tide_height": tide_height,
            "tide_band":   get_tide_band(tide_height),
            "is_future":   bool(row["is_future"]),
        })
    print(f"  {len(all_tides)} total low tides")

    # ── 2. Future tides only (no OPW) ────────────────────────────────────────
    print("Processing future tides...")
    future_tides = [t for t in all_tides if t["is_future"]]
    print(f"  {len(future_tides)} future low tides")

    # ── 3. Tides and flow (past only, with matched OPW) ──────────────────────
    print("Processing tides and flow...")
    historic = tides_df[~tides_df["is_future"]].copy()
    tides_and_flow = []

    for _, row in historic.iterrows():
        tide_dt     = row["datetime"]
        tide_height = round(float(row["tide_height"]), 2)
        nearest_opw = find_nearest_opw(tide_dt, opw_df)
        opw_level   = nearest_opw["level"] if nearest_opw else None

        tides_and_flow.append({
            "datetime":    tide_dt.strftime("%Y-%m-%d %H:%M"),
            "tide_height": tide_height,
            "tide_band":   get_tide_band(tide_height),
            "opw":         nearest_opw,
            "opw_band":    get_opw_band(opw_level) if opw_level is not None else None,
            "overall":     get_overall(tide_height, opw_level) if opw_level is not None else None,
        })

    matched = sum(1 for t in tides_and_flow if t["opw"] is not None)
    print(f"  {len(tides_and_flow)} historic tides, {matched} matched to OPW reading")

    # ── 4. OPW at low tide only (no tide data) ───────────────────────────────
    print("Processing OPW at low tide...")
    opw_at_low_tide = []

    for entry in tides_and_flow:
        if entry["opw"] is not None:
            opw = entry["opw"]
            level = opw["level"]
            opw_at_low_tide.append({
                "datetime": opw["datetime"],
                "level":    level,
                "opw_band": get_opw_band(level),
            })

    print(f"  {len(opw_at_low_tide)} OPW readings at low tide")

    return all_tides, future_tides, tides_and_flow, opw_at_low_tide


def save_json(data, filename):
    """Saves a Python object as a formatted JSON file."""
    os.makedirs("data", exist_ok=True)  # create /data folder if it doesn't exist
    filepath = os.path.join("data", filename)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"  Saved {filepath} ({len(data)} records, "
          f"{os.path.getsize(filepath) / 1024:.1f} KB)")


# ── QUICK TEST ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from fetch_opw   import fetch_opw_data
    from fetch_tides import fetch_tide_data

    print("=== Fetching data ===")
    opw_df   = fetch_opw_data()
    tides_df = fetch_tide_data(days_back=30, days_forward=7)

    print("\n=== Processing ===")
    all_tides, future_tides, tides_and_flow, opw_at_low_tide = process(opw_df, tides_df)

    print("\n=== Saving JSON files ===")
    save_json(all_tides,       "all_tides.json")
    save_json(future_tides,    "future_tides.json")
    save_json(tides_and_flow,  "tides_and_flow.json")
    save_json(opw_at_low_tide, "opw_data.json")

    print("\n=== Sample: first all_tides entry ===")
    if all_tides:
        print(json.dumps(all_tides[0], indent=2))

    print("\n=== Sample: first tides_and_flow entry ===")
    if tides_and_flow:
        print(json.dumps(tides_and_flow[0], indent=2))

    print("\n=== Sample: first opw_at_low_tide entry ===")
    if opw_at_low_tide:
        print(json.dumps(opw_at_low_tide[0], indent=2))

    print("\nDone!")
