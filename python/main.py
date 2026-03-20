# main.py
# The single entry point to run everything.
# Run this file to fetch fresh data and update all JSON outputs.
#
# Usage:
#   python main.py              — full run (30 days back, 7 days forward)
#   python main.py --quick      — quick test (3 days back, 2 days forward)

import sys
import json
from datetime import datetime

from fetch_opw   import fetch_opw_data
from fetch_tides import fetch_tide_data
from process_data import process, save_json


def run(days_back=30, days_forward=7):
    start_time = datetime.now()
    print("=" * 50)
    print("  Curragower Falls — Data Pipeline")
    print(f"  Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # ── Step 1: Fetch OPW data ────────────────────────────────────────────
    print("\n[1/3] Fetching OPW river level data...")
    opw_df = fetch_opw_data()

    # ── Step 2: Fetch tide data ───────────────────────────────────────────
    print(f"\n[2/3] Fetching tide data ({days_back} days back, {days_forward} days forward)...")
    tides_df = fetch_tide_data(days_back=days_back, days_forward=days_forward)

    # ── Step 3: Process and save ──────────────────────────────────────────
    print("\n[3/3] Processing and saving...")
    all_tides, future_tides, tides_and_flow, opw_at_low_tide = process(opw_df, tides_df)

    save_json(all_tides,       "all_tides.json")
    save_json(future_tides,    "future_tides.json")
    save_json(tides_and_flow,  "tides_and_flow.json")
    save_json(opw_at_low_tide, "opw_data.json")

    # ── Summary ───────────────────────────────────────────────────────────
    elapsed = (datetime.now() - start_time).total_seconds()
    print("\n" + "=" * 50)
    print("  ✓ Done!")
    print(f"  All tides:       {len(all_tides)}")
    print(f"  Future tides:    {len(future_tides)}")
    print(f"  Tides and flow:  {len(tides_and_flow)}")
    print(f"  OPW at low tide: {len(opw_at_low_tide)}")
    print(f"  Time taken:      {elapsed:.1f}s")
    print("=" * 50)

    # Print the most recent tide condition as a quick sanity check
    if tides_and_flow:
        latest = tides_and_flow[-1]
        print(f"\n  Latest tide:  {latest['datetime']}")
        print(f"  Tide height:  {latest['tide_height']}m  → {latest['tide_band']['label']}")
        if latest["opw"]:
            print(f"  OPW level:    {latest['opw']['level']}m  → {latest['opw_band']['label']}")
            print(f"  Overall:      {latest['overall']['label']}")


if __name__ == "__main__":
    quick = "--quick" in sys.argv
    if quick:
        print("(Quick mode: 3 days back, 2 days forward)")
        run(days_back=3, days_forward=2)
    else:
        run(days_back=30, days_forward=7)
