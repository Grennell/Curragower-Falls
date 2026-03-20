# fetch_opw.py
# Fetches the OPW water level CSV and parses it into a clean list of readings.
#
# Libraries used:
#   requests  — for making HTTP requests (fetching URLs)
#   pandas    — for reading and working with CSV/table data

import requests
import pandas as pd
import urllib3
from io import StringIO
from datetime import datetime

from urls import get_opw_url

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_opw_data():
    """
    Fetches the OPW Ball's Bridge water level CSV and returns a
    cleaned pandas DataFrame with columns:
        datetime  (Python datetime object)
        level     (float, water level in metres)
    """

    url = get_opw_url()
    print(f"Fetching OPW data from: {url}")

    # Make the HTTP request — like a browser visiting a URL
    response = requests.get(url, timeout=15, verify=False)

    # Raise an error if something went wrong (e.g. 404, 500)
    response.raise_for_status()

    # The CSV content is in response.text
    # We wrap it in StringIO so pandas can read it like a file
    raw_text = response.text
    print(f"  Got {len(raw_text)} characters of CSV data")

    # Read into a DataFrame — we'll inspect the columns first
    # OPW CSVs typically have no header row, just two columns: datetime, value
    try:
        df = pd.read_csv(
            StringIO(raw_text),
            skipinitialspace=True,
        )
    except Exception as e:
        print(f"  Error parsing CSV: {e}")
        print(f"  First 200 chars of raw data: {raw_text[:200]}")
        raise
    # Rename columns to our standard names regardless of what OPW calls them
    df.columns = ["datetime_str", "level"]

    print(f"  Parsed {len(df)} rows. First few:")
    print(df.head(3).to_string(index=False))

    # Drop any rows where level is missing or not a number
    df = df.dropna(subset=["level"])
    df = df[pd.to_numeric(df["level"], errors="coerce").notna()]
    df["level"] = df["level"].astype(float)

    # Parse the datetime column
    # OPW format is typically: "01-Jan-2026 00:15:00" — we'll try a few formats
    def parse_opw_datetime(s):
        for fmt in ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
            "%d-%b-%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S",
            "%d/%m/%Y %H:%M"]:
            try:
                return datetime.strptime(str(s).strip(), fmt)
            except ValueError:
                continue
        print(f"  ⚠ Could not parse datetime: {s!r}")
        return None

    df["datetime"] = df["datetime_str"].apply(parse_opw_datetime)
    df = df.dropna(subset=["datetime"])  # drop rows we couldn't parse

    # Sort chronologically
    df = df.sort_values("datetime").reset_index(drop=True)

    print(f"  Clean rows after parsing: {len(df)}")
    print(f"  Date range: {df['datetime'].min()} → {df['datetime'].max()}")
    print(f"  Level range: {df['level'].min():.3f}m → {df['level'].max():.3f}m")

    # Return just the two useful columns
    return df[["datetime", "level"]]


# ── QUICK TEST ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df = fetch_opw_data()
    print("\n=== Sample of OPW data ===")
    print(df.head(10).to_string(index=False))
