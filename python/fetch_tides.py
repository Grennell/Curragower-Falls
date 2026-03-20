# fetch_tides.py
# Fetches tide predictions for Limerick Dock from tidetimes.org.uk.
# Scrapes each day's page and extracts the low tide times and heights.
#
# Libraries used:
#   requests       — HTTP requests
#   beautifulsoup4 — HTML parsing (picking out the data we want from a webpage)
#   pandas         — storing results in a table

import requests
import pandas as pd
import urllib3
from bs4 import BeautifulSoup
from datetime import datetime

from urls import get_tide_urls

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def parse_tide_page(date, html):
    """
    Parses a single tidetimes.org.uk page for one day.
    Returns a list of dicts, one per LOW tide, e.g.:
        [{"datetime": datetime(...), "tide_height": 0.38, "type": "Low"}, ...]
    """
    soup = BeautifulSoup(html, "html.parser")
    tides = []

    # tidetimes.org.uk displays tides in a table — we look for rows
    # containing "Low" in the High/Low column
    # The table structure can vary slightly, so we look broadly
    rows = soup.find_all("tr")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        # Typical columns: Hi/Lo | Time | Height
        tide_type = cells[0].get_text(strip=True)
        time_str  = cells[1].get_text(strip=True)
        height_str = cells[2].get_text(strip=True)

        # Skip header rows silently
        if tide_type.strip() == "Hi/Lo":
            continue
        if not any(c.isdigit() for c in time_str):
            continue
        # We only want Low tides
        if "low" not in tide_type.lower():
            continue

        # Parse the time — format is like "12:16" or "06:32"
        try:
            time_obj = datetime.strptime(time_str, "%H:%M").time()
            tide_datetime = datetime.combine(date, time_obj)
        except ValueError:
            print(f"  ⚠ Could not parse tide time: {time_str!r} on {date}")
            continue

        # Parse the height — format is like "0.38m" or "1.2m"
        try:
            height = float(height_str.replace("m", "").strip())
        except ValueError:
            print(f"  ⚠ Could not parse tide height: {height_str!r} on {date}")
            continue

        tides.append({
            "datetime":    tide_datetime,
            "tide_height": height,
            "type":        "Low",
        })

    return tides


def fetch_tide_data(days_back=30, days_forward=7, delay=1.0):
    """
    Fetches all low tide times and heights for the past N days and next M days.
    Uses parallel requests to fetch multiple pages simultaneously — much faster
    than fetching one page at a time.

    Returns a pandas DataFrame with columns:
        datetime     (Python datetime)
        tide_height  (float, metres)
        type         (always "Low")
        is_future    (bool — True if the tide is in the future)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    tide_urls = get_tide_urls(days_back=days_back, days_forward=days_forward)
    now = datetime.now()

    print(f"Fetching tide data for {len(tide_urls)} days (parallel)...")

    def fetch_one(date_url_tuple):
        """Fetches and parses a single day's tide page."""
        date, url = date_url_tuple
        try:
            response = requests.get(url, timeout=15, verify=False, headers={
                "User-Agent": "CurragowerFalls/1.0 (tide data for personal website)"
            })
            response.raise_for_status()
            tides = parse_tide_page(date, response.text)
            return date, tides, None  # (date, results, error)
        except requests.RequestException as e:
            return date, [], str(e)   # (date, empty, error message)

    all_tides = []

    # ThreadPoolExecutor runs up to max_workers fetches at the same time
    # 8 workers is a good balance — fast but not hammering the server
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_one, item): item for item in tide_urls}

        for future in as_completed(futures):
            date, tides, error = future.result()
            if error:
                print(f"  ⚠ Failed {date}: {error}")
            else:
                print(f"  ✓ {date}: {len(tides)} low tide(s)")
                all_tides.extend(tides)

    if not all_tides:
        print("⚠ No tide data was fetched!")
        return pd.DataFrame(columns=["datetime", "tide_height", "type", "is_future"])

    df = pd.DataFrame(all_tides)
    df = df.sort_values("datetime").reset_index(drop=True)
    df["is_future"] = df["datetime"] > now

    print(f"\nTotal low tides fetched: {len(df)}")
    print(f"  Historic: {(~df['is_future']).sum()}")
    print(f"  Future:   {df['is_future'].sum()}")
    print(f"  Height range: {df['tide_height'].min():.2f}m → {df['tide_height'].max():.2f}m")

    return df


# ── QUICK TEST ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Fetch just 3 days back + 2 days forward to keep the test quick
    df = fetch_tide_data(days_back=3, days_forward=2, delay=0.5)
    print("\n=== Sample of Tide data ===")
    print(df.to_string(index=False))
