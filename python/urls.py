# urls.py
# Responsible for generating all the URLs we need to fetch data.
# Keeping URLs in one place means if a source changes, we only fix it here.

from datetime import datetime, timedelta


# ── OPW ──────────────────────────────────────────────────────────────────────
# The OPW (Office of Public Works) gauging station at Ball's Bridge, Limerick.
# Station ID: 25061, Parameter: 0001 (water level)
# The URL has a cache-busting timestamp on the end — we generate a fresh one each run.

def get_opw_url():
    """Returns the URL for the OPW Ball's Bridge CSV data."""
    timestamp = int(datetime.now().timestamp())
    url = f"https://waterlevel.ie/data/month/25061_0001.csv?{timestamp}"
    return url


# ── TIDES ─────────────────────────────────────────────────────────────────────
# tidetimes.org.uk has a separate page for each date, structured like:
#   https://www.tidetimes.org.uk/limerick-dock-tide-times-YYYY-MM-DD
# We need to fetch the past 30 days (historic) + next 7 days (future predictions).

def get_tide_urls(days_back=30, days_forward=7):
    """
    Returns a list of tide page URLs covering the past N days and next M days.

    Each URL corresponds to one calendar day's tide predictions.
    We'll parse each page to extract the low tide times and heights.
    """
    urls = []

    today = datetime.now().date()
    start = today - timedelta(days=days_back)
    end   = today + timedelta(days=days_forward)

    # Loop through each day in the range
    current = start
    while current <= end:
        date_str = current.strftime("%Y%m%d")  # e.g. "2026-03-19"
        url = f"https://www.tidetimes.org.uk/limerick-dock-tide-times-{date_str}"
        urls.append((current, url))   # store as (date, url) tuple
        current += timedelta(days=1)

    return urls


# ── QUICK TEST ────────────────────────────────────────────────────────────────
# This block only runs if you execute this file directly (not when imported).
# It's a handy way to test each module in isolation.

if __name__ == "__main__":
    print("=== OPW URL ===")
    print(get_opw_url())

    print("\n=== TIDE URLs (first 3 and last 3) ===")
    tide_urls = get_tide_urls()
    for date, url in tide_urls[:3]:
        print(f"  {date}  →  {url}")
    print("  ...")
    for date, url in tide_urls[-3:]:
        print(f"  {date}  →  {url}")
    print(f"\n  Total: {len(tide_urls)} days")
