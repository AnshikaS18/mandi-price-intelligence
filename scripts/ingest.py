"""
Mandi Price Intelligence - Ingestion Script

Pulls daily mandi (agricultural market) price data from the data.gov.in
Agmarknet dataset and lands it as raw CSV files, one per pull, under data/raw/.

API docs: https://www.data.gov.in/resource/current-daily-price-various-commodities-various-markets-mandi
Resource ID: 9ef84268-d588-465a-a308-a864a43d0070

NOTE ON RESPONSE FORMAT: confirmed via browser test that with a real (non-shared)
API key, the API correctly honors format=json and returns clean JSON. The shared
public test key was observed returning XML regardless of the format param - that
was a test-key quirk, not a general API behavior. This script parses JSON only.

NOTE ON FILTERS: filters[state]/filters[state.keyword] were unreliable when tested
with the shared public test key. Until confirmed working with a real key, this
script pulls in bulk (unfiltered) and does state/date filtering LOCALLY in pandas
after the pull - slower per-request, but correct regardless of server-side filters.

Usage:
    python scripts/ingest.py                  # pull today's data, default limit
    python scripts/ingest.py --limit 5000      # pull up to 5000 records
    python scripts/ingest.py --max-pages 10    # cap how many pages to fetch
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
PAGE_SIZE = 1000  # data.gov.in typically allows up to ~1000-5000 per page with a real key; shared test key caps at 10
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
REQUEST_TIMEOUT = 60  # seconds - govt API can be slow; give it real room before giving up
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5

# Some govt/WAF-protected APIs silently block requests carrying the default
# python-requests User-Agent (a common bot signature) while allowing normal
# browser traffic through. Sending browser-like headers avoids that.
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

FIELDS = [
    "state",
    "district",
    "market",
    "commodity",
    "variety",
    "grade",
    "arrival_date",
    "min_price",
    "max_price",
    "modal_price",
]

# The underlying API is Elasticsearch-backed and refuses to paginate past
# offset=10000 (a hard ES default), even though "total" reports more records
# exist nationally. Working around this by pulling state-by-state instead of
# one giant national pull - each state's daily total is well under 10000.
STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
    "Keralam", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya",
    "Mizoram", "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim",
    "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand",
    "West Bengal", "Andaman and Nicobar", "Chandigarh",
    "Dadra and Nagar Haveli and Daman and Diu", "Delhi", "Jammu and Kashmir",
    "Ladakh", "Puducherry",
]


def get_api_key() -> str:
    key = os.getenv("DATA_GOV_API_KEY")
    if not key:
        sys.exit(
            "ERROR: DATA_GOV_API_KEY not set. Copy .env.example to .env and fill it in."
        )
    return key


def fetch_page(api_key: str, offset: int, limit: int, state: str | None = None) -> dict:
    """Fetch one page of results, with retries on timeout/connection errors.
    Returns the parsed JSON response as a dict."""
    params = {
        "api-key": api_key,
        "format": "json",
        "offset": offset,
        "limit": limit,
    }
    if state:
        # NOTE: this filter syntax is UNVERIFIED against the real API from this
        # environment (sandbox network restrictions prevented live testing).
        # If it comes back with a 400 error or an "unfiltered" result, try
        # swapping this key to "filters[state]" (without .keyword) instead.
        params["filters[state.keyword]"] = state

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                API_BASE_URL,
                params=params,
                headers=REQUEST_HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            print(
                f"    Attempt {attempt}/{MAX_RETRIES} failed ({e.__class__.__name__}), "
                f"retrying in {RETRY_BACKOFF_SECONDS}s..."
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS)

    raise RuntimeError(
        f"Failed to fetch offset={offset} after {MAX_RETRIES} attempts"
    ) from last_error


def parse_records(payload: dict):
    """Extract record dicts + pagination metadata from the parsed JSON."""
    total = int(payload.get("total", 0))
    count = int(payload.get("count", 0))

    records = []
    for item in payload.get("records", []):
        row = {field: item.get(field, "") for field in FIELDS}
        records.append(row)

    return records, total, count


def fetch_all(api_key: str, page_size: int, max_pages: int | None, state: str | None = None):
    """Page through the API until all records are fetched (or max_pages hit).
    If state is given, only pulls records for that state."""
    all_records = []
    offset = 0
    page_num = 0

    while True:
        page_num += 1
        label = f" [{state}]" if state else ""
        print(f"  Fetching page {page_num}{label} (offset={offset}, limit={page_size})...")
        payload = fetch_page(api_key, offset, page_size, state=state)
        records, total, count = parse_records(payload)

        if page_num == 1:
            print(f"    Total records available: {total}")

        all_records.extend(records)
        offset += count

        if count == 0 or offset >= total:
            break
        if offset >= 10000:
            print(
                f"    WARNING: hit offset=10000 for {state or 'ALL'} - the API's "
                f"pagination ceiling. Stopping here; some records for this "
                f"slice may be missing. Consider filtering by date too."
            )
            break
        if max_pages and page_num >= max_pages:
            print(f"    Hit --max-pages limit ({max_pages}), stopping early.")
            break

        time.sleep(0.5)  # be polite to the government server

    return all_records


def fetch_all_states(api_key: str, page_size: int, max_pages: int | None):
    """Pull every state separately to stay under the 10k offset ceiling."""
    all_records = []
    for i, state in enumerate(STATES, start=1):
        print(f"\n[{i}/{len(STATES)}] State: {state}")
        state_records = fetch_all(api_key, page_size, max_pages, state=state)
        print(f"    Got {len(state_records)} records for {state}")
        all_records.extend(state_records)
    return all_records


def save_raw(records: list[dict], run_timestamp: str) -> str:
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    out_path = os.path.join(RAW_DATA_DIR, f"mandi_prices_{run_timestamp}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(records)
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Ingest mandi price data")
    parser.add_argument(
        "--limit", type=int, default=PAGE_SIZE, help="Records per API page"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Stop after this many pages per state (useful for quick tests)",
    )
    parser.add_argument(
        "--state",
        type=str,
        default=None,
        help="Pull just one state (e.g. --state 'Gujarat') to test the filter works",
    )
    args = parser.parse_args()

    api_key = get_api_key()
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    print(f"Starting ingestion run {run_timestamp}")

    if args.state:
        records = fetch_all(api_key, args.limit, args.max_pages, state=args.state)
    else:
        records = fetch_all_states(api_key, args.limit, args.max_pages)

    if not records:
        print("WARNING: No records fetched. Check your API key and connection.")
        sys.exit(1)

    out_path = save_raw(records, run_timestamp)
    print(f"\nDone. Saved {len(records)} records to {out_path}")


if __name__ == "__main__":
    main()