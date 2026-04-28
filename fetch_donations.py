"""
fetch_donations.py — Fetches all donations from the Łatwogang fundraiser on siepomaga.pl
and saves them to a JSON file.

Background
----------
siepomaga.pl is a React SPA with no public API documentation. This script uses the
internal payments API discovered by reverse-engineering the site's JS bundles.

API endpoint
------------
GET https://www.siepomaga.pl/api/v1/payments
  ?target_type=Fundraise
  &target_id=LZSw1Ox      (fundraiser ID for "latwogang")
  &sort_by=newest
  &per_page=200            (max allowed)
  &after_id=<id>           (cursor — ID of second-to-last item from previous page)
  &after_value=<timestamp> (cursor — state_changed_at of second-to-last item)

Notes
-----
- A browser User-Agent header is required; plain urllib gets HTTP 403.
- Pagination is cursor-based (not page-number based).
- The fundraiser ID "LZSw1Ox" was resolved via:
    GET /api/donor/web/v2/permalinks/latwogang → fundraise.id

Usage
-----
    python3 fetch_donations.py

Output
------
    latwogang_wplaty.json   — all donations (~650 MB for 3.2M records)
    fetch_latwogang.log     — progress log

Each record in the output JSON has these fields:
    id          str   — unique payment ID
    osoba       str   — donor name (None if anonymous)
    komentarz   str   — donation comment (None if none)
    kwota       str   — amount (as string, e.g. "100.0")
    waluta      str   — currency code (e.g. "PLN")
    data        str   — ISO 8601 timestamp of payment
    anonimowy   bool  — True if donor name is absent
    firma       str   — company name if donated as a company (usually None)
"""

import urllib.request
import urllib.parse
import json
import time

# ── Configuration ────────────────────────────────────────────────────────────

BASE_URL    = "https://www.siepomaga.pl/api/v1/payments"
TARGET_TYPE = "Fundraise"
TARGET_ID   = "LZSw1Ox"   # latwogang fundraiser ID
PER_PAGE    = 200          # max 200; 250 returns an error
OUTPUT_FILE = "latwogang_wplaty.json"
LOG_FILE    = "fetch_latwogang.log"
CHECKPOINT_EVERY = 25      # save progress to disk every N pages

HEADERS = {
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.siepomaga.pl/latwogang",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(msg, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(msg + "\n")


def fetch_page(after_id: str | None = None, after_value: str | None = None) -> dict:
    """Fetch one page of payments. Retries up to 3 times on network errors."""
    params = {
        "target_type": TARGET_TYPE,
        "target_id":   TARGET_ID,
        "sort_by":     "newest",
        "per_page":    str(PER_PAGE),
    }
    if after_id:
        params["after_id"]    = after_id
    if after_value:
        params["after_value"] = after_value

    url = BASE_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=HEADERS)

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except Exception as exc:
            if attempt == 2:
                raise
            wait = 2 ** attempt
            log(f"  Attempt {attempt + 1} failed: {exc} — retrying in {wait}s")
            time.sleep(wait)


def parse_payment(item: dict) -> dict:
    payer = item.get("payer") or {}
    return {
        "id":        item["id"],
        "osoba":     payer.get("name"),
        "komentarz": item.get("comment_text"),
        "kwota":     item.get("amount"),
        "waluta":    item.get("currency"),
        "data":      item.get("state_changed_at"),
        "anonimowy": payer.get("name") is None,
        "firma":     payer.get("company"),
    }

# ── Main fetch loop ───────────────────────────────────────────────────────────

open(LOG_FILE, "w").close()
log("Starting donation fetch — siepomaga.pl/latwogang")
log(f"Output: {OUTPUT_FILE}  |  Checkpoint every {CHECKPOINT_EVERY} pages\n")

all_payments: list[dict] = []
page        = 0
after_id    = None
after_value = None

while True:
    try:
        response = fetch_page(after_id, after_value)
    except Exception as exc:
        log(f"ERROR on page {page}: {exc}")
        break

    data = response.get("data", [])
    if not data:
        log("No more data — fetch complete.")
        break

    all_payments.extend(parse_payment(item) for item in data)
    page += 1
    total = len(all_payments)

    if page % CHECKPOINT_EVERY == 0:
        log(f"Page {page:>5} | {total:>9,} records | last timestamp: {data[-1]['state_changed_at']}")
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(all_payments, f, ensure_ascii=False)

    # Last page: fewer records than requested
    if len(data) < PER_PAGE:
        log(f"Last page reached ({len(data)} records on this page).")
        break

    # Advance cursor: use the last item of the current page.
    # Using data[-1] (not data[-2]) ensures no record is duplicated across pages.
    cursor      = data[-1]
    after_id    = cursor["id"]
    after_value = cursor.get("state_changed_at", "")

    time.sleep(0.05)  # be polite to the server

# ── Final save ────────────────────────────────────────────────────────────────

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(all_payments, f, ensure_ascii=False, indent=2)

log(f"\nDone! {len(all_payments):,} donations saved to {OUTPUT_FILE}")
