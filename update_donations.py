"""
Incremental update: fetches only new donations (newer than the most recent
record in the existing latwogang_wplaty.json) and prepends them to the file.

Usage:
    python3 update_donations.py [path/to/latwogang_wplaty.json]

Defaults to ../latwogang_wplaty.json (relative to this script).
After updating, run preprocess.py to regenerate dashboard data files.
"""
import urllib.request, urllib.parse, json, time, sys, os

script_dir = os.path.dirname(os.path.abspath(__file__))
DATA_FILE  = sys.argv[1] if len(sys.argv) > 1 else os.path.join(script_dir, "..", "latwogang_wplaty.json")
BASE_URL   = "https://www.siepomaga.pl/api/v1/payments"
PER_PAGE   = 200
HEADERS    = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer":    "https://www.siepomaga.pl/latwogang",
}

print("Loading existing data...", flush=True)
with open(DATA_FILE, encoding="utf-8") as f:
    existing = json.load(f)

existing_ids = {d["id"] for d in existing}
print(f"  Existing: {len(existing):,} records, newest: {existing[0]['data']}", flush=True)


def fetch_page(after_id=None, after_value=None):
    params = {"target_type": "Fundraise", "target_id": "LZSw1Ox",
              "sort_by": "newest", "per_page": str(PER_PAGE)}
    if after_id:    params["after_id"]    = after_id
    if after_value: params["after_value"] = after_value
    url = BASE_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def parse(item):
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


new_payments = []
after_id = after_value = None
page = 0

print("Fetching new donations...", flush=True)
while True:
    resp  = fetch_page(after_id, after_value)
    batch = resp.get("data", [])
    if not batch:
        print("No more data.", flush=True)
        break

    stop = False
    for item in batch:
        if item["id"] in existing_ids:
            stop = True
            break
        new_payments.append(parse(item))

    page += 1
    if page % 10 == 0:
        print(f"  Page {page} | {len(new_payments):,} new records | {batch[-1]['state_changed_at']}", flush=True)

    if stop or len(batch) < PER_PAGE:
        break

    cursor      = batch[-1]
    after_id    = cursor["id"]
    after_value = cursor.get("state_changed_at", "")
    time.sleep(0.05)

print(f"\nFetched {len(new_payments):,} new donations.", flush=True)

if new_payments:
    combined = new_payments + existing
    print(f"Saving {len(combined):,} total records...", flush=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False)
    print(f"Done. Run preprocess.py to regenerate dashboard data.", flush=True)
else:
    print("Nothing new to save.", flush=True)
