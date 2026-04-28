"""
preprocess.py — agregacja danych z latwogang_wplaty.json
Generuje katalog data/ z plikami JSON używanymi przez dashboard.

Użycie:
    python3 preprocess.py

Oczekuje pliku latwogang_wplaty.json w bieżącym katalogu (lub w katalogu nadrzędnym).
Generuje pliki w katalogu data/ (tworzy go jeśli nie istnieje).
"""

import json
import os
import sys
import random
import hashlib
from collections import defaultdict
from datetime import datetime, timedelta

# ── Konfiguracja ──────────────────────────────────────────────────────────────
INPUT_FILE  = os.path.join(os.path.dirname(__file__), "..", "latwogang_wplaty.json")
OUTPUT_DIR  = os.path.join(os.path.dirname(__file__), "data")
TOP_N_TREEMAP   = 2000   # ile top osób pokazać w treemapie
TOP_N_DETAILS   = 500    # ile top osób z pełną listą wpłat (klik w treemapę)
MAX_PAYS_DETAIL = 100    # max wpłat per osoba w person_details
TOP_N_DONORS    = 50     # ile top donors z pełnymi wpłatami
MAX_PAYS_DONOR  = 50     # max wpłat per donor w top_donors
COMMENTS_SAMPLE = 500    # ile losowych komentarzy

os.makedirs(OUTPUT_DIR, exist_ok=True)

def out(name):
    return os.path.join(OUTPUT_DIR, name)

def log(msg):
    print(msg, flush=True)

# ── Wczytaj dane ──────────────────────────────────────────────────────────────
log("Wczytuję latwogang_wplaty.json...")
with open(INPUT_FILE, encoding="utf-8") as f:
    data = json.load(f)
log(f"  Załadowano {len(data):,} rekordów.")

# ── 1. STATS ──────────────────────────────────────────────────────────────────
log("Generuję stats.json...")
total_amount   = sum(float(d["kwota"]) for d in data if d["kwota"])
named_count    = sum(1 for d in data if d["osoba"])
anon_count     = sum(1 for d in data if not d["osoba"])
comment_count  = sum(1 for d in data if d["komentarz"])
company_count  = sum(1 for d in data if d["firma"])
unique_persons = len(set(d["osoba"] for d in data if d["osoba"]))

stats = {
    "total_payments": len(data),
    "total_amount_pln": round(total_amount, 2),
    "named_count": named_count,
    "anon_count": anon_count,
    "comment_count": comment_count,
    "company_count": company_count,
    "unique_named_persons": unique_persons,
    "date_first": data[-1]["data"],
    "date_last":  data[0]["data"],
}
with open(out("stats.json"), "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False, indent=2)
log(f"  stats.json OK — total: {total_amount:,.0f} PLN")

# ── 2. TREEMAP ────────────────────────────────────────────────────────────────
log(f"Generuję treemap.json (top {TOP_N_TREEMAP} osób)...")

person_total        = defaultdict(float)
person_count        = defaultdict(int)   # all payments (incl. null kwota)
person_amount_count = defaultdict(int)   # only payments with a known amount
anon_total    = 0.0
anon_payments = 0

for d in data:
    amt_raw = d["kwota"]
    amt = float(amt_raw) if amt_raw is not None else None
    if d["osoba"]:
        if amt is not None:
            person_total[d["osoba"]] += amt
            person_amount_count[d["osoba"]] += 1
        person_count[d["osoba"]] += 1
    else:
        anon_total    += amt if amt is not None else 0.0
        anon_payments += 1

# Sortuj wg sumy, weź top N
sorted_persons = sorted(person_total.items(), key=lambda x: x[1], reverse=True)
top_persons = sorted_persons[:TOP_N_TREEMAP]

treemap_items = [
    {
        "name":   name,
        "value":  round(total, 2),
        "count":  person_count[name],
        "avg":    round(total / person_amount_count[name], 2) if person_amount_count[name] else None,
    }
    for name, total in top_persons
]

# Osoby poza top N — agregat "Pozostałe"
other_total = sum(t for _, t in sorted_persons[TOP_N_TREEMAP:])
other_count = sum(person_count[n] for n, _ in sorted_persons[TOP_N_TREEMAP:])

treemap_data = {
    "top": treemap_items,
    "anon":  {"name": "Anonim",     "value": round(anon_total, 2),  "count": anon_payments},
    "other": {"name": "Pozostałe",  "value": round(other_total, 2), "count": other_count,
               "persons": len(sorted_persons) - TOP_N_TREEMAP},
}
with open(out("treemap.json"), "w", encoding="utf-8") as f:
    json.dump(treemap_data, f, ensure_ascii=False)
log(f"  treemap.json OK — {len(treemap_items)} osób")

# ── 3. TIMELINE HOURLY ────────────────────────────────────────────────────────
log("Generuję timeline_hourly.json...")

hourly_count  = defaultdict(int)
hourly_amount = defaultdict(float)
for d in data:
    if not d["data"]:
        continue
    h = d["data"][:13]  # "2026-04-27T22"
    amt = float(d["kwota"]) if d["kwota"] is not None else 0.0
    hourly_count[h]  += 1
    hourly_amount[h] += amt

# Zero-fill: generate all hours between first and last observed hour
h_min = min(hourly_count.keys())
h_max = max(hourly_count.keys())
h_cur = datetime.fromisoformat(h_min)
h_end = datetime.fromisoformat(h_max)
hourly = []
while h_cur <= h_end:
    hourly.append(h_cur.strftime("%Y-%m-%dT%H"))
    h_cur += timedelta(hours=1)

# Cumulative
cumulative = 0.0
cum_by_hour = {}
for h in hourly:
    cumulative += hourly_amount[h]
    cum_by_hour[h] = round(cumulative, 2)

timeline_hourly = [
    {
        "hour":       h,
        "count":      hourly_count[h],
        "amount":     round(hourly_amount[h], 2),
        "cumulative": cum_by_hour[h],
    }
    for h in hourly
]
with open(out("timeline_hourly.json"), "w", encoding="utf-8") as f:
    json.dump(timeline_hourly, f, ensure_ascii=False)
log(f"  timeline_hourly.json OK — {len(timeline_hourly)} godzin")

# ── 4. TIMELINE MINUTELY ──────────────────────────────────────────────────────
log("Generuję timeline_minutely.json...")

minutely_count  = defaultdict(int)
minutely_amount = defaultdict(float)
for d in data:
    if not d["data"]:
        continue
    m = d["data"][:16]  # "2026-04-27T22:31"
    amt = float(d["kwota"]) if d["kwota"] is not None else 0.0
    minutely_count[m]  += 1
    minutely_amount[m] += amt

# Zero-fill: generate all minutes between first and last observed minute
m_min = min(minutely_count.keys())
m_max = max(minutely_count.keys())
m_cur = datetime.fromisoformat(m_min)
m_end = datetime.fromisoformat(m_max)
all_minutes = []
while m_cur <= m_end:
    all_minutes.append(m_cur.strftime("%Y-%m-%dT%H:%M"))
    m_cur += timedelta(minutes=1)

cumulative = 0.0
timeline_minutely = []
for m in all_minutes:
    cumulative += minutely_amount[m]
    timeline_minutely.append({
        "minute":     m,
        "count":      minutely_count[m],
        "amount":     round(minutely_amount[m], 2),
        "cumulative": round(cumulative, 2),
    })
with open(out("timeline_minutely.json"), "w", encoding="utf-8") as f:
    json.dump(timeline_minutely, f, ensure_ascii=False)
log(f"  timeline_minutely.json OK — {len(timeline_minutely)} minut")

# ── 5. DISTRIBUTION ───────────────────────────────────────────────────────────
log("Generuję distribution.json...")

buckets = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 5000, 100000]
bucket_labels = ["1", "2", "5", "10", "20", "50", "100", "200", "500", "1 000", "5 000", "5 000+"]
counts = [0] * len(buckets)

for d in data:
    if not d["kwota"]:
        continue
    amt = float(d["kwota"])
    placed = False
    for i, b in enumerate(buckets):
        if amt <= b:
            counts[i] += 1
            placed = True
            break
    if not placed:
        counts[-1] += 1

distribution = [
    {"label": bucket_labels[i], "count": counts[i], "max_value": buckets[i]}
    for i in range(len(buckets))
]
with open(out("distribution.json"), "w", encoding="utf-8") as f:
    json.dump(distribution, f, ensure_ascii=False)
log("  distribution.json OK")

# ── 6. HEATMAP (dzień × godzina) ─────────────────────────────────────────────
log("Generuję heatmap.json...")

heatmap_count  = defaultdict(int)
heatmap_amount = defaultdict(float)
days_set = set()

for d in data:
    if not d["data"]:
        continue
    dt_str = d["data"][:16]
    try:
        dt = datetime.fromisoformat(d["data"][:19])
    except Exception:
        continue
    day  = dt.strftime("%Y-%m-%d")
    hour = dt.hour
    amt  = float(d["kwota"]) if d["kwota"] else 0.0
    heatmap_count[(day, hour)]  += 1
    heatmap_amount[(day, hour)] += amt
    days_set.add(day)

days = sorted(days_set)
heatmap_rows = []
for day in days:
    for hour in range(24):
        key = (day, hour)
        heatmap_rows.append({
            "day":    day,
            "hour":   hour,
            "count":  heatmap_count.get(key, 0),
            "amount": round(heatmap_amount.get(key, 0.0), 2),
        })

heatmap = {"days": days, "data": heatmap_rows}
with open(out("heatmap.json"), "w", encoding="utf-8") as f:
    json.dump(heatmap, f, ensure_ascii=False)
log(f"  heatmap.json OK — {len(days)} dni")

# ── 7. TOP DONORS ─────────────────────────────────────────────────────────────
log(f"Generuję top_donors.json (top {TOP_N_DONORS})...")

# Zbierz wpłaty per osoba (top N)
top_donor_names = {name for name, _ in sorted_persons[:TOP_N_DONORS]}
donor_payments = defaultdict(list)

for d in data:
    if d["osoba"] and d["osoba"] in top_donor_names:
        donor_payments[d["osoba"]].append({
            "id":        d["id"],
            "kwota":     d["kwota"],
            "komentarz": d["komentarz"],
            "data":      d["data"],
        })

top_donors = []
for name, total in sorted_persons[:TOP_N_DONORS]:
    pays = sorted(donor_payments[name], key=lambda x: x["data"] or "", reverse=True)
    top_donors.append({
        "name":     name,
        "total":    round(total, 2),
        "count":    person_count[name],
        "avg":      round(total / person_amount_count[name], 2) if person_amount_count[name] else None,
        "payments": pays[:MAX_PAYS_DONOR],
    })

with open(out("top_donors.json"), "w", encoding="utf-8") as f:
    json.dump(top_donors, f, ensure_ascii=False)
log(f"  top_donors.json OK")

# ── 8. PERSON DETAILS (top 2000) ──────────────────────────────────────────────
log(f"Generuję person_details.json (top {TOP_N_DETAILS} osób, max {MAX_PAYS_DETAIL} wpłat)...")

top_treemap_names = {name for name, _ in sorted_persons[:TOP_N_DETAILS]}
person_payments   = defaultdict(list)

for d in data:
    if d["osoba"] and d["osoba"] in top_treemap_names:
        person_payments[d["osoba"]].append({
            "id":        d["id"],
            "kwota":     d["kwota"],
            "komentarz": d["komentarz"],
            "data":      d["data"],
        })

person_details = {}
for name, total in sorted_persons[:TOP_N_DETAILS]:
    pays = sorted(person_payments[name], key=lambda x: x["data"] or "", reverse=True)
    person_details[name] = {
        "total":    round(total, 2),
        "count":    person_count[name],
        "avg":      round(total / person_amount_count[name], 2) if person_amount_count[name] else None,
        "payments": pays[:MAX_PAYS_DETAIL],
    }

with open(out("person_details.json"), "w", encoding="utf-8") as f:
    json.dump(person_details, f, ensure_ascii=False)
log("  person_details.json OK")

# ── 9. COMMENTS SAMPLE ────────────────────────────────────────────────────────
log(f"Generuję comments_sample.json ({COMMENTS_SAMPLE} komentarzy)...")

with_comments = [
    {
        "osoba":     d["osoba"] or "Anonim",
        "komentarz": d["komentarz"],
        "kwota":     d["kwota"],
        "data":      d["data"],
    }
    for d in data if d["komentarz"]
]
random.seed(42)
sample = random.sample(with_comments, min(COMMENTS_SAMPLE, len(with_comments)))
# Posortuj wg kwoty malejąco by pokazać ciekawe wpłaty
sample.sort(key=lambda x: float(x["kwota"]) if x["kwota"] else 0, reverse=True)

with open(out("comments_sample.json"), "w", encoding="utf-8") as f:
    json.dump(sample, f, ensure_ascii=False, indent=2)
log(f"  comments_sample.json OK — {len(sample)} komentarzy")

# ── 10. INLINE DATA INTO index.html ──────────────────────────────────────────
log("\nWbudowuję dane do index.html...")

DATA_FILES = [
    "stats.json",
    "treemap.json",
    "timeline_hourly.json",
    "distribution.json",
    "heatmap.json",
    "top_donors.json",
    "comments_sample.json",
    "person_details.json",
    "timeline_minutely.json",
]

inline_data = {}
for fname in DATA_FILES:
    fpath = out(fname)
    with open(fpath, encoding="utf-8") as f:
        inline_data[f"data/{fname}"] = json.load(f)

# Include top_companies.json if present (manually curated, not generated above)
companies_path = os.path.join(OUTPUT_DIR, "top_companies.json")
if os.path.exists(companies_path):
    with open(companies_path, encoding="utf-8") as f:
        inline_data["data/top_companies.json"] = json.load(f)
    log("  top_companies.json included in inline data")

inline_json = json.dumps(inline_data, ensure_ascii=False)
inline_script = f"<script>window.__INLINE__={inline_json};</script>"

index_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
with open(index_path, encoding="utf-8") as f:
    html = f.read()

import re
html_new = re.sub(
    r'<!-- INLINE_DATA_START -->.*?<!-- INLINE_DATA_END -->',
    f'<!-- INLINE_DATA_START -->{inline_script}<!-- INLINE_DATA_END -->',
    html,
    flags=re.DOTALL,
)
with open(index_path, "w", encoding="utf-8") as f:
    f.write(html_new)

size_kb = len(inline_json.encode()) / 1024
log(f"  index.html zaktualizowany (dane inline: {size_kb:.0f} KB)")

# ── Podsumowanie ──────────────────────────────────────────────────────────────
log("\n✅ Preprocessing zakończony!")
log(f"   Pliki zapisane w: {OUTPUT_DIR}")
for fname in sorted(os.listdir(OUTPUT_DIR)):
    size = os.path.getsize(os.path.join(OUTPUT_DIR, fname))
    log(f"   {fname:35s} {size/1024:8.1f} KB")
