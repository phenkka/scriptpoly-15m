import json, statistics

rows = [json.loads(l) for l in open("/opt/scriptpoly/data/trades_success.jsonl")]
print("=== ALL trades: poly_book_age distribution ===")
ages = [r.get("book_freshness", {}).get("poly_book_age_at_submit_ms") for r in rows]
ages = sorted([a for a in ages if a is not None])
if ages:
    p90 = ages[int(len(ages) * 0.9)]
    p99 = ages[int(len(ages) * 0.99)]
    print("count=%d min=%.0f max=%.0f median=%.0f p90=%.0f p99=%.0f" % (
        len(ages), min(ages), max(ages), statistics.median(ages), p90, p99))

def get_pnl(r):
    return r.get("net_pnl") or r.get("summary", {}).get("net_pnl") or 0

print()
print("=== LOSING trades (pnl < -1): poly_book_age ===")
bad = [r for r in rows if get_pnl(r) < -1]
print("found=%d" % len(bad))
for r in bad:
    pnl = get_pnl(r)
    age = r.get("book_freshness", {}).get("poly_book_age_at_submit_ms") or 0
    pred_age = r.get("book_freshness", {}).get("pred_book_age_at_submit_ms") or 0
    label = r.get("summary", {}).get("label") or r.get("label", "")
    print("pnl=$%.2f  poly_age=%.0fms  pred_age=%.0fms  %s  %s" % (pnl, age, pred_age, r["ts"][:19], label))

print()
print("=== WINNING trades: poly_book_age ===")
good = [r for r in rows if get_pnl(r) > 0]
print("found=%d" % len(good))
good_ages = sorted([r.get("book_freshness", {}).get("poly_book_age_at_submit_ms") or 0 for r in good])
if good_ages:
    print("min=%.0f max=%.0f median=%.0f" % (min(good_ages), max(good_ages), statistics.median(good_ages)))
