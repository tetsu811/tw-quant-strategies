"""
Taiwan Stock Quantitative Strategy - Daily Picks
Strategies:
  1. Revenue Momentum: YoY growth + acceleration + MA alignment
  2. Chip Momentum: Investment trust net buy + foreign alignment + price momentum
Uses FinMind API V4 with dynamic login token.
"""
import os, sys, datetime, requests, pandas as pd, numpy as np, json

FINMIND_USER = os.environ.get("FINMIND_USER", "")
FINMIND_PASSWORD = os.environ.get("FINMIND_PASSWORD", "")
API_URL = "https://api.finmindtrade.com/api/v4/data"
LOGIN_URL = "https://api.finmindtrade.com/api/v4/login"

# --------------- helpers ---------------

def get_token():
    if not FINMIND_PASSWORD:
        print("ERROR: FINMIND_PASSWORD not set"); return ""
    r = requests.post(LOGIN_URL, data={
        "user_id": FINMIND_USER,
        "password": FINMIND_PASSWORD
    }, timeout=30)
    d = r.json()
    if d.get("status") != 200:
        print(f"Login failed: {d}"); return ""
    tok = d.get("token", "")
    print(f"Login OK, token length={len(tok)}")
    return tok

TOKEN = get_token()
if not TOKEN:
    print("Cannot proceed without token"); sys.exit(1)

def api_get(dataset, **kw):
    params = {"dataset": dataset, "token": TOKEN}
    params.update({k: v for k, v in kw.items() if v})
    r = requests.get(API_URL, params=params, timeout=60)
    d = r.json()
    if d.get("status") != 200 or not d.get("data"):
        return pd.DataFrame()
    return pd.DataFrame(d["data"])

today = datetime.date.today()

# --------------- Step 1: Institutional Chip Data ---------------
print("=" * 60)
print("Step 1: Fetching Institutional Data (35 days)")
print("=" * 60)

chip_start = (today - datetime.timedelta(days=35)).strftime("%Y-%m-%d")
chip_df = api_get("TaiwanStockInstitutionalInvestorsBuySell", start_date=chip_start)
print(f"Institutional rows: {len(chip_df)}")

if chip_df.empty:
    print("No institutional data. Exiting."); sys.exit(1)

chip_df["buy"] = pd.to_numeric(chip_df["buy"], errors="coerce")
chip_df["sell"] = pd.to_numeric(chip_df["sell"], errors="coerce")
chip_df["net"] = chip_df["buy"] - chip_df["sell"]

# Investment trust aggregation
trust_df = chip_df[chip_df["name"].str.contains("Investment_Trust", na=False)]
trust_agg = trust_df.groupby("stock_id")["net"].sum().reset_index()
trust_agg.columns = ["stock_id", "trust_net"]

# Foreign investor aggregation
foreign_df = chip_df[chip_df["name"] == "Foreign_Investor"]
foreign_agg = foreign_df.groupby("stock_id")["net"].sum().reset_index()
foreign_agg.columns = ["stock_id", "foreign_net"]

merged = trust_agg.merge(foreign_agg, on="stock_id", how="left")
merged["foreign_net"] = merged["foreign_net"].fillna(0)

# Filter: trust net buy > 500 shares
candidates = merged[merged["trust_net"] >= 500].copy()
candidates = candidates.sort_values("trust_net", ascending=False).head(50)
print(f"Chip candidates (trust>=500): {len(candidates)}")

stock_ids = candidates["stock_id"].tolist()

# --------------- Step 2: Stock Info (names) ---------------
print("\nStep 2: Fetching stock info...")
info_df = api_get("TaiwanStockInfo")
name_map = {}
if not info_df.empty:
    for _, row in info_df.iterrows():
        name_map[row["stock_id"]] = row.get("stock_name", "")
print(f"Stock names loaded: {len(name_map)}")

# --------------- Step 3: Price Data per stock ---------------
print("\nStep 3: Fetching price data for candidates...")
price_start = (today - datetime.timedelta(days=120)).strftime("%Y-%m-%d")
price_map = {}
for sid in stock_ids:
    pdf = api_get("TaiwanStockPrice", data_id=sid, start_date=price_start)
    if not pdf.empty and len(pdf) >= 5:
        pdf["close"] = pd.to_numeric(pdf["close"], errors="coerce")
        pdf = pdf.sort_values("date")
        price_map[sid] = pdf
print(f"Price data fetched for {len(price_map)} stocks")

# --------------- Step 4: Revenue Data per stock ---------------
print("\nStep 4: Fetching revenue data for candidates...")
rev_start = (today - datetime.timedelta(days=450)).strftime("%Y-%m-%d")
rev_map = {}
for sid in stock_ids:
    rdf = api_get("TaiwanStockMonthRevenue", data_id=sid, start_date=rev_start)
    if not rdf.empty and len(rdf) >= 13:
        rdf["revenue"] = pd.to_numeric(rdf["revenue"], errors="coerce")
        rdf = rdf.sort_values("date")
        rev_map[sid] = rdf
print(f"Revenue data fetched for {len(rev_map)} stocks")

# --------------- Step 5: Score & Rank ---------------
print("\n" + "=" * 60)
print("Step 5: Scoring candidates")
print("=" * 60)

results = []
for _, row in candidates.iterrows():
    sid = row["stock_id"]
    trust_net = int(row["trust_net"])
    foreign_net = int(row["foreign_net"])
    score = 0

    # --- Chip score ---
    score += min(trust_net / 1_000_000, 10)
    if foreign_net > 0:
        score += 3

    # --- Price score ---
    price = None
    price_chg = None
    above_ma20 = False
    above_ma60 = False
    if sid in price_map:
        pdf = price_map[sid]
        latest_close = pdf["close"].iloc[-1]
        earliest_close = pdf["close"].iloc[0]
        price = latest_close
        price_chg = round((latest_close / earliest_close - 1) * 100, 1) if earliest_close > 0 else 0
        ma20 = pdf["close"].tail(20).mean()
        ma60 = pdf["close"].tail(60).mean()
        above_ma20 = latest_close > ma20
        above_ma60 = latest_close > ma60
        if above_ma20: score += 2
        if above_ma60: score += 2
        if price_chg and price_chg > 10: score += 2
        if price_chg and price_chg > 20: score += 1

    # --- Revenue score ---
    rev_yoys = []
    rev_str = ""
    latest_rev = None
    if sid in rev_map:
        rdf = rev_map[sid]
        rows_list = rdf.to_dict("records")
        recent3 = rows_list[-3:]
        for r in recent3:
            r_date = pd.Timestamp(r["date"])
            match = [prev for prev in rows_list
                     if pd.Timestamp(prev["date"]).year == r_date.year - 1
                     and pd.Timestamp(prev["date"]).month == r_date.month]
            if match and match[0]["revenue"] > 0:
                yoy = (r["revenue"] - match[0]["revenue"]) / match[0]["revenue"] * 100
                rev_yoys.append(round(yoy, 1))
        pos_count = sum(1 for y in rev_yoys if y > 0)
        if pos_count >= 2: score += 3
        if pos_count >= 3: score += 2
        latest_rev = rows_list[-1]["revenue"]
        rev_str = f"{latest_rev / 1e8:.1f}億"

    results.append({
        "stock_id": sid,
        "name": name_map.get(sid, ""),
        "score": round(score, 1),
        "trust_net": trust_net,
        "foreign_net": foreign_net,
        "price": price,
        "price_chg": price_chg,
        "above_ma20": above_ma20,
        "above_ma60": above_ma60,
        "rev_yoys": rev_yoys,
        "rev_str": rev_str,
    })

results.sort(key=lambda x: x["score"], reverse=True)

# --------------- Step 6: Categorize into Tiers ---------------
top_tier = [r for r in results if r["score"] >= 14][:5]
second_tier = [r for r in results if 11 <= r["score"] < 14][:5]
third_tier = [r for r in results if 8 <= r["score"] < 11][:5]

print(f"\nTop Tier: {len(top_tier)}")
for r in top_tier:
    print(f"  {r['stock_id']} {r['name']} score={r['score']}")
print(f"Second Tier: {len(second_tier)}")
for r in second_tier:
    print(f"  {r['stock_id']} {r['name']} score={r['score']}")
print(f"Third Tier: {len(third_tier)}")
for r in third_tier:
    print(f"  {r['stock_id']} {r['name']} score={r['score']}")

# If all tiers empty, try lower thresholds
if not top_tier and not second_tier and not third_tier:
    print("No candidates at normal thresholds, using top 10 by score...")
    top_tier = results[:3]
    second_tier = results[3:7]
    third_tier = results[7:10]

# --------------- Step 7: Format & Push to LINE ---------------
def fmt_stock(r):
    ma = ""
    if r["above_ma20"] and r["above_ma60"]:
        ma = "📈MA20+60✓"
    elif r["above_ma20"]:
        ma = "📊 MA20✓"
    elif r["above_ma60"]:
        ma = "📊 MA60✓"
    yoy_str = ",".join(f"{y:+.1f}%" for y in r["rev_yoys"]) if r["rev_yoys"] else "N/A"
    price_str = f"${r['price']:.0f}" if r['price'] else "?"
    chg_str = f"({r['price_chg']:+.1f}%)" if r['price_chg'] is not None else ""
    trust_str = f"投信{r['trust_net']:+,}"
    foreign_str = f"外資{r['foreign_net']:+,}"
    return (f"▪ {r['stock_id']} {r['name']}  {price_str}{chg_str}\n"
            f"  {trust_str} / {foreign_str}\n"
            f"  營收YoY: {yoy_str}  {ma}")

date_str = today.strftime("%Y/%m/%d")
lines = [f"🇹🇼 台股量化選股 {date_str}\n"]

if top_tier:
    lines.append("🏆 Top Tier（強烈觀察）")
    for r in top_tier:
        lines.append(fmt_stock(r))
    lines.append("")

if second_tier:
    lines.append("⭐ Second Tier（値得追蹤）")
    for r in second_tier:
        lines.append(fmt_stock(r))
    lines.append("")

if third_tier:
    lines.append("📊 Third Tier（穩健型）")
    for r in third_tier:
        lines.append(fmt_stock(r))
    lines.append("")

lines.append("⚠️ 以上為量化篩選結果，非投資建議")
message = "\n".join(lines)

print("\n" + "=" * 60)
print("LINE Message Preview:")
print("=" * 60)
print(message)

# Push to LINE
from line_push import push_line_message
push_line_message(message)
print("\nDone!")
