"""
Debug analysis: relaxed conditions to find near-miss stocks
Uses FinMind login API to get dynamic token (avoids IP binding)
"""
import os, datetime, requests, pandas as pd, numpy as np

FINMIND_USER = os.environ.get("FINMIND_USER", "tetsu")
FINMIND_PASSWORD = os.environ.get("FINMIND_PASSWORD", "")
API_URL = "https://api.finmindtrade.com/api/v4/data"
LOGIN_URL = "https://api.finmindtrade.com/api/v4/login"

def get_token():
    """Login to FinMind and get a dynamic token"""
    if not FINMIND_PASSWORD:
        print("ERROR: FINMIND_PASSWORD not set")
        return ""
    r = requests.post(LOGIN_URL, data={
        "user_id": FINMIND_USER,
        "password": FINMIND_PASSWORD
    }, timeout=30)
    data = r.json()
    if data.get("status") != 200:
        print(f"Login failed: {data}")
        return ""
    token = data.get("token", "")
    print(f"Login OK, token length={len(token)}")
    return token

TOKEN = get_token()

def _get(dataset, **kw):
    params = {"dataset": dataset, "token": TOKEN}
    params.update({k: v for k, v in kw.items() if v})
    r = requests.get(API_URL, params=params, timeout=60)
    data = r.json()
    if data.get("status") != 200 or not data.get("data"):
        print(f"  API: {dataset} -> status={data.get('status')}, msg={data.get('msg','')}")
        return pd.DataFrame()
    return pd.DataFrame(data["data"])

today = datetime.date.today()

# ========== Strategy 1: Revenue Momentum (Relaxed) ==========
print("=" * 60)
print("Strategy 1: Revenue Momentum (Relaxed Analysis)")
print("=" * 60)

rev_start = (today - datetime.timedelta(days=450)).strftime("%Y-%m-%d")
price_start = (today - datetime.timedelta(days=120)).strftime("%Y-%m-%d")

print(f"Fetching monthly revenue (start={rev_start})...")
revenue_df = _get("TaiwanStockMonthRevenue", start_date=rev_start)
print(f"Revenue data: {len(revenue_df)} rows")

if not revenue_df.empty:
    revenue_df["date"] = pd.to_datetime(revenue_df["date"])
    revenue_df["revenue"] = pd.to_numeric(revenue_df["revenue"], errors="coerce")
    revenue_df = revenue_df.sort_values(["stock_id", "date"])
    revenue_df["revenue_yoy"] = revenue_df.groupby("stock_id")["revenue"].pct_change(periods=12) * 100

    relaxed_rev = []
    for sid, grp in revenue_df.groupby("stock_id"):
        grp = grp.dropna(subset=["revenue_yoy"]).tail(3)
        if len(grp) < 3:
            continue
        yoy = grp["revenue_yoy"].values
        latest_rev = grp["revenue"].iloc[-1]
        accel = yoy[-1] - yoy[-2]
        pos_count = (grp["revenue_yoy"] > 0).sum()

        if pos_count >= 2 and latest_rev >= 300_000:
            relaxed_rev.append({
                "stock_id": sid,
                "rev_B": round(latest_rev / 1_000_000, 1),
                "yoy_m1": round(yoy[0], 1),
                "yoy_m2": round(yoy[1], 1),
                "yoy_m3": round(yoy[2], 1),
                "accel": round(accel, 1),
                "pos_months": int(pos_count),
            })

    print(f"\nRelaxed filter (>=2mo positive, rev>=3B): {len(relaxed_rev)} stocks")

    print(f"\nFetching prices (start={price_start})...")
    price_df = _get("TaiwanStockPrice", start_date=price_start)
    print(f"Price data: {len(price_df)} rows")

    if not price_df.empty and relaxed_rev:
        price_df["date"] = pd.to_datetime(price_df["date"])
        for col in ["close", "Trading_Volume", "Trading_money"]:
            if col in price_df.columns:
                price_df[col] = pd.to_numeric(price_df[col], errors="coerce")

        candidates = [x["stock_id"] for x in relaxed_rev]
        pdf = price_df[price_df["stock_id"].isin(candidates)].sort_values(["stock_id", "date"])
        pdf["ma20"] = pdf.groupby("stock_id")["close"].transform(lambda x: x.rolling(20).mean())
        pdf["ma60"] = pdf.groupby("stock_id")["close"].transform(lambda x: x.rolling(60).mean())

        rs_data = {}
        for sid in price_df["stock_id"].unique():
            sdf = price_df[price_df["stock_id"] == sid].sort_values("date").tail(61)
            if len(sdf) >= 61:
                rs_data[sid] = (sdf["close"].iloc[-1] / sdf["close"].iloc[0] - 1) * 100
        rs_series = pd.Series(rs_data)
        rs_rank = rs_series.rank(pct=True)

        final_s1 = []
        for info in relaxed_rev:
            sid = info["stock_id"]
            sdf = pdf[pdf["stock_id"] == sid].sort_values("date")
            if sdf.empty or len(sdf) < 60:
                continue
            latest = sdf.iloc[-1]
            if pd.isna(latest.get("ma20")) or pd.isna(latest.get("ma60")):
                continue

            bullish = latest["close"] > latest["ma20"] > latest["ma60"]
            rs = rs_data.get(sid, None)
            rs_pct = rs_rank.get(sid, 0) if sid in rs_rank.index else 0

            final_s1.append({
                **info,
                "close": round(latest["close"], 1),
                "ma20": round(latest["ma20"], 1),
                "ma60": round(latest["ma60"], 1),
                "bullish": "Y" if bullish else "N",
                "rs60": round(rs, 1) if rs else None,
                "rs_pct": round(rs_pct * 100, 0) if rs_pct else None,
            })

        if final_s1:
            df1 = pd.DataFrame(final_s1)
            strict = df1[(df1["pos_months"] == 3) & (df1["accel"] > 0) & (df1["rev_B"] >= 5) & (df1["bullish"] == "Y") & (df1["rs_pct"] >= 80)]
            print(f"\n=== STRICT pass: {len(strict)} stocks ===")
            if not strict.empty:
                print(strict.sort_values("rs60", ascending=False).to_string(index=False))

            relaxed1 = df1[(df1["pos_months"] == 3) & (df1["accel"] > 0) & (df1["rev_B"] >= 5) & (df1["bullish"] == "Y")]
            print(f"\n=== Relaxed RS (3mo+accel+rev5B+bullish): {len(relaxed1)} stocks ===")
            if not relaxed1.empty:
                print(relaxed1.sort_values("rs60", ascending=False).head(15).to_string(index=False))

            relaxed2 = df1[(df1["pos_months"] == 3) & (df1["rev_B"] >= 5) & (df1["bullish"] == "Y")]
            print(f"\n=== More relaxed (3mo+rev5B+bullish, no accel): {len(relaxed2)} stocks ===")
            if not relaxed2.empty:
                print(relaxed2.sort_values("rs60", ascending=False).head(15).to_string(index=False))

            relaxed3 = df1[(df1["pos_months"] >= 2) & (df1["rev_B"] >= 3) & (df1["bullish"] == "Y")]
            print(f"\n=== Widest (>=2mo+rev3B+bullish): {len(relaxed3)} stocks ===")
            if not relaxed3.empty:
                print(relaxed3.sort_values("rs60", ascending=False).head(20).to_string(index=False))

print("\n")
# ========== Strategy 2: Institutional Chip (Relaxed) ==========
print("=" * 60)
print("Strategy 2: Institutional Chip (Relaxed Analysis)")
print("=" * 60)

inst_start = (today - datetime.timedelta(days=35)).strftime("%Y-%m-%d")
print(f"Fetching institutional data (start={inst_start})...")
inst_df = _get("TaiwanStockInstitutionalInvestorsBuySell", start_date=inst_start)
print(f"Institutional data: {len(inst_df)} rows")

if not inst_df.empty:
    inst_df["date"] = pd.to_datetime(inst_df["date"])
    inst_df["buy"] = pd.to_numeric(inst_df["buy"], errors="coerce")
    inst_df["sell"] = pd.to_numeric(inst_df["sell"], errors="coerce")
    inst_df["net"] = inst_df["buy"] - inst_df["sell"]
    inst_df = inst_df.sort_values(["stock_id", "name", "date"])

    trust_df = inst_df[inst_df["name"] == "Investment_Trust"]
    foreign_df = inst_df[inst_df["name"] == "Foreign_Investor"]

    chip_results = []
    for sid in trust_df["stock_id"].unique():
        t = trust_df[trust_df["stock_id"] == sid].sort_values("date")
        t5 = t.tail(5)
        trust_5d = t5["net"].sum() / 1000
        t20 = t.tail(20)
        trust_20d = t20["net"].sum() / 1000
        buy_days = (t20["net"] > 0).sum()

        f = foreign_df[foreign_df["stock_id"] == sid].sort_values("date").tail(20)
        foreign_20d = f["net"].sum() / 1000 if not f.empty else 0

        if trust_20d >= 500:
            chip_results.append({
                "stock_id": sid,
                "trust_5d": round(trust_5d),
                "trust_20d": round(trust_20d),
                "buy_days": int(buy_days),
                "foreign_20d": round(foreign_20d),
                "pass_5d": "Y" if trust_5d >= 500 else "N",
                "pass_20d": "Y" if trust_20d >= 1500 else "N",
                "pass_days": "Y" if buy_days >= 10 else "N",
                "pass_foreign": "Y" if foreign_20d > 0 else "N",
            })

    print(f"\nRelaxed chip filter (trust_20d>=500): {len(chip_results)} stocks")

    if chip_results and not price_df.empty:
        chip_candidates = [x["stock_id"] for x in chip_results]
        cpdf = price_df[price_df["stock_id"].isin(chip_candidates)].sort_values(["stock_id", "date"])
        cpdf["ma20"] = cpdf.groupby("stock_id")["close"].transform(lambda x: x.rolling(20).mean())
        cpdf["ma60"] = cpdf.groupby("stock_id")["close"].transform(lambda x: x.rolling(60).mean())

        final_s2 = []
        for info in chip_results:
            sid = info["stock_id"]
            sdf = cpdf[cpdf["stock_id"] == sid].sort_values("date")
            if len(sdf) < 60:
                continue
            latest = sdf.iloc[-1]
            if pd.isna(latest.get("ma20")) or pd.isna(latest.get("ma60")):
                continue

            bullish = latest["close"] > latest["ma20"] > latest["ma60"]
            s20 = sdf.tail(21)
            ret_20d = (s20["close"].iloc[-1] / s20["close"].iloc[0] - 1) * 100 if len(s20) >= 21 else None
            avg_money = sdf.tail(20)["Trading_money"].mean()

            final_s2.append({
                **info,
                "close": round(latest["close"], 1),
                "bullish": "Y" if bullish else "N",
                "ret20d": round(ret_20d, 1) if ret_20d else None,
                "avg_money_M": round(avg_money / 1e6, 0) if avg_money else None,
            })

        if final_s2:
            df2 = pd.DataFrame(final_s2)

            strict2 = df2[(df2["pass_5d"]=="Y") & (df2["pass_20d"]=="Y") & (df2["pass_days"]=="Y") & (df2["pass_foreign"]=="Y") & (df2["bullish"]=="Y")]
            if not strict2.empty:
                strict2b = strict2[(strict2["ret20d"] >= 3) & (strict2["ret20d"] <= 30) & (strict2["avg_money_M"] >= 50)]
                print(f"\n=== STRICT pass: {len(strict2b)} stocks ===")
                if not strict2b.empty:
                    print(strict2b.sort_values("trust_20d", ascending=False).to_string(index=False))

            relaxed_s2 = df2[(df2["trust_20d"] >= 500) & (df2["bullish"] == "Y")].sort_values("trust_20d", ascending=False)
            print(f"\n=== Relaxed (trust_20d>=500+bullish): {len(relaxed_s2)} stocks ===")
            if not relaxed_s2.empty:
                print(relaxed_s2.head(20).to_string(index=False))

print("\nDone")
