"""
策略 2：投信認養 + 籌碼動能
"""
import os, datetime, requests
import pandas as pd, numpy as np

FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "")
API_URL = "https://api.finmindtrade.com/api/v4/data"
HEADERS = {"Authorization": f"Bearer {FINMIND_TOKEN}"}

def _get(dataset, data_id="", start_date="", end_date="", **kw):
    params = {"dataset": dataset, "data_id": data_id, "start_date": start_date, "end_date": end_date}
    params.update(kw)
    params = {k: v for k, v in params.items() if v}
    r = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != 200 or not data.get("data"):
        return pd.DataFrame()
    return pd.DataFrame(data["data"])

def get_institutional_data(start_date):
    df = _get("TaiwanStockInstitutionalInvestorsBuySell", start_date=start_date)
    if df.empty: return df
    df["date"] = pd.to_datetime(df["date"])
    df["buy"] = pd.to_numeric(df["buy"], errors="coerce")
    df["sell"] = pd.to_numeric(df["sell"], errors="coerce")
    df["net"] = df["buy"] - df["sell"]
    return df

def get_price_data(start_date):
    df = _get("TaiwanStockPrice", start_date=start_date)
    if df.empty: return df
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open","max","min","close","Trading_Volume","Trading_money"]:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def get_revenue_data(start_date):
    df = _get("TaiwanStockMonthRevenue", start_date=start_date)
    if df.empty: return df
    df["date"] = pd.to_datetime(df["date"])
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    return df

def screen_chip(inst_df):
    if inst_df.empty: return pd.DataFrame(columns=["stock_id"])
    df = inst_df.copy().sort_values(["stock_id","name","date"])
    trust_df = df[df["name"]=="Investment_Trust"]
    foreign_df = df[df["name"]=="Foreign_Investor"]
    results = []
    for sid in trust_df["stock_id"].unique():
        t = trust_df[trust_df["stock_id"]==sid].sort_values("date")
        trust_5d = t.tail(5)["net"].sum()/1000
        if trust_5d < 500: continue
        t20 = t.tail(20)
        trust_20d = t20["net"].sum()/1000
        if trust_20d < 1500: continue
        buy_days = (t20["net"]>0).sum()
        if buy_days < 10: continue
        f = foreign_df[foreign_df["stock_id"]==sid].sort_values("date").tail(20)
        foreign_20d = f["net"].sum()/1000 if not f.empty else 0
        if foreign_20d <= 0: continue
        results.append({"stock_id":sid,"trust_5d":round(trust_5d),"trust_20d":round(trust_20d),"trust_buy_days":int(buy_days),"foreign_20d":round(foreign_20d)})
    return pd.DataFrame(results)

def screen_price_chip(price_df, candidates):
    if price_df.empty: return pd.DataFrame()
    df = price_df[price_df["stock_id"].isin(candidates)].copy().sort_values(["stock_id","date"])
    df["ma20"] = df.groupby("stock_id")["close"].transform(lambda x: x.rolling(20).mean())
    df["ma60"] = df.groupby("stock_id")["close"].transform(lambda x: x.rolling(60).mean())
    results = []
    for sid in candidates:
        sdf = df[df["stock_id"]==sid].sort_values("date")
        if len(sdf)<60: continue
        latest = sdf.iloc[-1]
        if not (latest["close"]>latest["ma20"]>latest["ma60"]): continue
        s20 = sdf.tail(21)
        if len(s20)<21: continue
        ret_20d = (s20["close"].iloc[-1]/s20["close"].iloc[0]-1)*100
        if not (3<=ret_20d<=30): continue
        avg_money = sdf.tail(20)["Trading_money"].mean()
        if avg_money < 50000000: continue
        results.append({"stock_id":sid,"close":round(latest["close"],2),"ma20":round(latest["ma20"],2),"ma60":round(latest["ma60"],2),"ret_20d":round(ret_20d,2),"avg_daily_money_m":round(avg_money/1e6,1)})
    return pd.DataFrame(results)

def screen_revenue(revenue_df, candidates):
    if revenue_df.empty: return candidates
    df = revenue_df[revenue_df["stock_id"].isin(candidates)].copy().sort_values(["stock_id","date"])
    passed = []
    for sid in candidates:
        grp = df[df["stock_id"]==sid].sort_values("date")
        if len(grp)<13: passed.append(sid); continue
        latest = grp.iloc[-1]["revenue"]
        year_ago = grp.iloc[-13]["revenue"]
        if year_ago and year_ago>0:
            yoy = (latest/year_ago-1)*100
            if yoy < -10: continue
        passed.append(sid)
    return passed

def run():
    today = datetime.date.today()
    inst_start = (today-datetime.timedelta(days=35)).strftime("%Y-%m-%d")
    price_start = (today-datetime.timedelta(days=120)).strftime("%Y-%m-%d")
    rev_start = (today-datetime.timedelta(days=450)).strftime("%Y-%m-%d")
    print("[策略2] 抓取三大法人資料...")
    inst_df = get_institutional_data(inst_start)
    print(f"[策略2] 法人資料：{len(inst_df)} 筆")
    chip_picks = screen_chip(inst_df)
    print(f"[策略2] 籌碼篩選通過：{len(chip_picks)} 檔")
    if chip_picks.empty:
        print("[策略2] 無符合籌碼條件的標的"); return pd.DataFrame()
    print("[策略2] 抓取股價資料...")
    price_df = get_price_data(price_start)
    price_picks = screen_price_chip(price_df, chip_picks["stock_id"].tolist())
    print(f"[策略2] 股價篩選通過：{len(price_picks)} 檔")
    if price_picks.empty: return pd.DataFrame()
    print("[策略2] 抓取營收資料...")
    revenue_df = get_revenue_data(rev_start)
    rev_passed = screen_revenue(revenue_df, price_picks["stock_id"].tolist())
    final = price_picks[price_picks["stock_id"].isin(rev_passed)].copy()
    final = final.merge(chip_picks, on="stock_id", how="inner")
    final = final.sort_values("trust_20d", ascending=False)
    print(f"[策略2] 最終精選：{len(final)} 檔")
    return final

if __name__ == "__main__":
    result = run()
    if not result.empty:
        print("\n=== 策略 2：投信認養精選 ===")
        print(result.to_string(index=False))
    else: print("今日無符合條件標的")
