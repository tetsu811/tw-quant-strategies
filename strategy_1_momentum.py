"""
策略 1 v2：月營收加速度 + 股價動能
--------------------------------------
篩選條件：
  1. 月營收 YoY 連 3 月為正
  2. 營收加速度 > 0（最近月 YoY − 前月 YoY）
  3. 月營收 ≥ 5 億
  4. 多頭排列：收盤 > 20MA > 60MA
  5. RS60（60 日相對強度）前 20%
"""

import os
import datetime
import requests
import pandas as pd
import numpy as np

FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "")
API_URL = "https://api.finmindtrade.com/api/v4/data"


def _get(dataset: str, data_id: str = "", start_date: str = "", end_date: str = "", **kw) -> pd.DataFrame:
    params = {"dataset": dataset, "data_id": data_id, "start_date": start_date, "end_date": end_date, "token": FINMIND_TOKEN}
    params.update(kw)
    params = {k: v for k, v in params.items() if v}
    r = requests.get(API_URL, params=params, timeout=30)
    data = r.json()
    if data.get("status") != 200 or not data.get("data"):
        if data.get("msg"):
            print(f"  [FinMind] {dataset}: {data.get('msg')}")
        return pd.DataFrame()
    return pd.DataFrame(data["data"])


def get_revenue_data(start_date: str) -> pd.DataFrame:
    """取得月營收資料"""
    df = _get("TaiwanStockMonthRevenue", start_date=start_date)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    return df


def get_price_data(start_date: str) -> pd.DataFrame:
    """取得股價資料"""
    df = _get("TaiwanStockPrice", start_date=start_date)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "max", "min", "close", "Trading_Volume", "Trading_money"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def screen_revenue(revenue_df: pd.DataFrame) -> pd.DataFrame:
    """篩選營收條件：連 3 月 YoY 正、加速度 > 0、營收 ≥ 5 億"""
    if revenue_df.empty:
        return pd.DataFrame(columns=["stock_id"])

    df = revenue_df.copy()
    df = df.sort_values(["stock_id", "date"])

    # 計算 YoY
    df["revenue_yoy"] = df.groupby("stock_id")["revenue"].pct_change(periods=12) * 100

    results = []
    for sid, grp in df.groupby("stock_id"):
        grp = grp.dropna(subset=["revenue_yoy"]).tail(3)
        if len(grp) < 3:
            continue
        # 連 3 月 YoY 為正
        if not (grp["revenue_yoy"] > 0).all():
            continue
        # 加速度 > 0
        yoy_vals = grp["revenue_yoy"].values
        accel = yoy_vals[-1] - yoy_vals[-2]
        if accel <= 0:
            continue
        # 最新月營收 ≥ 5 億
        latest_rev = grp["revenue"].iloc[-1]
        if latest_rev < 500_000:  # 千元為單位，5 億 = 500,000 千
            continue
        results.append({
            "stock_id": sid,
            "latest_revenue": latest_rev,
            "latest_yoy": round(yoy_vals[-1], 2),
            "yoy_accel": round(accel, 2),
        })

    return pd.DataFrame(results)


def screen_price_momentum(price_df: pd.DataFrame, candidates: list) -> pd.DataFrame:
    """篩選股價動能：多頭排列 + RS60 前 20%"""
    if price_df.empty:
        return pd.DataFrame()

    df = price_df[price_df["stock_id"].isin(candidates)].copy()
    df = df.sort_values(["stock_id", "date"])

    # 計算均線
    df["ma20"] = df.groupby("stock_id")["close"].transform(lambda x: x.rolling(20).mean())
    df["ma60"] = df.groupby("stock_id")["close"].transform(lambda x: x.rolling(60).mean())

    # 取最新一天
    latest = df.groupby("stock_id").tail(1).copy()
    latest = latest.dropna(subset=["ma20", "ma60"])

    # 多頭排列：收盤 > MA20 > MA60
    latest = latest[(latest["close"] > latest["ma20"]) & (latest["ma20"] > latest["ma60"])]

    # 計算 RS60（60 日漲幅排名）
    rs_data = []
    all_stocks = price_df["stock_id"].unique()
    for sid in all_stocks:
        sdf = price_df[price_df["stock_id"] == sid].sort_values("date").tail(61)
        if len(sdf) < 61:
            continue
        ret_60 = (sdf["close"].iloc[-1] / sdf["close"].iloc[0] - 1) * 100
        rs_data.append({"stock_id": sid, "rs60": ret_60})

    if not rs_data:
        return pd.DataFrame()

    rs_df = pd.DataFrame(rs_data)
    rs_df["rs60_rank"] = rs_df["rs60"].rank(pct=True)
    top20 = rs_df[rs_df["rs60_rank"] >= 0.80]["stock_id"].tolist()

    result = latest[latest["stock_id"].isin(top20)].copy()
    result = result.merge(rs_df[["stock_id", "rs60"]], on="stock_id", how="left")

    return result[["stock_id", "close", "ma20", "ma60", "rs60"]].round(2)


def run() -> pd.DataFrame:
    """執行策略 1，回傳精選股清單"""
    today = datetime.date.today()
    rev_start = (today - datetime.timedelta(days=450)).strftime("%Y-%m-%d")
    price_start = (today - datetime.timedelta(days=120)).strftime("%Y-%m-%d")

    print("[策略1] 抓取月營收資料...")
    revenue_df = get_revenue_data(rev_start)
    print(f"[策略1] 營收資料：{len(revenue_df)} 筆")

    rev_picks = screen_revenue(revenue_df)
    print(f"[策略1] 營收篩選通過：{len(rev_picks)} 檔")

    if rev_picks.empty:
        print("[策略1] 無符合營收條件的標的")
        return pd.DataFrame()

    print("[策略1] 抓取股價資料...")
    price_df = get_price_data(price_start)
    print(f"[策略1] 股價資料：{len(price_df)} 筆")

    momentum_picks = screen_price_momentum(price_df, rev_picks["stock_id"].tolist())
    print(f"[策略1] 動能篩選通過：{len(momentum_picks)} 檔")

    if momentum_picks.empty:
        return pd.DataFrame()

    final = momentum_picks.merge(rev_picks, on="stock_id", how="inner")
    final = final.sort_values("rs60", ascending=False)
    print(f"[策略1] 最終精選：{len(final)} 檔")
    return final


if __name__ == "__main__":
    result = run()
    if not result.empty:
        print("\n=== 策略 1：營收動能精選 ===")
        print(result.to_string(index=False))
    else:
        print("今日無符合條件標的")
