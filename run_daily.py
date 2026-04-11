#!/usr/bin/env python3
"""
TW Stock Quant Strategies - Daily Runner
=========================================
Three independent strategies + sell signal detection + HTML output for GitHub Pages.

Strategy 1: Revenue Momentum (營收動能) - Score /20
Strategy 2: Chip Momentum (籌碼動能) - Score /18  [Redesigned: relative metrics]
Strategy 3: Ownership Concentration (籌碼集中) - Score /16  [New]
Sell Signal: Investment Trust consecutive net sell >= 3 days
"""

import os
import json
import time
import requests
import datetime
from collections import defaultdict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
FINMIND_API = "https://api.finmindtrade.com/api/v4/data"
FINMIND_LOGIN = "https://api.finmindtrade.com/api/v4/login"
FINMIND_USER = os.environ.get("FINMIND_USER", "")
FINMIND_PASSWORD = os.environ.get("FINMIND_PASSWORD", "")
FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "")
SELL_SIGNAL_CONSECUTIVE_DAYS = 3


def login_finmind():
    global FINMIND_TOKEN
    if FINMIND_TOKEN:
        print(f"[login] Using token: {FINMIND_TOKEN[:20]}...")
        return FINMIND_TOKEN
    resp = requests.post(FINMIND_LOGIN, data={"user_id": FINMIND_USER, "password": FINMIND_PASSWORD})
    data = resp.json()
    if data.get("token"):
        FINMIND_TOKEN = data["token"]
        print(f"[login] Got token: {FINMIND_TOKEN[:20]}...")
        return FINMIND_TOKEN
    raise Exception(f"Login failed: {data}")


def fetch_api(dataset, start_date, data_id=None, retry=2):
    params = {"dataset": dataset, "start_date": start_date, "token": FINMIND_TOKEN}
    if data_id:
        params["data_id"] = data_id
    for attempt in range(retry + 1):
        try:
            resp = requests.get(FINMIND_API, params=params, timeout=60)
            result = resp.json()
            if result.get("status") == 200:
                return result.get("data", [])
            print(f"[fetch] {dataset} {data_id or 'ALL'}: status={result.get('status')}")
            return []
        except Exception as e:
            print(f"[fetch] Attempt {attempt+1} failed: {e}")
            if attempt < retry:
                time.sleep(3)
    return []


def get_stock_names():
    data = fetch_api("TaiwanStockInfo", "2024-01-01")
    return {r.get("stock_id", ""): r.get("stock_name", "")
            for r in data if r.get("stock_id") and r.get("stock_name")}


# ===========================================================================
# Strategy 1: Revenue Momentum (營收動能) - unchanged
# ===========================================================================
def run_strategy_1(name_map):
    print("\n" + "=" * 60)
    print("Strategy 1: Revenue Momentum")
    print("=" * 60)

    today = datetime.date.today()
    cur_year, prev_year = today.year, today.year - 1

    rev_data = []
    for y in [prev_year, cur_year]:
        for m in range(1, 4):
            sd = f"{y}-{m:02d}-01"
            rows = fetch_api("TaiwanStockMonthRevenue", sd)
            rev_data.extend(rows)
            print(f"  [rev] {sd}: {len(rows)} rows")
            time.sleep(0.3)

    if not rev_data:
        print("[S1] No revenue data")
        return []

    by_stock = defaultdict(list)
    for r in rev_data:
        by_stock[r["stock_id"]].append(r)

    candidates = []
    for sid, rows in by_stock.items():
        rows.sort(key=lambda x: x["date"])
        yoys, latest_rev = [], 0
        for r in rows:
            yr, mo = int(r["date"][:4]), int(r["date"][5:7])
            if yr == cur_year:
                rev_now = float(r.get("revenue", 0))
                match = [p for p in rows
                         if int(p["date"][:4]) == prev_year and int(p["date"][5:7]) == mo]
                if match and float(match[0].get("revenue", 0)) > 0:
                    yoy = (rev_now - float(match[0]["revenue"])) / float(match[0]["revenue"]) * 100
                    yoys.append(round(yoy, 1))
                latest_rev = max(latest_rev, rev_now)
        if len(yoys) < 2:
            continue
        if any(y > 300 or y < -50 for y in yoys):
            continue
        if yoys[-1] < 5 or latest_rev < 3e8:
            continue
        candidates.append({
            "stock_id": sid, "yoys": yoys,
            "pos_count": sum(1 for y in yoys if y > 0),
            "latest_rev": latest_rev,
            "accel": yoys[-1] - yoys[-2]
        })
    print(f"  [S1] {len(candidates)} candidates")

    for c in candidates:
        s = 0
        yl = c["yoys"][-1]
        if yl > 50: s += 5
        elif yl > 30: s += 4
        elif yl > 15: s += 3
        elif yl > 5: s += 2
        if c["accel"] > 20: s += 3
        elif c["accel"] > 10: s += 2
        elif c["accel"] > 0: s += 1
        if c["pos_count"] >= 3: s += 3
        rb = c["latest_rev"] / 1e8
        if rb >= 100: s += 3
        elif rb >= 10: s += 2
        elif rb >= 5: s += 1
        c["rev_score"] = s

    candidates.sort(key=lambda x: x["rev_score"], reverse=True)
    top = candidates[:30]

    for c in top:
        sid = c["stock_id"]
        pd = fetch_api("TaiwanStockPrice",
                        (today - datetime.timedelta(days=120)).strftime("%Y-%m-%d"),
                        data_id=sid)
        time.sleep(0.3)
        c["price"], c["price_chg"] = None, 0
        c["above_ma20"], c["above_ma60"], c["ma_bonus"] = False, False, 0
        if pd and len(pd) >= 5:
            closes = [float(p["close"]) for p in pd if p.get("close")]
            if len(closes) >= 5:
                c["price"] = closes[-1]
                c["price_chg"] = round((closes[-1] / closes[0] - 1) * 100, 1)
                if len(closes) >= 20:
                    c["above_ma20"] = closes[-1] > sum(closes[-20:]) / 20
                if len(closes) >= 60:
                    c["above_ma60"] = closes[-1] > sum(closes[-60:]) / 60
                else:
                    c["above_ma60"] = closes[-1] > sum(closes) / len(closes)
                b = (2 if c.get("above_ma20") else 0) + (2 if c.get("above_ma60") else 0)
                if c["price_chg"] > 10: b += 1
                if c["price_chg"] > 20: b += 1
                c["ma_bonus"] = b

    for c in top:
        c["total_score"] = c["rev_score"] + c.get("ma_bonus", 0)
        c["name"] = name_map.get(c["stock_id"], "?")
    top.sort(key=lambda x: x["total_score"], reverse=True)

    result = []
    for c in top[:5]:
        result.append(c)
        result.append(c)
    print(f"  [S1] {len(result)} picks")
    return result


def _build_revenue_yoy_map():
    today = datetime.date.today()
    cur_year, prev_year = today.year, today.year - 1
    rev_data = []
    for y in [prev_year, cur_year]:
        for m in range(1, 4):
            rows = fetch_api("TaiwanStockMonthRevenue", f"{y}-{m:02d}-01")
            rev_data.extend(rows)
            time.sleep(0.3)
    by_stock = defaultdict(list)
    for r in rev_data:
        by_stock[r["stock_id"]].append(r)
    rev_map = {}
    for sid, rows in by_stock.items():
        rows.sort(key=lambda x: x["date"])
        yoys = []
        for r in rows:
            yr, mo = int(r["date"][:4]), int(r["date"][5:7])
            if yr == cur_year:
                match = [p for p in rows
                         if int(p["date"][:4]) == prev_year and int(p["date"][5:7]) == mo]
                if match and float(match[0].get("revenue", 0)) > 0:
                    yoys.append(round((float(r["revenue"]) - float(match[0]["revenue"]))
                                      / float(match[0]["revenue"]) * 100, 1))
        if yoys:
            rev_map[sid] = yoys
    return rev_map


# ===========================================================================
# Strategy 2: Chip Momentum (籌碼動能) - REDESIGNED with relative metrics
# ===========================================================================
def run_strategy_2(name_map):
    """
    Strategy 2: 籌碼動能 - 滿分 18
    A. 投信籌碼面 (10): 投量比(4) + 連續買超(3) + 集中度(3)
    B. 法人同向 (4): 外資方向(2) + 三法人同向(2)
    D. 技術面 (4): 均線排列(2) + 量價配合(2)
    必要條件: 投量比>=5% 或 近10日買超>=5天
    """
    print("\n" + "=" * 60)
    print("Strategy 2: Chip Momentum (Relative)")
    print("=" * 60)

    today = datetime.date.today()
    start_ref = (today - datetime.timedelta(days=130)).strftime("%Y-%m-%d")

    # Get trading days via TSMC
    tsmc = fetch_api("TaiwanStockPrice", start_ref, data_id="2330")
    all_tdays = sorted(set(p["date"] for p in tsmc))
    if len(all_tdays) < 20:
        print("[S2] Not enough trading days")
        return []

    last5 = all_tdays[-5:]
    last10 = all_tdays[-10:]
    last20 = all_tdays[-20:]
    latest_day = all_tdays[-1]
    print(f"  [S2] Latest trading day: {latest_day}, total {len(all_tdays)} days")

    # Step 1: Latest-day institutional data (all stocks)
    day_data = fetch_api("TaiwanStockInstitutionalInvestorsBuySell", latest_day)
    print(f"  [S2] {latest_day}: {len(day_data)} institutional rows")

    trust_net_today = defaultdict(float)
    for r in day_data:
        if r.get("name") == "Investment_Trust":
            trust_net_today[r["stock_id"]] += float(r.get("buy", 0)) - float(r.get("sell", 0))

    # Step 2: Latest-day volume (all stocks)
    price_all = fetch_api("TaiwanStockPrice", latest_day)
    vol_map = {}
    for p in price_all:
        vol_map[p["stock_id"]] = float(p.get("Trading_Volume", 0)) / 1000  # to 張

    # Step 3: 投量比 pre-filter
    tvr_map = {}
    for sid, net in trust_net_today.items():
        if net <= 0:
            continue
        vol = vol_map.get(sid, 0)
        if vol > 100:
            ratio = net / vol * 100
            if ratio >= 2:
                tvr_map[sid] = ratio

    pre_sids = sorted(tvr_map.keys(), key=lambda s: tvr_map[s], reverse=True)[:80]
    print(f"  [S2] {len(pre_sids)} stocks with TVR >= 2%")

    # Step 4: Detailed analysis
    start_inst = (today - datetime.timedelta(days=45)).strftime("%Y-%m-%d")
    candidates = []

    for sid in pre_sids:
        inst_rows = fetch_api("TaiwanStockInstitutionalInvestorsBuySell",
                              start_inst, data_id=sid)
        price_rows = fetch_api("TaiwanStockPrice", start_ref, data_id=sid)
        time.sleep(0.15)

        if not inst_rows or not price_rows:
            continue

        # Build daily maps
        d_trust = defaultdict(float)
        d_foreign = defaultdict(float)
        d_dealer = defaultdict(float)
        for r in inst_rows:
            net = float(r.get("buy", 0)) - float(r.get("sell", 0))
            if r["name"] == "Investment_Trust":
                d_trust[r["date"]] += net
            elif r["name"] == "Foreign_Investor":
                d_foreign[r["date"]] += net
            elif "Dealer" in r.get("name", ""):
                d_dealer[r["date"]] += net

        d_close, d_vol = {}, {}
        for p in price_rows:
            d_close[p["date"]] = float(p.get("close", 0))
            d_vol[p["date"]] = float(p.get("Trading_Volume", 0)) / 1000

        closes = [d_close[d] for d in sorted(d_close) if d_close[d] > 0]
        vols = [d_vol[d] for d in sorted(d_vol) if d_vol[d] > 0]
        if len(closes) < 20 or len(vols) < 20:
            continue

        # === A. 投信籌碼面 (max 10) ===
        score_a = 0

        # A1. 投量比 (max 4)
        tvr_vals = []
        for d in last5:
            t, v = d_trust.get(d, 0), d_vol.get(d, 0)
            if v > 0 and t > 0:
                tvr_vals.append(t / v * 100)
        avg_tvr = sum(tvr_vals) / len(tvr_vals) if tvr_vals else 0
        best_tvr = max(avg_tvr, tvr_map.get(sid, 0))

        if best_tvr > 10:
            score_a += 4
        elif best_tvr > 5:
            score_a += 2

        # A2. 連續買超天數 (max 3)
        buy_days_10 = sum(1 for d in last10 if d_trust.get(d, 0) > 0)
        consec = 0
        for d in reversed(last10):
            if d_trust.get(d, 0) > 0:
                consec += 1
            else:
                break

        if buy_days_10 >= 7:
            score_a += 2
        elif buy_days_10 >= 5:
            score_a += 1
        if consec >= 3:
            score_a += 1

        # A3. 籌碼集中度 (max 3)
        inst_dates = sorted(d_trust.keys())

        def _conc(n):
            w = inst_dates[-n:] if len(inst_dates) >= n else inst_dates
            ct = sum(d_trust.get(d, 0) for d in w)
            cv = sum(d_vol.get(d, 0) for d in w)
            return ct / cv * 100 if cv > 0 else 0

        c20 = _conc(20)
        c60 = _conc(min(60, len(inst_dates)))

        if c60 > 5:
            score_a += 3
        elif c20 > 3:
            score_a += 2
        elif c20 > 1:
            score_a += 1

        # === B. 法人同向 (max 4) ===
        score_b = 0
        f5 = sum(d_foreign.get(d, 0) for d in last5)
        f20 = sum(d_foreign.get(d, 0) for d in last20)
        t5 = sum(d_trust.get(d, 0) for d in last5)
        dl5 = sum(d_dealer.get(d, 0) for d in last5)

        if f20 > 0:
            score_b += 2
        elif f5 > 0:
            score_b += 1

        if t5 > 0 and f5 > 0 and dl5 > 0:
            score_b += 2
        elif t5 > 0 and f5 > 0:
            score_b += 1

        # === D. 技術面 (max 4) ===
        score_d = 0
        price = closes[-1]
        ma5 = sum(closes[-5:]) / min(5, len(closes))
        ma20 = sum(closes[-20:]) / min(20, len(closes))
        ma60 = (sum(closes[-60:]) / min(60, len(closes))
                if len(closes) >= 60 else sum(closes) / len(closes))

        if ma5 > ma20 > ma60:
            score_d += 2
        elif price > ma20:
            score_d += 1

        vol5 = sum(vols[-5:]) / min(5, len(vols))
        vol20 = sum(vols[-20:]) / min(20, len(vols))
        vr = vol5 / vol20 if vol20 > 0 else 0

        if vr > 1.5:
            score_d += 2
        elif vr > 1.0:
            score_d += 1

        total = score_a + score_b + score_d

        # Gate: TVR >= 5% OR buy_days >= 5
        if best_tvr < 5 and buy_days_10 < 5:
            continue

        candidates.append({
            "stock_id": sid,
            "name": name_map.get(sid, "?"),
            "total_score": total,
            "score_detail": f"A{score_a}+B{score_b}+D{score_d}",
            "tvr": round(best_tvr, 1),
            "buy_days": buy_days_10,
            "consec": consec,
            "conc_20": round(c20, 1),
            "foreign_5d": f5,
            "trust_5d": t5,
            "price": price,
            "above_ma20": price > ma20,
            "above_ma60": price > ma60,
            "vol_ratio": round(vr, 2),
        })

    candidates.sort(key=lambda x: x["total_score"], reverse=True)
    result = []
    for i, c in enumerate(candidates[:5]):
        result.append(c)
    print(f"  [S2] {len(result)} picks (from {len(candidates)} qualified)")
    return result


# ===========================================================================
# Strategy 3: Ownership Concentration (籌碼集中) - NEW
# ===========================================================================
def run_strategy_3(name_map):
    """
    Strategy 3: 籌碼集中 - 滿分 16
    Pre-filter: Top 30 by 5-day volume surge + price change
    A. 集保大戶持股變化 (6): 大戶(>=400張)持股比例週增幅
    B. 融資融券信號 (4): 融資減+股價穩=主力吸籌, 融券增=軋空
    C. 法人集中買超比 (3): 三大法人5日淨買超 / 成交量
    D. 量價特徵 (3): 均線+量能+漲幅
    """
    print("\n" + "=" * 60)
    print("Strategy 3: Ownership Concentration")
    print("=" * 60)

    today = datetime.date.today()
    start_ref = (today - datetime.timedelta(days=100)).strftime("%Y-%m-%d")

    # Get trading days
    tsmc = fetch_api("TaiwanStockPrice", start_ref, data_id="2330")
    all_tdays = sorted(set(p["date"] for p in tsmc))
    if len(all_tdays) < 10:
        print("[S3] Not enough trading days")
        return []

    latest_day = all_tdays[-1]
    day_5ago = all_tdays[-6] if len(all_tdays) >= 6 else all_tdays[0]
    last5 = all_tdays[-5:]

    # Step 1: Pre-filter by volume surge + price change
    print(f"  [S3] Pre-filter: {day_5ago} vs {latest_day}")
    price_now = fetch_api("TaiwanStockPrice", latest_day)
    time.sleep(0.3)
    price_ago = fetch_api("TaiwanStockPrice", day_5ago)

    now_map = {}
    for p in price_now:
        now_map[p["stock_id"]] = {
            "close": float(p.get("close", 0)),
            "vol": float(p.get("Trading_Volume", 0))
        }

    ago_map = {}
    for p in price_ago:
        ago_map[p["stock_id"]] = {
            "close": float(p.get("close", 0)),
            "vol": float(p.get("Trading_Volume", 0))
        }

    activity = []
    for sid in now_map:
        if sid not in ago_map:
            continue
        n, a = now_map[sid], ago_map[sid]
        if a["close"] <= 0 or a["vol"] <= 0 or n["vol"] <= 0:
            continue
        if not (sid.isdigit() and len(sid) == 4):
            continue
        pchg = abs(n["close"] / a["close"] - 1)
        vchg = n["vol"] / a["vol"] if a["vol"] > 0 else 1
        score = vchg * (1 + pchg * 2)
        activity.append((sid, score, n["close"] / a["close"] - 1, vchg))

    activity.sort(key=lambda x: x[1], reverse=True)
    top30 = [s[0] for s in activity[:30]]
    print(f"  [S3] Top 30 active stocks selected")

    # Step 2: Get institutional data for latest 5 days (all stocks, batch)
    inst_5d = defaultdict(float)
    for day in last5:
        rows = fetch_api("TaiwanStockInstitutionalInvestorsBuySell", day)
        for r in rows:
            net = float(r.get("buy", 0)) - float(r.get("sell", 0))
            inst_5d[r["stock_id"]] += net
        time.sleep(0.2)

    # Step 3: Detailed analysis for top 30
    start_holding = (today - datetime.timedelta(days=35)).strftime("%Y-%m-%d")
    start_margin = (today - datetime.timedelta(days=20)).strftime("%Y-%m-%d")

    candidates = []
    for sid in top30:
        holding = fetch_api("TaiwanStockHoldingSharesPer", start_holding, data_id=sid)
        time.sleep(0.1)
        margin = fetch_api("TaiwanStockMarginPurchaseShortSale", start_margin, data_id=sid)
        time.sleep(0.1)
        prices = fetch_api("TaiwanStockPrice",
                           (today - datetime.timedelta(days=130)).strftime("%Y-%m-%d"),
                           data_id=sid)
        time.sleep(0.1)

        # === A. 集保大戶持股 (max 6) ===
        score_a = 0
        big_pct_now, big_pct_prev = 0, 0

        if holding:
            by_date = defaultdict(list)
            for h in holding:
                by_date[h["date"]].append(h)
            dates = sorted(by_date.keys())

            def _big_pct(records):
                total = 0
                for r in records:
                    level = r.get("HoldingSharesLevel", "")
                    try:
                        low = int(level.split("-")[0].replace(",", "").strip())
                        if low >= 400001:
                            total += float(r.get("percent", 0))
                    except ValueError:
                        if "above" in level.lower() or "\u4ee5\u4e0a" in level:
                            total += float(r.get("percent", 0))
                return total

            if len(dates) >= 2:
                big_pct_now = _big_pct(by_date[dates[-1]])
                big_pct_prev = _big_pct(by_date[dates[-2]])
            elif len(dates) == 1:
                big_pct_now = _big_pct(by_date[dates[0]])

            chg = big_pct_now - big_pct_prev
            if chg > 2:
                score_a = 6
            elif chg > 1:
                score_a = 4
            elif chg > 0.5:
                score_a = 2
            elif big_pct_now > 60:
                score_a = 1

        # === B. 融資融券 (max 4) ===
        score_b = 0
        margin_chg, short_chg = 0, 0

        if margin and len(margin) >= 2:
            margin.sort(key=lambda x: x["date"])
            latest_m = margin[-1]
            prev_m = margin[0] if len(margin) <= 5 else margin[-6]

            margin_bal = int(latest_m.get("MarginPurchaseTodayBalance", 0))
            prev_margin_bal = int(prev_m.get("MarginPurchaseTodayBalance", 0))
            margin_chg = margin_bal - prev_margin_bal

            short_bal = int(latest_m.get("ShortSaleTodayBalance", 0))
            prev_short_bal = int(prev_m.get("ShortSaleTodayBalance", 0))
            short_chg = short_bal - prev_short_bal

            # 融資減少 + 股價穩 = 主力吸籌
            n_close = now_map.get(sid, {}).get("close", 0)
            a_close = ago_map.get(sid, {}).get("close", 0)
            price_ok = n_close >= a_close * 0.98 if a_close > 0 else False
            if margin_chg < 0 and price_ok:
                score_b += 2
            # 融券增加 = 軋空題暈
            if short_chg > 0 and short_chg > max(50, int(short_bal * 0.05)):
                score_b += 2
            elif short_chg > 0:
                score_b += 1

        # === C. 法人集中買超比 (max 3) ===
        score_c = 0
        inst_ratio = 0

        vol_5d = 0
        if prices:
            for p in prices:
                if p["date"] in last5:
                    vol_5d += float(p.get("Trading_Volume", 0)) / 1000

        total_inst = inst_5d.get(sid, 0)
        if vol_5d > 0 and total_inst > 0:
            inst_ratio = total_inst / vol_5d * 100
            if inst_ratio > 30:
                score_c = 3
            elif inst_ratio > 20:
                score_c = 2
            elif inst_ratio > 10:
                score_c = 1

        # === D. 量價特徵 (max 3) ===
        score_d = 0
        price_val = now_map.get(sid, {}).get("close", 0)
        above_ma20 = False

        if prices:
            cls = [float(p["close"]) for p in prices if float(p.get("close", 0)) > 0]
            vls = [float(p.get("Trading_Volume", 0)) / 1000
                   for p in prices if float(p.get("Trading_Volume", 0)) > 0]

            if len(cls) >= 20:
                ma20 = sum(cls[-20:]) / 20
                if cls[-1] > ma20:
                    score_d += 1
                    above_ma20 = True

            if len(vls) >= 20:
                v5 = sum(vls[-5:]) / min(5, len(vls))
                v20 = sum(vls[-20:]) / 20
                if v20 > 0 and v5 > v20 * 1.5:
                    score_d += 1

            if len(cls) >= 5 and cls[-5] > 0:
                pchg5 = (cls[-1] / cls[-5] - 1) * 100
                if pchg5 > 3:
                    score_d += 1

        total = score_a + score_b + score_c + score_d

        candidates.append({
            "stock_id": sid,
            "name": name_map.get(sid, "?"),
            "total_score": total,
            "score_detail": f"A{score_a}+B{score_b}+C{score_c}+D{score_d}",
            "big_holder_pct": round(big_pct_now, 1),
            "big_holder_chg": round(big_pct_now - big_pct_prev, 2),
            "margin_chg": margin_chg,
            "short_chg": short_chg,
            "inst_ratio": round(inst_ratio, 1),
            "price": price_val,
            "above_ma20": above_ma20,
        })

    candidates.sort(key=lambda x: x["total_score"], reverse=True)
    result = []
    for i, c in enumerate(candidates[:5]):
        result.append(c)
    print(f"  [S3] {len(result)} picks")
    return result


# ===========================================================================
# Sell Signal Detection (unchanged)
# ===========================================================================
def detect_sell_signals(name_map):
    print("\n" + "=" * 60)
    print(f"Sell Signal Detection (trust consec sell >= {SELL_SIGNAL_CONSECUTIVE_DAYS} days)")
    print("=" * 60)

    today = datetime.date.today()
    start = (today - datetime.timedelta(days=55)).strftime("%Y-%m-%d")
    tsmc = fetch_api("TaiwanStockPrice", start, data_id="2330")
    tdays = sorted(set(p["date"] for p in tsmc))
    if len(tdays) < SELL_SIGNAL_CONSECUTIVE_DAYS:
        print("[sell] Not enough days")
        return []

    check_days = tdays[-(SELL_SIGNAL_CONSECUTIVE_DAYS + 2):]
    daily_trust = {}
    for day in check_days:
        dd = fetch_api("TaiwanStockInstitutionalInvestorsBuySell", day)
        tm = defaultdict(float)
        for r in dd:
            if r.get("name") == "Investment_Trust":
                tm[r["stock_id"]] += float(r.get("buy", 0)) - float(r.get("sell", 0))
        daily_trust[day] = tm
        print(f"  [sell] {day}: {len(tm)} stocks")
        time.sleep(0.3)

    last_n = check_days[-SELL_SIGNAL_CONSECUTIVE_DAYS:]
    all_sids = set()
    for dm in daily_trust.values():
        all_sids.update(dm.keys())

    signals = []
    for sid in all_sids:
        consec = 0
        for day in last_n:
            if daily_trust.get(day, {}).get(sid, 0) < 0:
                consec += 1
            else:
                consec = 0
        if consec >= SELL_SIGNAL_CONSECUTIVE_DAYS:
            total = sum(daily_trust.get(d, {}).get(sid, 0) for d in last_n)
            signals.append({
                "stock_id": sid,
                "name": name_map.get(sid, "?"),
                "consecutive_days": consec,
                "total_net": total,
                "daily_nets": {d: daily_trust.get(d, {}).get(sid, 0) for d in last_n}
            })

    signals.sort(key=lambda x: x["total_net"])
    signals = [s for s in signals if s["total_net"] < -100000]

    # --- MA filter: keep only stocks that broke below MA20 or MA60 ---
    price_start = (today - datetime.timedelta(days=120)).strftime("%Y-%m-%d")
    filtered = []
    for s in signals[:40]:  # check top 40 candidates (some may be filtered out)
        sid = s["stock_id"]
        try:
            prices = fetch_api("TaiwanStockPrice", price_start, data_id=sid)
            closes = [float(p["close"]) for p in sorted(prices, key=lambda x: x["date"]) if float(p.get("close", 0)) > 0]
            if len(closes) < 5:
                continue
            cur = closes[-1]
            ma20 = sum(closes[-20:]) / min(len(closes), 20) if len(closes) >= 5 else cur
            ma60 = sum(closes[-60:]) / min(len(closes), 60) if len(closes) >= 5 else cur
            below_ma20 = cur < ma20
            below_ma60 = cur < ma60
            s["price"] = cur
            s["ma20"] = round(ma20, 2)
            s["ma60"] = round(ma60, 2)
            s["below_ma20"] = below_ma20
            s["below_ma60"] = below_ma60
            if below_ma20 or below_ma60:
                filtered.append(s)
            time.sleep(0.15)
        except Exception as e:
            print(f"  [sell] MA check failed for {sid}: {e}")
            continue

    print(f"  [sell] {len(signals)} raw -> {len(filtered)} after MA filter")
    return filtered[:20]


# ===========================================================================
# HTML Output - UPDATED for 3 strategies
# ===========================================================================
def generate_html(s1, s2, s3, sell):
    today_s = datetime.date.today().strftime("%Y-%m-%d")
    now_s = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    def fn(n):
        if n is None:
            return "-"
        if abs(n) >= 1e6:
            return f"{n/1e6:.1f}M"
        if abs(n) >= 1e3:
            return f"{n/1e3:.0f}K"
        return f"{n:.0f}"

    def ys(yoys):
        if not yoys:
            return "N/A"
        return " / ".join(f"{y:.1f}%" for y in yoys)

    def tb(tier):
        colors = {"Top": "#e74c3c", "Second": "#f39c12", "Third": "#3498db"}
        return f'<span class="badge" style="background:{colors.get(tier, "#999")}">{tier}</span>'

    def mi(v):
        return '<span class="ma-yes">&#10003;</span>' if v else '<span class="ma-no">&#10007;</span>'

    # S1 rows
    s1r = ""
    for c in s1:
        s1r += (f'<tr>'
                f'<td><b>{c["stock_id"]}</b></td>'
                f'<td>{c.get("name","?")}</td>'
                f'<td class="num">{c.get("total_score",0)}</td>'
                f'<td>{ys(c.get("yoys",[]))}</td>'
                f'<td class="num">{c.get("accel",0):+.1f}%</td>'
                f'<td class="num">{c.get("latest_rev",0)/1e8:.1f}\u5104</td>'
                f'<td class="num">{c.get("price") or "-"}</td>'
                f'<td class="num">{c.get("price_chg",0):+.1f}%</td>'
                f'<td class="ctr">{mi(c.get("above_ma20"))}</td>'
                f'<td class="ctr">{mi(c.get("above_ma60"))}</td></tr>\n')

    # S2 rows
    s2r = ""
    for c in s2:
        s2r += (f'<tr><td><b>{c["stock_id"]}</b></td>'
                f'<td>{c.get("name","?")}</td>'
                f'<td class="num">{c.get("total_score",0)}</td>'
                f'<td class="num">{c.get("tvr",0):.1f}%</td>'
                f'<td class="num">{c.get("buy_days",0)}/10</td>'
                f'<td class="num">{c.get("consec",0)}d</td>'
                f'<td class="num">{c.get("conc_20",0):.1f}%</td>'
                f'<td class="num">{fn(c.get("foreign_5d",0))}</td>'
                f'<td class="num">{c.get("price") or "-"}</td>'
                f'<td class="ctr">{mi(c.get("above_ma20"))}</td>'
                f'<td class="num">{c.get("vol_ratio",0):.1f}x</td></tr>\n')

    # S3 rows
    s3r = ""
    for c in s3:
        s3r += (f'<tr><td><b>{c["stock_id"]}</b></td>'
                f'<td>{c.get("name","?")}</td>'
                f'<td class="num">{c.get("total_score",0)}</td>'
                f'<td class="num">{c.get("big_holder_pct",0):.1f}%</td>'
                f'<td class="num">{c.get("big_holder_chg",0):+.2f}%</td>'
                f'<td class="num">{fn(c.get("margin_chg",0))}</td>'
                f'<td class="num">{fn(c.get("short_chg",0))}</td>'
                f'<td class="num">{c.get("inst_ratio",0):.1f}%</td>'
                f'<td class="num">{c.get("price") or "-"}</td>'
                f'<td class="ctr">{mi(c.get("above_ma20"))}</td></tr>\n')

    # Sell rows
    sellr = ""
    for s in sell:
        nets = s.get("daily_nets", {})
        ds = " / ".join(fn(nets[d]) for d in sorted(nets.keys()))
        price_s = f'{s["price"]:.2f}' if s.get("price") else "-"
        ma20_s = f'{s["ma20"]:.2f}' if s.get("ma20") else "-"
        ma60_s = f'{s["ma60"]:.2f}' if s.get("ma60") else "-"
        bm20 = '<span class="ma-no">&#9660;</span>' if s.get("below_ma20") else '<span class="ma-yes">&#9650;</span>'
        bm60 = '<span class="ma-no">&#9660;</span>' if s.get("below_ma60") else '<span class="ma-yes">&#9650;</span>'
        sellr += (f'<tr><td><b>{s["stock_id"]}</b></td>'
                  f'<td>{s.get("name","?")}</td>'
                  f'<td class="sr">{s["consecutive_days"]} \u5929</td>'
                  f'<td class="sr">{fn(s["total_net"])}</td>'
                  f'<td class="num">{price_s}</td>'
                  f'<td class="ctr">{bm20} {ma20_s}</td>'
                  f'<td class="ctr">{bm60} {ma60_s}</td>'
                  f'<td class="dd">{ds}</td></tr>\n')
    nosell = '<tr><td colspan="8" class="empty">\u4eca\u65e5\u7121\u8ce3\u51fa\u8b66\u793a</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>\u53f0\u80a1\u7b56\u7565\u89c0\u5bdf {today_s}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Noto Sans TC",sans-serif;background:#0a0a0f;color:#d0d0d0;padding:20px;max-width:1200px;margin:0 auto}}
h1{{text-align:center;font-size:22px;color:#fff;margin-bottom:4px;padding-top:80px}}
.sub{{text-align:center;color:#666;font-size:13px;margin-bottom:24px}}
.sec{{border-radius:12px;padding:20px;margin-bottom:20px;overflow-x:auto}}
.s1,.s2,.s3{{background:#111828;border:1px solid #1e2d4a}}
.sell{{background:#1f1114;border:1px solid #3d1a22}}
h2{{font-size:17px;margin-bottom:4px;color:#fff}}
.desc{{color:#777;font-size:12px;margin-bottom:14px}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{background:#0d1520;color:#8899bb;padding:8px 6px;text-align:left;font-weight:600;font-size:11px;border-bottom:2px solid #1e2d4a;white-space:nowrap}}
.sell th{{background:#1a0d10;border-bottom-color:#3d1a22}}
td{{padding:7px 6px;border-bottom:1px solid #181f30;white-space:nowrap}}
.sell td{{border-bottom-color:#2a1118}}
tr:hover td{{background:rgba(255,255,255,0.02)}}
.num{{text-align:right;font-variant-numeric:tabular-nums}}
.ctr{{text-align:center}}
.badge{{display:inline-block;color:#fff;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}}
.ma-yes{{color:#2ecc71;font-weight:bold}}
.ma-no{{color:#e74c3c}}
.sr{{color:#e74c3c;font-weight:bold;text-align:right}}
.dd{{font-size:11px;color:#999}}
.empty{{text-align:center;color:#555;padding:20px}}
.legend{{display:flex;gap:16px;flex-wrap:wrap;margin-top:8px;font-size:11px;color:#666}}
.footer{{text-align:center;color:#444;font-size:11px;margin-top:30px;padding-top:14px;border-top:1px solid #1a1a2a}}
.footer a{{color:#555;text-decoration:none}}
@media(max-width:768px){{body{{padding:10px}}table{{font-size:11px}}th,td{{padding:5px 3px}}}}
</style></head>
<body>
<h1>\u53f0\u80a1\u7b56\u7565\u89c0\u5bdf - \u6bcf\u65e5\u5e02\u5834\u89c0\u5bdf</h1>
<p class="sub">\u66f4\u65b0\uff1a{now_s} (UTC+8) | FinMind API</p>

<div class="sec s1"><h2>&#x1F4C8; \u7b56\u7565\u4e00\uff1a\u71df\u6536\u52d5\u80fd</h2>
<p class="desc">\u8fd1\u4e09\u6708\u71df\u6536 YoY \u6b63\u6210\u9577 + \u52a0\u901f + \u898f\u6a21&ge;3\u5104 + \u5747\u7dda\u591a\u982d | \u6eff\u520620</p>
<table><thead><tr><th>\u4ee3\u865f</th><th>\u540d\u7a31</th><th>\u5206\u6578</th><th>YoY</th><th>\u52a0\u901f\u5ea6</th><th>\u71df\u6536</th><th>\u80a1\u50f9</th><th>\u6f32\u5e45</th><th>MA20</th><th>MA60</th></tr></thead>
<tbody>{s1r}</tbody></table></div>

<div class="sec s2"><h2>&#x1F4B0; \u7b56\u7565\u4e8c\uff1a\u7c4c\u78bc\u52d5\u80fd</h2>
<p class="desc">\u6295\u91cf\u6bd4 + \u9023\u7e8c\u8cb7\u8d85 + \u7c4c\u78bc\u96c6\u4e2d\u5ea6 + \u6cd5\u4eba\u540c\u5411 + \u6280\u8853\u9762 | \u6eff\u520618</p>
<table><thead><tr><th>\u4ee3\u865f</th><th>\u540d\u7a31</th><th>\u5206\u6578</th><th>\u6295\u91cf\u6bd4</th><th>\u8cb7\u8d85\u5929</th><th>\u9023\u7e8c</th><th>\u96c6\u4e2d\u5ea6</th><th>\u5916\u8cc75D</th><th>\u80a1\u50f9</th><th>MA20</th><th>\u91cf\u6bd4</th></tr></thead>
<tbody>{s2r}</tbody></table></div>

<div class="sec s3"><h2>&#x1F50D; \u7b56\u7565\u4e09\uff1a\u7c4c\u78bc\u96c6\u4e2d</h2>
<p class="desc">\u96c6\u4fdd\u5927\u6236\u589e + \u878d\u8cc7\u6e1b + \u6cd5\u4eba\u96c6\u4e2d\u8cb7 + \u91cf\u50f9\u914d\u5408 | \u6eff\u520616</p>
<table><thead><tr><th>\u4ee3\u865f</th><th>\u540d\u7a31</th><th>\u5206\u6578</th><th>\u5927\u6236%</th><th>\u5927\u6236\u8b8a\u5316</th><th>\u878d\u8cc7\u8b8a</th><th>\u878d\u5238\u8b8a</th><th>\u6cd5\u4eba\u6bd4</th><th>\u80a1\u50f9</th><th>MA20</th></tr></thead>
<tbody>{s3r}</tbody></table></div>

<div class="sec sell"><h2>&#x26A0;&#xFE0F; \u8ce3\u51fa\u8b66\u793a</h2>
<p class="desc">\u6295\u4fe1\u9023\u7e8c {SELL_SIGNAL_CONSECUTIVE_DAYS} \u5929\u6de8\u8ce3\u8d85 \u4e14 \u5408\u8a08>10\u842c\u5f35 \u4e14 \u8dcc\u7834MA20\u6216MA60</p>
<table><thead><tr><th>\u4ee3\u865f</th><th>\u540d\u7a31</th><th>\u9023\u8ce3</th><th>\u5408\u8a08</th><th>\u80a1\u50f9</th><th>MA20</th><th>MA60</th><th>\u660e\u7d30</th></tr></thead>
<tbody>{sellr if sellr else nosell}</tbody></table></div>

<div class="legend">
<span>&#10003; \u7ad9\u4e0a\u5747\u7dda</span><span>&#10007; \u8dcc\u7834</span><span>&#9660; \u8dcc\u7834\u5747\u7dda</span><span>&#9650; \u7ad9\u4e0a\u5747\u7dda</span>
<span>\u6295\u91cf\u6bd4=\u6295\u4fe1\u8cb7\u8d85/\u6210\u4ea4\u91cf</span>
<span>\u96c6\u4e2d\u5ea6=20\u65e5\u7d2f\u8a08\u6295\u4fe1/\u6210\u4ea4\u91cf</span>
<span>\u5927\u6236%=\u96c6\u4fdd&ge;400\u5f35\u6301\u80a1\u6bd4</span>
</div>
<div class="footer">Powered by <a href="https://finmindtrade.com/">FinMind</a> | <a href="https://tetsu811.com">Tetsu</a> | \u50c5\u4f9b\u7814\u7a76\u53c3\u8003</div>
</body></html>"""
    return html


# ===========================================================================
# LINE Message - UPDATED for 3 strategies
# ===========================================================================
def format_line_message(s1, s2, s3, sell):
    today_s = datetime.date.today().strftime("%Y-%m-%d")
    lines = [f"\U0001F4C8 \u53f0\u80a1\u7b56\u7565\u89c0\u5bdf {today_s}\n"]

    lines.append("=== \u7b56\u7565\u4e00\uff1a\u71df\u6536\u52d5\u80fd ===")
    for c in s1:
        lines.append(f"{c['stock_id']} {c.get('name','?')} "
                     f"s={c['total_score']} "
                     f"YoY={'/'.join(f'{y:.0f}%' for y in c.get('yoys',[]))}")

    lines.append("\n=== \u7b56\u7565\u4e8c\uff1a\u7c4c\u78bc\u52d5\u80fd ===")
    for c in s2:
        lines.append(f"{c['stock_id']} {c.get('name','?')} "
                     f"s={c.get('total_score',0)} "
                     f"TVR={c.get('tvr',0):.1f}% "
                     f"buy={c.get('buy_days',0)}/10d")

    lines.append("\n=== \u7b56\u7565\u4e09\uff1a\u7c4c\u78bc\u96c6\u4e2d ===")
    for c in s3:
        lines.append(f"{c['stock_id']} {c.get('name','?')} "
                     f"s={c.get('total_score',0)} "
                     f"\u5927\u6236={c.get('big_holder_pct',0):.1f}%"
                     f"({c.get('big_holder_chg',0):+.1f})")

    if sell:
        lines.append(f"\n\u26A0\uFE0F \u8ce3\u51fa\u8b66\u793a "
                     f"(\u6295\u4fe1\u9023\u8ce3>={SELL_SIGNAL_CONSECUTIVE_DAYS}\u5929)")
        for s in sell[:10]:
            lines.append(f"  {s['stock_id']} {s.get('name','?')} "
                         f"\u9023{s['consecutive_days']}\u5929 "
                         f"\u6de8\u8ce3={s['total_net']/1000:.0f}K")

    lines.append(f"\n\U0001F310 https://tetsu811.github.io/tw-quant-strategies/")
    return "\n".join(lines)


# ===========================================================================
# Main - UPDATED
# ===========================================================================
def main():
    print("=" * 60)
    print(f"TW Stock Quant Strategies - {datetime.date.today()}")
    print("=" * 60)

    login_finmind()
    name_map = get_stock_names()

    s1 = run_strategy_1(name_map)
    s2 = run_strategy_2(name_map)
    s3 = run_strategy_3(name_map)
    sell = detect_sell_signals(name_map)

    html = generate_html(s1, s2, s3, sell)
    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n[html] docs/index.html ({len(html)} chars)")

    results = {
        "date": datetime.date.today().isoformat(),
        "strategy_1": s1,
        "strategy_2": s2,
        "strategy_3": s3,
        "sell_signals": sell,
    }
    with open("docs/latest.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)

    lt = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
    lu = os.environ.get("LINE_USER_IDS", "")
    if lt and lu:
        from line_push import push_line_message
        push_line_message(format_line_message(s1, s2, s3, sell))
        print("[line] Pushed")
    else:
        print("[line] Skipped")

    print("\nDone!")


if __name__ == "__main__":
    main()
