#!/usr/bin/env python3
"""
TW Stock Quant Strategies - Daily Runner
=========================================
Two independent strategies + sell signal detection + HTML output for GitHub Pages.

Strategy 1: Revenue Momentum (營收動能)
Strategy 2: Chip Momentum (籌碼動能)
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
    if data_id: params["data_id"] = data_id
    for attempt in range(retry + 1):
        try:
            resp = requests.get(FINMIND_API, params=params, timeout=60)
            result = resp.json()
            if result.get("status") == 200: return result.get("data", [])
            print(f"[fetch] {dataset} {data_id or 'ALL'}: status={result.get('status')}")
            return []
        except Exception as e:
            print(f"[fetch] Attempt {attempt+1} failed: {e}")
            if attempt < retry: time.sleep(3)
    return []

def get_stock_names():
    data = fetch_api("TaiwanStockInfo", "2024-01-01")
    return {r.get("stock_id",""): r.get("stock_name","") for r in data if r.get("stock_id") and r.get("stock_name")}

def run_strategy_1(name_map):
    print("\n" + "="*60)
    print("Strategy 1: Revenue Momentum")
    print("="*60)
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
        print("[S1] No revenue data"); return []
    by_stock = defaultdict(list)
    for r in rev_data: by_stock[r["stock_id"]].append(r)
    candidates = []
    for sid, rows in by_stock.items():
        rows.sort(key=lambda x: x["date"])
        yoys, latest_rev = [], 0
        for r in rows:
            yr, mo = int(r["date"][:4]), int(r["date"][5:7])
            if yr == cur_year:
                rev_now = float(r.get("revenue", 0))
                match = [p for p in rows if int(p["date"][:4])==prev_year and int(p["date"][5:7])==mo]
                if match and float(match[0].get("revenue",0))>0:
                    yoy = (rev_now - float(match[0]["revenue"])) / float(match[0]["revenue"]) * 100
                    yoys.append(round(yoy, 1))
                    latest_rev = max(latest_rev, rev_now)
        if len(yoys)<2: continue
        if any(y>300 or y<-50 for y in yoys): continue
        if yoys[-1]<5 or latest_rev<3e8: continue
        candidates.append({"stock_id":sid,"yoys":yoys,"pos_count":sum(1 for y in yoys if y>0),"latest_rev":latest_rev,"accel":yoys[-1]-yoys[-2]})
    print(f"  [S1] {len(candidates)} candidates")
    for c in candidates:
        s = 0
        yl = c["yoys"][-1]
        if yl>50: s+=5
        elif yl>30: s+=4
        elif yl>15: s+=3
        elif yl>5: s+=2
        if c["accel"]>20: s+=3
        elif c["accel"]>10: s+=2
        elif c["accel"]>0: s+=1
        if c["pos_count"]>=3: s+=3
        rb = c["latest_rev"]/1e8
        if rb>=100: s+=3
        elif rb>=10: s+=2
        elif rb>=5: s+=1
        c["rev_score"] = s
    candidates.sort(key=lambda x: x["rev_score"], reverse=True)
    top = candidates[:30]
    for c in top:
        sid = c["stock_id"]
        pd = fetch_api("TaiwanStockPrice", (today-datetime.timedelta(days=120)).strftime("%Y-%m-%d"), data_id=sid)
        time.sleep(0.3)
        c["price"],c["price_chg"],c["above_ma20"],c["above_ma60"],c["ma_bonus"] = None,0,False,False,0
        if pd and len(pd)>=5:
            closes = [float(p["close"]) for p in pd if p.get("close")]
            if len(closes)>=5:
                c["price"] = closes[-1]
                c["price_chg"] = round((closes[-1]/closes[0]-1)*100, 1)
                if len(closes)>=20: c["above_ma20"] = closes[-1] > sum(closes[-20:])/20
                if len(closes)>=60: c["above_ma60"] = closes[-1] > sum(closes[-60:])/60
                else: c["above_ma60"] = closes[-1] > sum(closes)/len(closes)
                b = (2 if c["above_ma20"] else 0) + (2 if c["above_ma60"] else 0)
                if c["price_chg"]>10: b+=1
                if c["price_chg"]>20: b+=1
                c["ma_bonus"] = b
    for c in top:
        c["total_score"] = c["rev_score"] + c.get("ma_bonus",0)
        c["name"] = name_map.get(c["stock_id"], "?")
    top.sort(key=lambda x: x["total_score"], reverse=True)
    result = []
    for i, c in enumerate(top[:15]):
        c["tier"] = "Top" if i<5 else ("Second" if i<10 else "Third")
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
    for r in rev_data: by_stock[r["stock_id"]].append(r)
    rev_map = {}
    for sid, rows in by_stock.items():
        rows.sort(key=lambda x: x["date"])
        yoys = []
        for r in rows:
            yr, mo = int(r["date"][:4]), int(r["date"][5:7])
            if yr == cur_year:
                match = [p for p in rows if int(p["date"][:4])==prev_year and int(p["date"][5:7])==mo]
                if match and float(match[0].get("revenue",0))>0:
                    yoys.append(round((float(r["revenue"])-float(match[0]["revenue"]))/float(match[0]["revenue"])*100, 1))
        if yoys: rev_map[sid] = yoys
    return rev_map

def run_strategy_2(name_map):
    print("\n" + "="*60)
    print("Strategy 2: Chip Momentum")
    print("="*60)
    today = datetime.date.today()
    start_55d = (today - datetime.timedelta(days=55)).strftime("%Y-%m-%d")
    tsmc = fetch_api("TaiwanStockPrice", start_55d, data_id="2330")
    tdays = sorted(set(p["date"] for p in tsmc))
    last35 = tdays[-35:] if len(tdays)>=35 else tdays
    print(f"  [S2] Using {len(last35)} trading days")
    latest_day = last35[-1] if last35 else today.strftime("%Y-%m-%d")
    day_data = fetch_api("TaiwanStockInstitutionalInvestorsBuySell", latest_day)
    print(f"  [S2] {latest_day}: {len(day_data)} rows")
    trust_by = defaultdict(float)
    for r in day_data:
        if r.get("name")=="Investment_Trust":
            trust_by[r["stock_id"]] += float(r.get("buy",0)) - float(r.get("sell",0))
    top_daily = sorted(trust_by.items(), key=lambda x: x[1], reverse=True)[:60]
    cids = [s for s,_ in top_daily]
    inst = {}
    for sid in cids:
        d = fetch_api("TaiwanStockInstitutionalInvestorsBuySell", start_55d, data_id=sid)
        if d: inst[sid] = d
        time.sleep(0.2)
    print(f"  [S2] History for {len(inst)} stocks")
    candidates = []
    for sid, rows in inst.items():
        dates = sorted(set(r["date"] for r in rows))
        win = dates[-35:] if len(dates)>=35 else dates
        tn, fn = 0, 0
        for r in rows:
            if r["date"] not in win: continue
            net = float(r.get("buy",0)) - float(r.get("sell",0))
            if r["name"]=="Investment_Trust": tn += net
            elif r["name"]=="Foreign_Investor": fn += net
        if tn < 500000: continue
        candidates.append({"stock_id":sid,"trust_net":tn,"foreign_net":fn})
    print(f"  [S2] {len(candidates)} stocks >= 500K")
    rev_map = _build_revenue_yoy_map()
    for c in candidates[:50]:
        sid = c["stock_id"]
        pd = fetch_api("TaiwanStockPrice", (today-datetime.timedelta(days=120)).strftime("%Y-%m-%d"), data_id=sid)
        time.sleep(0.2)
        c["price"],c["price_chg"],c["above_ma20"],c["above_ma60"] = None,0,False,False
        if pd and len(pd)>=5:
            closes = [float(p["close"]) for p in pd if p.get("close")]
            if len(closes)>=5:
                c["price"] = closes[-1]
                c["price_chg"] = round((closes[-1]/closes[0]-1)*100, 1)
                if len(closes)>=20: c["above_ma20"] = closes[-1] > sum(closes[-20:])/20
                if len(closes)>=60: c["above_ma60"] = closes[-1] > sum(closes[-60:])/60
                else: c["above_ma60"] = closes[-1] > sum(closes)/len(closes)
        c["rev_yoys"] = rev_map.get(sid, [])
        s = 0
        tn = c["trust_net"]
        if tn>=5e6: s+=5
        elif tn>=1e6: s+=4
        elif tn>=5e5: s+=3
        fn = c["foreign_net"]
        if fn>0: s+=3
        if fn>5e6: s+=1
        if c["above_ma20"]: s+=2
        if c["above_ma60"]: s+=2
        if c["price_chg"]>20: s+=2
        elif c["price_chg"]>10: s+=1
        py = sum(1 for y in c["rev_yoys"] if y>0)
        if py>=2: s+=2
        if py>=3: s+=1
        c["total_score"] = s
        c["name"] = name_map.get(sid, "?")
    candidates.sort(key=lambda x: x.get("total_score",0), reverse=True)
    result = []
    for i, c in enumerate(candidates[:15]):
        c["tier"] = "Top" if i<5 else ("Second" if i<10 else "Third")
        result.append(c)
    print(f"  [S2] {len(result)} picks")
    return result

def detect_sell_signals(name_map):
    print("\n" + "="*60)
    print(f"Sell Signal Detection (trust consec sell >= {SELL_SIGNAL_CONSECUTIVE_DAYS} days)")
    print("="*60)
    today = datetime.date.today()
    start = (today - datetime.timedelta(days=55)).strftime("%Y-%m-%d")
    tsmc = fetch_api("TaiwanStockPrice", start, data_id="2330")
    tdays = sorted(set(p["date"] for p in tsmc))
    if len(tdays) < SELL_SIGNAL_CONSECUTIVE_DAYS:
        print("[sell] Not enough days"); return []
    check_days = tdays[-(SELL_SIGNAL_CONSECUTIVE_DAYS + 2):]
    daily_trust = {}
    for day in check_days:
        dd = fetch_api("TaiwanStockInstitutionalInvestorsBuySell", day)
        tm = defaultdict(float)
        for r in dd:
            if r.get("name")=="Investment_Trust":
                tm[r["stock_id"]] += float(r.get("buy",0)) - float(r.get("sell",0))
        daily_trust[day] = tm
        print(f"  [sell] {day}: {len(tm)} stocks")
        time.sleep(0.3)
    last_n = check_days[-SELL_SIGNAL_CONSECUTIVE_DAYS:]
    all_sids = set()
    for dm in daily_trust.values(): all_sids.update(dm.keys())
    signals = []
    for sid in all_sids:
        consec = 0
        for day in last_n:
            if daily_trust.get(day,{}).get(sid,0) < 0: consec += 1
            else: consec = 0
        if consec >= SELL_SIGNAL_CONSECUTIVE_DAYS:
            total = sum(daily_trust.get(d,{}).get(sid,0) for d in last_n)
            signals.append({"stock_id":sid,"name":name_map.get(sid,"?"),"consecutive_days":consec,"total_net":total,"daily_nets":{d:daily_trust.get(d,{}).get(sid,0) for d in last_n}})
    signals.sort(key=lambda x: x["total_net"])
    signals = [s for s in signals if s["total_net"] < -100000]
    print(f"  [sell] {len(signals)} signals")
    return signals[:20]

def generate_html(s1, s2, sell):
    today_s = datetime.date.today().strftime("%Y-%m-%d")
    now_s = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    def fn(n):
        if n is None: return "-"
        if abs(n)>=1e6: return f"{n/1e6:.1f}M"
        if abs(n)>=1e3: return f"{n/1e3:.0f}K"
        return f"{n:.0f}"
    def ys(yoys):
        if not yoys: return "N/A"
        return " / ".join(f"{y:.1f}%" for y in yoys)
    def tb(tier):
        colors = {"Top":"#e74c3c","Second":"#f39c12","Third":"#3498db"}
        return f'<span class="badge" style="background:{colors.get(tier,"#999")}">{tier}</span>'
    def mi(v):
        return '<span class="ma-yes">&#10003;</span>' if v else '<span class="ma-no">&#10007;</span>'
    s1r = ""
    for c in s1:
        s1r += f'<tr><td>{tb(c.get("tier",""))}</td><td><b>{c["stock_id"]}</b></td><td>{c.get("name","?")}</td><td class="num">{c.get("total_score",0)}</td><td>{ys(c.get("yoys",[]))}</td><td class="num">{c.get("accel",0):+.1f}%</td><td class="num">{c.get("latest_rev",0)/1e8:.1f}\u5104</td><td class="num">{c.get("price") or "-"}</td><td class="num">{c.get("price_chg",0):+.1f}%</td><td class="ctr">{mi(c.get("above_ma20"))}</td><td class="ctr">{mi(c.get("above_ma60"))}</td></tr>\n'
    s2r = ""
    for c in s2:
        s2r += f'<tr><td>{tb(c.get("tier",""))}</td><td><b>{c["stock_id"]}</b></td><td>{c.get("name","?")}</td><td class="num">{c.get("total_score",0)}</td><td class="num">{fn(c.get("trust_net",0))}</td><td class="num">{fn(c.get("foreign_net",0))}</td><td class="num">{c.get("price") or "-"}</td><td class="num">{c.get("price_chg",0):+.1f}%</td><td class="ctr">{mi(c.get("above_ma20"))}</td><td class="ctr">{mi(c.get("above_ma60"))}</td><td>{ys(c.get("rev_yoys",[]))}</td></tr>\n'
    sellr = ""
    for s in sell:
        nets = s.get("daily_nets",{})
        ds = " / ".join(fn(nets[d]) for d in sorted(nets.keys()))
        sellr += f'<tr><td><b>{s["stock_id"]}</b></td><td>{s.get("name","?")}</td><td class="sr">{s["consecutive_days"]} \u5929</td><td class="sr">{fn(s["total_net"])}</td><td class="dd">{ds}</td></tr>\n'
    nosell = '<tr><td colspan="5" class="empty">\u4eca\u65e5\u7121\u8ce3\u51fa\u8b66\u793a</td></tr>'
    html = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>\u53f0\u80a1\u7b56\u7565\u89c0\u5bdf {today_s}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Noto Sans TC",sans-serif;background:#0a0a0f;color:#d0d0d0;padding:20px;max-width:1200px;margin:0 auto}}
h1{{text-align:center;font-size:22px;color:#fff;margin-bottom:4px}}
.sub{{text-align:center;color:#666;font-size:13px;margin-bottom:24px}}
.sec{{border-radius:12px;padding:20px;margin-bottom:20px}}
.s1,.s2{{background:#111828;border:1px solid #1e2d4a}}
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
<table><thead><tr><th>\u5c64\u7d1a</th><th>\u4ee3\u865f</th><th>\u540d\u7a31</th><th>\u5206\u6578</th><th>YoY</th><th>\u52a0\u901f\u5ea6</th><th>\u71df\u6536</th><th>\u80a1\u50f9</th><th>\u6f32\u5e45</th><th>MA20</th><th>MA60</th></tr></thead>
<tbody>{s1r}</tbody></table></div>
<div class="sec s2"><h2>&#x1F4B0; \u7b56\u7565\u4e8c\uff1a\u7c4c\u78bc\u52d5\u80fd</h2>
<p class="desc">\u6295\u4fe135\u65e5\u6de8\u8cb7&ge;50\u842c\u5f35 + \u5916\u8cc7\u540c\u5411 + \u5747\u7dda + \u71df\u6536 | \u6eff\u520618</p>
<table><thead><tr><th>\u5c64\u7d1a</th><th>\u4ee3\u865f</th><th>\u540d\u7a31</th><th>\u5206\u6578</th><th>\u6295\u4fe1</th><th>\u5916\u8cc7</th><th>\u80a1\u50f9</th><th>\u6f32\u5e45</th><th>MA20</th><th>MA60</th><th>YoY</th></tr></thead>
<tbody>{s2r}</tbody></table></div>
<div class="sec sell"><h2>&#x26A0;&#xFE0F; \u8ce3\u51fa\u8b66\u793a</h2>
<p class="desc">\u6295\u4fe1\u9023\u7e8c {SELL_SIGNAL_CONSECUTIVE_DAYS} \u5929\u6de8\u8ce3\u8d85 \u4e14 \u5408\u8a08>10\u842c\u5f35</p>
<table><thead><tr><th>\u4ee3\u865f</th><th>\u540d\u7a31</th><th>\u9023\u8ce3</th><th>\u5408\u8a08</th><th>\u660e\u7d30</th></tr></thead>
<tbody>{sellr if sellr else nosell}</tbody></table></div>
<div class="legend"><span>&#10003; \u7ad9\u4e0a\u5747\u7dda</span><span>&#10007; \u8dcc\u7834</span><span>\u6f32\u5e45=\u8fd180\u65e5</span><span>\u52a0\u901f\u5ea6=3\u6708YoY-2\u6708YoY</span></div>
<div class="footer">Powered by <a href="https://finmindtrade.com/">FinMind</a> | <a href="https://tetsu811.com">Tetsu</a> | \u50c5\u4f9b\u7814\u7a76\u53c3\u8003</div>
</body></html>"""
    return html

def format_line_message(s1, s2, sell):
    today_s = datetime.date.today().strftime("%Y-%m-%d")
    lines = [f"\U0001F4C8 \u53f0\u80a1\u7b56\u7565\u89c0\u5bdf {today_s}\n"]
    lines.append("=== \u7b56\u7565\u4e00\uff1a\u71df\u6536\u52d5\u80fd ===")
    for c in s1:
        lines.append(f"[{c['tier']}] {c['stock_id']} {c.get('name','?')} s={c['total_score']} YoY={'/'.join(f'{y:.0f}%' for y in c.get('yoys',[]))}")
    lines.append("\n=== \u7b56\u7565\u4e8c\uff1a\u7c4c\u78bc\u52d5\u80fd ===")
    for c in s2:
        lines.append(f"[{c['tier']}] {c['stock_id']} {c.get('name','?')} s={c.get('total_score',0)} trust={c.get('trust_net',0)/1000:.0f}K")
    if sell:
        lines.append(f"\n\u26A0\uFE0F \u8ce3\u51fa\u8b66\u793a (\u6295\u4fe1\u9023\u8ce3>={SELL_SIGNAL_CONSECUTIVE_DAYS}\u5929)")
        for s in sell[:10]:
            lines.append(f"  {s['stock_id']} {s.get('name','?')} \u9023{s['consecutive_days']}\u5929 \u6de8\u8ce3={s['total_net']/1000:.0f}K")
    lines.append(f"\n\U0001F310 https://tetsu811.github.io/tw-quant-strategies/")
    return "\n".join(lines)

def main():
    print("=" * 60)
    print(f"TW Stock Quant Strategies - {datetime.date.today()}")
    print("=" * 60)
    login_finmind()
    name_map = get_stock_names()
    s1 = run_strategy_1(name_map)
    s2 = run_strategy_2(name_map)
    sell = detect_sell_signals(name_map)
    html = generate_html(s1, s2, sell)
    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n[html] docs/index.html ({len(html)} chars)")
    results = {"date": datetime.date.today().isoformat(), "strategy_1": s1, "strategy_2": s2, "sell_signals": sell}
    with open("docs/latest.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    lt = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
    lu = os.environ.get("LINE_USER_IDS", "")
    if lt and lu:
        from line_push import push_line_message
        push_line_message(format_line_message(s1, s2, sell))
        print("[line] Pushed")
    else:
        print("[line] Skipped")
    print("\nDone!")

if __name__ == "__main__":
    main()

# test
