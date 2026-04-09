"""LINE Messaging API 推播模組"""
import os, json, requests, datetime
import pandas as pd
from tabulate import tabulate

LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_USER_IDS = os.environ.get("LINE_USER_IDS", "").split(",")
LINE_API_URL = "https://api.line.me/v2/bot/message/push"

def _push_message(user_id, messages):
    headers = {"Content-Type":"application/json","Authorization":f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"}
    body = {"to": user_id, "messages": messages}
    r = requests.post(LINE_API_URL, headers=headers, json=body, timeout=15)
    if r.status_code != 200:
        print(f"[LINE] 推送失敗 ({user_id[:8]}...): {r.status_code} {r.text}")
    else:
        print(f"[LINE] 推送成功 ({user_id[:8]}...)")

def format_strategy_result(strategy_name, df):
    today_str = datetime.date.today().strftime("%Y/%m/%d")
    header = f"📊 {strategy_name}\n📅 {today_str}\n"
    if df.empty:
        return header + "\n❌ 今日無符合條件標的"
    if "rs60" in df.columns:
        display_cols = ["stock_id","close","latest_yoy","yoy_accel","rs60"]
        col_names = ["代碼","收盤","營收YoY%","加速度","RS60"]
    else:
        display_cols = ["stock_id","close","ret_20d","trust_5d","trust_20d"]
        col_names = ["代碼","收盤","20d漲%","投信5d","投信20d"]
    available_cols = [c for c in display_cols if c in df.columns]
    table_df = df[available_cols].head(10).copy()
    rename_map = dict(zip(available_cols, col_names[:len(available_cols)]))
    table_df = table_df.rename(columns=rename_map)
    table_str = tabulate(table_df, headers="keys", tablefmt="simple", showindex=False)
    return f"{header}\n✅ 精選 {len(df)} 檔：\n\n{table_str}"

def push_results(strategy1_df, strategy2_df):
    msg1 = format_strategy_result("策略1：營收動能", strategy1_df)
    msg2 = format_strategy_result("策略2：投信認養", strategy2_df)
    full_msg = msg1 + "\n\n" + "─"*28 + "\n\n" + msg2
    if len(full_msg) > 4900:
        messages = [{"type":"text","text":msg1[:4900]},{"type":"text","text":msg2[:4900]}]
    else:
        messages = [{"type":"text","text":full_msg}]
    for uid in LINE_USER_IDS:
        uid = uid.strip()
        if uid: _push_message(uid, messages)

if __name__ == "__main__":
    test_df = pd.DataFrame({"stock_id":["2330","2454"],"close":[580.0,950.0],"latest_yoy":[25.3,18.7],"yoy_accel":[5.1,3.2],"rs60":[32.5,28.1]})
    print(format_strategy_result("策略1：營收動能", test_df))
