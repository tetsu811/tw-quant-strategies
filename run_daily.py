"""每日 Orchestrator — 依序執行策略 1 + 策略 2，彙整結果後 LINE 推播"""
import sys, datetime, traceback
import pandas as pd
from strategy_1_momentum import run as run_strategy1
from strategy_2_chip import run as run_strategy2
from line_push import push_results

def main():
    print("="*50)
    print(f"🚀 台股量化策略 — {datetime.date.today()}")
    print("="*50)
    try:
        print("\n📈 執行策略 1：營收動能...")
        s1_result = run_strategy1()
        print(f"   → 策略 1 完成，{len(s1_result)} 檔入選\n")
    except Exception as e:
        print(f"   ❌ 策略 1 失敗：{e}"); traceback.print_exc(); s1_result = pd.DataFrame()
    try:
        print("📊 執行策略 2：投信認養...")
        s2_result = run_strategy2()
        print(f"   → 策略 2 完成，{len(s2_result)} 檔入選\n")
    except Exception as e:
        print(f"   ❌ 策略 2 失敗：{e}"); traceback.print_exc(); s2_result = pd.DataFrame()
    print("📱 推播 LINE 通知...")
    try:
        push_results(s1_result, s2_result)
        print("   → LINE 推播完成\n")
    except Exception as e:
        print(f"   ❌ LINE 推播失敗：{e}"); traceback.print_exc()
    print("="*50)
    print("✅ 全部完成！")
    if not s1_result.empty:
        print(f"\n【策略1】{len(s1_result)} 檔：{', '.join(s1_result['stock_id'].tolist()[:10])}")
    if not s2_result.empty:
        print(f"【策略2】{len(s2_result)} 檔：{', '.join(s2_result['stock_id'].tolist()[:10])}")

if __name__ == "__main__":
    main()
