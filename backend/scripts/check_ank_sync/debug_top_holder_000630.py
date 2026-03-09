#!/usr/bin/env python3
"""
使用 dev_doc/akshare 接口调试 000630 十大股东数据拉取。

接口说明（见 akshare 文档）:
- stock_gdfx_free_top_10_em(symbol, date): 东方财富-个股-十大流通股东
  - symbol: 带市场标识，如 "sz000630"
  - date: 财报季度最后日，格式 "20250630"（YYYYMMDD）

用法（在 backend 目录下）:
  PYTHONPATH=. python scripts/check_ank_sync/debug_top_holder_000630.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_backend_dir = Path(__file__).resolve().parent.parent.parent
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))
os.chdir(_backend_dir)

# 与 sync 脚本一致：半年报 06-30、年报 12-31，最近 4 个报告期（当前约 2026-03 时为下式）
QUARTER_DATES_ISO = ["2025-12-31", "2025-06-30", "2024-12-31", "2024-06-30"]
# akshare 需要 YYYYMMDD
QUARTER_DATES_YMD = [d.replace("-", "") for d in QUARTER_DATES_ISO]

SYMBOL = "000630"
EM_CODE = "sz000630"  # 东财 code：深市 sz + 6 位


def try_akshare():
    """方式一：akshare stock_gdfx_free_top_10_em(symbol=sz000630, date=YYYYMMDD)。"""
    print("\n" + "=" * 60)
    print("【1】AKShare: stock_gdfx_free_top_10_em(symbol=%s, date=YYYYMMDD)" % EM_CODE)
    print("=" * 60)
    try:
        import akshare as ak
    except ImportError as e:
        print("  akshare 未安装:", e)
        return
    for date_iso, date_ymd in zip(QUARTER_DATES_ISO, QUARTER_DATES_YMD):
        print("\n  报告期 %s (date=%s):" % (date_iso, date_ymd))
        try:
            df = ak.stock_gdfx_free_top_10_em(symbol=EM_CODE, date=date_ymd)
            if df is not None and not df.empty:
                print("    行数:", len(df))
                print("    列:", list(df.columns))
                print("    前 2 行:")
                print(df.head(2).to_string())
            else:
                print("    返回空 DataFrame（该报告期可能未披露）")
        except Exception as e:
            print("    异常:", type(e).__name__, str(e))
            import traceback
            traceback.print_exc()


def try_direct_em_api():
    """方式二：直连东财 PageSDLTGD（与 check_and_sync_top_holder_latest 一致）。"""
    print("\n" + "=" * 60)
    print("【2】直连东财 PageSDLTGD: GET code=%s, date=YYYY-MM-DD" % EM_CODE)
    print("=" * 60)
    try:
        import requests
    except ImportError:
        print("  requests 未安装")
        return
    url = "https://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/PageSDLTGD"
    for date_iso in QUARTER_DATES_ISO:
        print("\n  报告期 date=%s:" % date_iso)
        try:
            r = requests.get(url, params={"code": EM_CODE, "date": date_iso}, timeout=15)
            print("    HTTP status:", r.status_code)
            j = r.json()
            if not isinstance(j, dict):
                print("    响应非 JSON 对象:", type(j))
                continue
            if "sdltgd" in j:
                rows = j["sdltgd"] or []
                print("    sdltgd 条数:", len(rows))
                if rows:
                    print("    首条键:", list(rows[0].keys()) if rows else "无")
                    print("    首条:", rows[0])
            else:
                print("    响应无 sdltgd 键。键:", list(j.keys()))
                if j.get("message"):
                    print("    message:", j.get("message"))
        except Exception as e:
            print("    异常:", type(e).__name__, str(e))
            import traceback
            traceback.print_exc()


def main():
    print("十大股东调试：000630（铜陵有色），对比 AKShare 与直连东财")
    try_akshare()
    try_direct_em_api()
    print("\n" + "=" * 60)
    print("调试结束。若 AKShare 成功而直连失败，多为请求头/限流；若都失败，多为网络或东财接口变更。")
    print("=" * 60)


if __name__ == "__main__":
    main()
