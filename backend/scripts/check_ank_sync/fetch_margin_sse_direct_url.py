#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直接请求上交所融资融券明细接口，检查某日是否有数据（不依赖 akshare 解析）。
数据源与 akshare stock_margin_detail_sse 一致：
  GET https://query.sse.com.cn/marketdata/tradedata/queryMargin.do
  参数: isPagination=true, tabType=mxtype, detailsDate=YYYYMMDD
  页面: https://www.sse.com.cn/market/othersdata/margin/detail/index.shtml?marginDate=YYYYMMDD
用法：
  cd backend && python scripts/check_ank_sync/fetch_margin_sse_direct_url.py
  cd backend && python scripts/check_ank_sync/fetch_margin_sse_direct_url.py 20260306 20260305
"""
from __future__ import annotations

import json
import sys

import requests

# 与 akshare stock_feature/stock_margin_sse.py 中 stock_margin_detail_sse 一致
URL = "https://query.sse.com.cn/marketdata/tradedata/queryMargin.do"
HEADERS = {
    "Referer": "https://www.sse.com.cn/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
}


def fetch_margin_detail_sse(date_yyyymmdd: str) -> dict:
    """请求上交所融资融券明细 API，返回原始 JSON。"""
    params = {
        "isPagination": "true",
        "tabType": "mxtype",
        "detailsDate": date_yyyymmdd,
        "stockCode": "",
        "beginDate": "",
        "endDate": "",
        "pageHelp.pageSize": "5000",
        "pageHelp.pageCount": "50",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": "21",
    }
    r = requests.get(URL, params=params, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json()


def main() -> None:
    if len(sys.argv) > 1:
        dates = [d.strip() for d in sys.argv[1:]]
    else:
        dates = ["20260306", "20260305"]

    print("URL:", URL)
    print("Params: tabType=mxtype, detailsDate=YYYYMMDD\n")

    for date_yyyymmdd in dates:
        print("--- date:", date_yyyymmdd, "---")
        try:
            data = fetch_margin_detail_sse(date_yyyymmdd)
        except Exception as e:
            print("  error:", e, "\n")
            continue

        print("  keys:", list(data.keys()))
        result = data.get("result")
        if result is None:
            print("  result: None or missing")
            print("  body sample:", json.dumps(data, ensure_ascii=False)[:400])
        elif isinstance(result, list):
            print("  result count:", len(result))
            if result:
                print("  first row:", result[0])
        else:
            print("  result type:", type(result).__name__, "value:", result)
        print()


if __name__ == "__main__":
    main()
