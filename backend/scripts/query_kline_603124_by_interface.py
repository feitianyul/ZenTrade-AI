#!/usr/bin/env python3
"""
按 dev_doc/akshare 与 data_sync 使用的 K 线接口，逐接口查询 603124 的最近 K 线日期。

K 线走的接口（参见 dev_doc/akshare接口与现有对接规划.md）：
  1. stock_zh_a_hist（东财）— 日/周/月，data_sync 主源之一，落库
  2. stock_zh_a_hist_tx（腾讯）— 日 K，data_sync 备用源，落库
  3. stock_zh_a_hist_min_em（东财）— 分钟 K，不落库，仅实时/缓存
  4. stock_zh_a_minute（新浪）— 分钟 K，不落库

用法（在 backend 目录）:
  PYTHONPATH=. python scripts/query_kline_603124_by_interface.py
  # Windows PowerShell:
  $env:PYTHONPATH="."; python scripts/query_kline_603124_by_interface.py
"""
from __future__ import annotations

import sys
from datetime import datetime

# 603124 上交所，东财用 6 位，腾讯用 sh603124
SYMBOL_6 = "603124"
SYMBOL_TX = "sh603124"
# 查询近期区间，便于拿到“最近日期”
END = datetime.now().strftime("%Y%m%d")
START = "20250101"


def _latest_date_from_df(df, date_col=None):
    if df is None or df.empty:
        return None
    if date_col is None:
        if "日期" in df.columns:
            date_col = "日期"
        elif "date" in df.columns:
            date_col = "date"
        else:
            date_col = df.columns[0]
    if date_col not in df.columns:
        return None
    last = df[date_col].iloc[-1]
    return str(last).split(" ")[0].strip() if last is not None else None


def query_stock_zh_a_hist(period: str = "daily"):
    """1. 东财 stock_zh_a_hist — 日/周/月 K 线（data_sync 主源之一）。"""
    try:
        import akshare as ak
    except ImportError:
        return "akshare 未安装"
    try:
        df = ak.stock_zh_a_hist(symbol=SYMBOL_6, period=period, start_date=START, end_date=END, adjust="")
        d = _latest_date_from_df(df)
        return d if d else "无数据"
    except Exception as e:
        return f"异常: {e}"


def query_stock_zh_a_hist_tx():
    """2. 腾讯 stock_zh_a_hist_tx — 日 K（data_sync 备用源，无 period 参数，仅日 K）。"""
    try:
        import akshare as ak
    except ImportError:
        return "akshare 未安装"
    try:
        df = ak.stock_zh_a_hist_tx(symbol=SYMBOL_TX, start_date=START, end_date=END, adjust="")
        d = _latest_date_from_df(df, "date")
        return d if d else "无数据"
    except Exception as e:
        return f"异常: {e}"


def query_stock_zh_a_hist_min_em():
    """3. 东财 stock_zh_a_hist_min_em — 分钟 K（不落库，仅看接口最近一条时间）。"""
    try:
        import akshare as ak
    except ImportError:
        return "akshare 未安装"
    try:
        # period: "1"/"5"/"15"/"30"/"60"
        df = ak.stock_zh_a_hist_min_em(symbol=SYMBOL_6, period="1", adjust="")
        if df is None or df.empty:
            return "无数据"
        if "时间" in df.columns:
            last = df["时间"].iloc[-1]
        else:
            last = df.iloc[-1].name
        return str(last).split(" ")[0].strip() if last is not None else "无时间列"
    except Exception as e:
        return f"异常: {e}"


def query_stock_zh_a_minute():
    """4. 新浪 stock_zh_a_minute — 分钟 K（可选，不落库）。"""
    try:
        import akshare as ak
    except ImportError:
        return "akshare 未安装"
    try:
        df = ak.stock_zh_a_minute(symbol=SYMBOL_TX, period="1", adjust="")
        if df is None or df.empty:
            return "无数据"
        if "day" in df.columns:
            last = df["day"].iloc[-1]
        else:
            last = df.iloc[-1].name
        return str(last).split(" ")[0].strip() if last is not None else "无时间列"
    except Exception as e:
        return f"异常: {e}"


def main():
    try:
        import akshare  # noqa: F401 先检查是否安装
    except ImportError:
        print("错误: 当前环境未安装 akshare，无法调用 K 线接口。")
        print("请先安装: pip install akshare")
        print("若使用虚拟环境，请先激活再安装，例如:")
        print("  source /opt/trading/backend/.venv/bin/activate  # 或你项目中的 venv 路径")
        print("  pip install akshare")
        sys.exit(1)

    print("symbol=603124 (上交所), 查询各 K 线接口返回的最近日期")
    print("=" * 60)

    print("\n1. stock_zh_a_hist（东财，日/周/月）")
    for p in ("daily", "weekly", "monthly"):
        d = query_stock_zh_a_hist(period=p)
        print(f"   period={p} 最近日期: {d}")

    print("\n2. stock_zh_a_hist_tx（腾讯，仅日K）")
    d2 = query_stock_zh_a_hist_tx()
    print(f"   sh603124 最近日期: {d2}")

    print("\n3. stock_zh_a_hist_min_em（东财，1 分钟 K）")
    d3 = query_stock_zh_a_hist_min_em()
    print(f"   603124 最近时间(日期): {d3}")

    print("\n4. stock_zh_a_minute（新浪，1 分钟 K）")
    d4 = query_stock_zh_a_minute()
    print(f"   sh603124 最近时间(日期): {d4}")

    print("\n" + "=" * 60)
    print("说明: 日/周/月落库走 1、2；分钟 K 不落库，仅接口探测。")


if __name__ == "__main__":
    main()
    sys.exit(0)
