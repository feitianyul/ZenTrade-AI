#!/usr/bin/env python3
"""
查询指定股票在 ClickHouse / 后端接口中的 K 线最近日期。
用法（在 backend 目录）:
  PYTHONPATH=. .venv/bin/python scripts/query_kline_latest_date.py 603124
  PYTHONPATH=. .venv/bin/python scripts/query_kline_latest_date.py 603124 --period weekly
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

_backend_dir = Path(__file__).resolve().parent.parent
_env_file = Path(os.getenv("ENV_FILE", _backend_dir / ".env"))
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=True)
    except Exception:
        pass


async def main(symbol: str, period: str) -> None:
    from src.services.data_service.kline_storage import load_kline_from_ch

    print(f"symbol={symbol} period={period}")
    bars = await load_kline_from_ch(symbol, period, count=1)
    if bars:
        latest = bars[-1]
        date_str = latest.get("date", "")
        print(f"通过后端接口(load_kline_from_ch)得到的最近日期: {date_str}")
    else:
        print("通过后端接口: 无数据")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="查询 K 线最近日期（调用后端 kline_storage）")
    parser.add_argument("symbol", help="股票代码，如 603124")
    parser.add_argument("--period", default="daily", help="周期，默认 daily")
    args = parser.parse_args()
    asyncio.run(main(args.symbol.strip(), args.period))
    sys.exit(0)
