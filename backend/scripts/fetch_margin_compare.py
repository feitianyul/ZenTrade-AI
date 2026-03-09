#!/usr/bin/env python3
"""融资融券：单独拉取 akshare 并与 stock_margin_trading 表做只读对比，用于分析 Length mismatch 等问题。

不修改任务代码与数据库，仅读取。
用法（在 backend 目录下）:
  python scripts/fetch_margin_compare.py              # 使用与 sync 相同的「最近交易日」
  python scripts/fetch_margin_compare.py 20260213     # 指定日期 YYYYMMDD
"""
import os
import sys
from datetime import datetime, timedelta

# 保证从 backend 运行时能 import src（读库对比用）
_backend = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _backend not in sys.path:
    sys.path.insert(0, _backend)

# 与 sync 一致：不走代理
for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(k, None)


def _last_trading_date_str():
    """与 data_sync_service 中一致：最近一个交易日 YYYYMMDD，周末回退到周五。"""
    d = datetime.now()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def run_fetch(trade_date: str):
    """仅 akshare 拉取，不写库。"""
    import akshare as ak

    print("=" * 60)
    print("1. 使用日期（与 sync_margin 一致）")
    print("=" * 60)
    print("trade_date (YYYYMMDD):", trade_date)
    print("trade_date 格式化 (入库用):", trade_date[:4] + "-" + trade_date[4:6] + "-" + trade_date[6:])

    print("\n" + "=" * 60)
    print("2. 调用 akshare.stock_margin_detail_sse(date=...)")
    print("=" * 60)
    try:
        df = ak.stock_margin_detail_sse(date=trade_date)
        if df is None:
            print("结果: 返回 None")
            return None
        if df.empty:
            print("结果: 空 DataFrame (0 行)")
            print("列名:", list(df.columns))
            return df
        print("结果: 成功")
        print("行数:", len(df))
        print("列名:", list(df.columns))
        # sync 使用的列名
        for col in ["标的证券代码", "证券代码", "融资余额", "融资买入额", "融资偿还额", "融券余量", "融券卖出量", "融券偿还量"]:
            if col in df.columns:
                print(f"  - {col}: 存在")
            else:
                print(f"  - {col}: 缺失")
        print("\n前 3 行:")
        print(df.head(3).to_string())
        return df
    except Exception as e:
        print("异常:", type(e).__name__, str(e))
        import traceback
        traceback.print_exc()
        return None


def run_db_compare(trade_date: str):
    """只读查询 stock_margin_trading 表，与 akshare 结果对比。不写库。"""
    trade_date_fmt = trade_date[:4] + "-" + trade_date[4:6] + "-" + trade_date[6:]

    print("\n" + "=" * 60)
    print("3. 数据库 stock_margin_trading 表（只读对比）")
    print("=" * 60)

    try:
        import asyncio
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import StockMarginTrading

        async def _query():
            async for session in get_session():
                stmt = select(StockMarginTrading).where(
                    StockMarginTrading.trade_date == trade_date_fmt
                )
                result = await session.execute(stmt)
                return result.scalars().all()

        rows = asyncio.run(_query())
        print("表名: stock_margin_trading")
        print("查询条件: trade_date =", repr(trade_date_fmt))
        print("该日记录数:", len(rows))
        if rows:
            r0 = rows[0]
            print("表字段(与 sync 写入一致): symbol, trade_date, rz_balance, rz_buy, rz_repay, rq_balance, rq_sell, rq_repay, rz_rq_balance, updated_at")
            print("首条样例: symbol=%s trade_date=%s rz_balance=%s" % (getattr(r0, "symbol", None), getattr(r0, "trade_date", None), getattr(r0, "rz_balance", None)))
        else:
            print("(该日无记录)")
    except Exception as e:
        print("读库失败(可忽略，仅作对比):", type(e).__name__, str(e))


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1].isdigit() and len(sys.argv[1]) == 8:
        trade_date = sys.argv[1]
        print("使用指定日期:", trade_date)
    else:
        trade_date = _last_trading_date_str()
        print("使用 sync 相同逻辑的最近交易日:", trade_date)

    run_fetch(trade_date)
    run_db_compare(trade_date)

    print("\n" + "=" * 60)
    print("问题分析摘要（仅分析，不改任务与数据）")
    print("=" * 60)
    print("""
- Length mismatch 来源：akshare 内部 stock_margin_sse.py 对 temp_df.columns 赋 13 个列名时，
  temp_df 为空（0 列/0 行），即该交易日接口返回空数据（如当日数据未出或非交易日），
  导致 Expected axis has 0 elements, new values have 13 elements。

- 表 stock_margin_trading 字段与 sync_margin 写入一致：
  symbol, trade_date, rz_balance, rz_buy, rz_repay, rq_balance, rq_sell, rq_repay, rz_rq_balance。
  akshare 列名：标的证券代码, 融资余额, 融资买入额, 融资偿还额, 融券余量, 融券卖出量, 融券偿还量。

- 建议：用「最近一个**有数据的**交易日」重试时，可指定历史日期对比，例如：
  python scripts/fetch_margin_compare.py 20260213
""")
