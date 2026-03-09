#!/usr/bin/env python3
"""十大股东：对比 AKShare 接口返回列名/格式 与 同步代码期望列名、数据库表结构。

用法（在 backend 目录下）:
  python scripts/check_top_holder_format.py [symbol]
  symbol 默认 000001

输出:
  【1】API 实际列名、类型、首行样例（若拉取成功）
  【2】同步代码期望的 API 列名 -> 数据库字段，并标出 API 是否包含该列
  【3】数据库表 stock_top_holders 结构（来自模型）
  【4】库中已有数据样例（若 MySQL 可达）
  【5】小结：若 API 报 KeyError（如 'sdltgd'）= 东财返回结构变更，AKShare 未适配，非本仓库列名问题

当前已知: KeyError 'sdltgd' 来自 akshare/stock_feature/stock_gdfx_em.py 第 414 行，
东财 JSON 已无 sdltgd 键，导致十大股东全量 100 只全失败。
"""
import os
import sys

# 确保 backend 为工作目录并可导入 src
_backend = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _backend not in sys.path:
    sys.path.insert(0, _backend)
os.chdir(_backend)

# 东财接口 PageSDLTGD 返回 sdltgd 的英文字段 -> 数据库 stock_top_holders 字段（与 data_sync_service + market_sync.StockTopHolder 一致）
API_TO_DB = {
    "END_DATE": "report_date",
    "HOLDER_RANK": "rank",
    "HOLDER_NAME": "holder_name",
    "HOLDER_TYPE": "holder_type",
    "HOLD_NUM": "hold_count",
    "FREE_HOLDNUM_RATIO": "hold_ratio",
    "HOLD_NUM_CHANGE": "change_type",
    "CHANGE_RATIO": "change_ratio",
}
# 数据库表 stock_top_holders 字段（与模型一致）
DB_COLUMNS_ORDER = [
    "id", "symbol", "report_date", "holder_type", "rank",
    "holder_name", "hold_count", "hold_ratio", "change_type", "change_count", "change_ratio", "updated_at",
]


def fetch_api(symbol: str):
    import akshare as ak
    return ak.stock_gdfx_free_top_10_em(symbol=symbol)


def main():
    symbol = sys.argv[1] if len(sys.argv) >= 2 else "000001"
    print("=" * 60)
    print("十大股东 格式对比：API vs 同步代码期望 vs 数据库")
    print("symbol =", symbol)
    print("=" * 60)

    # ---------- 1. 拉取 API 实际返回 ----------
    print("\n【1】AKShare 接口 stock_gdfx_free_top_10_em(symbol=%s)" % symbol)
    try:
        df = fetch_api(symbol)
    except Exception as e:
        print("  拉取失败:", type(e).__name__, str(e))
        print("  完整 traceback（便于定位是 AKShare 内部还是列名）:")
        import traceback
        traceback.print_exc()
        df = None
        if "KeyError" in type(e).__name__ or "key" in str(e).lower():
            print("  说明: KeyError 多为 AKShare 或东财接口返回结构变更，导致解析时缺键。")

    api_columns = []
    api_first_row = {}
    if df is not None and not df.empty:
        api_columns = list(df.columns)
        print("  实际列名 (%d 个):" % len(api_columns))
        for i, c in enumerate(api_columns):
            print("    [%d] %r" % (i, c))
        print("  列类型 (dtype):")
        for c in api_columns:
            print("    %s -> %s" % (repr(c), df[c].dtype))
        first = df.iloc[0]
        api_first_row = {c: first[c] for c in api_columns}
        print("  首行样例 (第1行):")
        for c in api_columns:
            print("    %s = %r" % (c, api_first_row.get(c)))
    else:
        print("  无数据 (df 为空或 None)")

    # ---------- 2. 东财接口英文字段 -> 数据库字段 ----------
    print("\n【2】东财接口 sdltgd 英文字段 -> 数据库字段")
    expected = list(API_TO_DB.keys())
    missing_in_api = [k for k in expected if k not in (api_columns or [])]
    extra_in_api = [c for c in (api_columns or []) if c not in expected]

    print("  API 列名 -> 数据库字段:")
    for api_col, db_col in API_TO_DB.items():
        in_api = api_col in (api_columns or [])
        status = "✓" if in_api else "✗ 缺失"
        print("    %s -> %s  %s" % (repr(api_col), db_col, status))
    if missing_in_api:
        print("  差异: 期望但 API 未返回的列:", missing_in_api)
    if extra_in_api:
        print("  API 多出的列:", extra_in_api)

    # ---------- 3. 数据库表结构（从模型读） ----------
    print("\n【3】数据库表 stock_top_holders 结构（来自模型）")
    try:
        from src.models.market_sync import StockTopHolder
        from sqlalchemy import inspect
        mapper = inspect(StockTopHolder)
        for col in mapper.columns:
            print("    %s: %s (nullable=%s)" % (col.name, col.type, col.nullable))
    except Exception as e:
        print("  无法加载模型:", type(e).__name__, str(e))

    # ---------- 4. 若库中有数据，打印一条样例 ----------
    print("\n【4】库中已有数据样例 (symbol=%s)" % symbol)
    try:
        from src.core.db import get_session
        from src.models.market_sync import StockTopHolder
        from sqlalchemy import select

        async def _query():
            async for session in get_session():
                r = await session.execute(
                    select(StockTopHolder).where(StockTopHolder.symbol == symbol).limit(1)
                )
                return r.scalar_one_or_none()

        import asyncio
        row = asyncio.run(_query())
        if row is not None:
            print("  存在至少一条记录，样例:")
            for col in DB_COLUMNS_ORDER:
                if hasattr(row, col):
                    print("    %s = %r" % (col, getattr(row, col)))
        else:
            print("  当前无该 symbol 的记录（表可能为空）")
    except Exception as e:
        print("  读库失败:", type(e).__name__, str(e))

    # ---------- 5. 小结：列名是否一致 / API 失败原因 ----------
    print("\n【5】小结")
    if not api_columns and df is None:
        print("  >>> API 拉取失败。若为 KeyError 'sdltgd':")
        print("      原因在 AKShare 内部: 东财接口返回的 JSON 已无键 'sdltgd'，见 akshare/stock_feature/stock_gdfx_em.py。")
        print("      与同步代码的「列名」无关，需升级/修补 AKShare 或换数据源。")
    if missing_in_api and api_columns:
        print("  >>> 列名不一致: 同步代码期望的列在 API 中缺失:", missing_in_api)
        print("     需在 data_sync_service 中改用上表【1】实际列名。")
    elif api_columns and set(expected).issubset(set(api_columns)):
        print("  >>> API 列名与代码期望一致，可排除列名导致的全量失败。")

    print()


if __name__ == "__main__":
    main()
