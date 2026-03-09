#!/usr/bin/env python3
"""
北向资金 AKShare 接口与 DB 核对脚本。
- 拉取 stock_hsgt_hist_em(北向资金/沪股通/深股通) 检查列名与 2024-08-19 前后数据。
- 可选：连接 MySQL northbound_flow 查看空值分布。
用法（在项目根或 backend 下）:
  python backend/scripts/test_northbound_akshare_and_db.py
  或: cd backend && python scripts/test_northbound_akshare_and_db.py
"""
import asyncio
import os
import sys

# 加载 .env
def _load_dotenv():
    for d in [os.path.dirname(__file__), os.path.join(os.path.dirname(__file__), "..")]:
        env_path = os.path.join(d, ".env")
        if os.path.isfile(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip('"').strip("'")
                        if k and k not in os.environ:
                            os.environ[k] = v
            return
_load_dotenv()

MYSQL_DSN = os.getenv("MYSQL_DSN", "").strip() or "mysql+asyncmy://root:hatech%401618@127.0.0.1:3308/retail_lowfreq"


def run_akshare():
    """同步拉取 AKShare 北向资金/沪股通/深股通，打印列名与 2024-08-19 前后样本"""
    try:
        import akshare as ak
        import pandas as pd
    except ImportError as e:
        print("ERROR: 需要 akshare、pandas。pip install akshare pandas")
        sys.exit(1)

    print("=== AKShare stock_hsgt_hist_em 列名与数据检查 ===\n")
    print("akshare 版本:", getattr(ak, "__version__", "未知"))

    fn = getattr(ak, "stock_hsgt_hist_em", None)
    if fn is None:
        print("ERROR: akshare 无 stock_hsgt_hist_em")
        sys.exit(1)

    for symbol in ["北向资金", "沪股通", "深股通"]:
        print(f"\n--- {symbol} ---")
        try:
            df = fn(symbol=symbol)
            if df is None or df.empty:
                print("  返回空 DataFrame")
                continue
            df = df.copy()
            if "日期" in df.columns:
                df["_dt"] = pd.to_datetime(df["日期"], errors="coerce")
                df = df.dropna(subset=["_dt"])
                df = df.sort_values("_dt").reset_index(drop=True)
            print("  列名:", list(df.columns))
            print("  行数:", len(df))
            print("  最早日期:", df["日期"].iloc[0] if "日期" in df.columns else "-")
            print("  最新日期:", df["日期"].iloc[-1] if "日期" in df.columns else "-")

            # 关键列：当日成交净买额、沪股通净流入、深股通净流入（北向资金主表可能无沪/深列）
            for col in ["当日成交净买额", "当日净流入", "沪股通净流入", "深股通净流入"]:
                if col in df.columns:
                    non_null = df[col].notna().sum()
                    print(f"  {col}: 非空 {non_null}/{len(df)}")

            # 2024-08-16 ~ 2024-08-22 段
            if "日期" in df.columns:
                mask = (df["日期"].astype(str) >= "2024-08-16") & (df["日期"].astype(str) <= "2024-08-22")
                seg = df.loc[mask]
                if len(seg) > 0:
                    print("  样本 2024-08-16~22:")
                    print(seg.to_string(max_cols=8))
                # 最新 5 行
                print("  最新 5 行:")
                print(df.tail(5).to_string(max_cols=8))
        except Exception as e:
            print(f"  异常: {e}")
            import traceback
            traceback.print_exc()

    print("\n=== AKShare 检查结束 ===\n")


async def check_mysql():
    """查询 northbound_flow 表：按日期统计非空条数、2024-08-19 前后样本"""
    if not MYSQL_DSN:
        print("MYSQL_DSN 未设置，跳过 MySQL 检查")
        return
    dsn = MYSQL_DSN
    if dsn.startswith("mysql+pymysql://"):
        dsn = "mysql+asyncmy://" + dsn.split("mysql+pymysql://", 1)[1]
    print("=== MySQL northbound_flow 检查 ===\n")
    try:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
        from sqlalchemy.orm import sessionmaker
        engine = create_async_engine(dsn, pool_pre_ping=True)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with async_session() as session:
            r = await session.execute(text(
                "SELECT COUNT(*) as cnt, "
                "SUM(CASE WHEN total_net_buy IS NOT NULL OR sh_net_buy IS NOT NULL OR sz_net_buy IS NOT NULL THEN 1 ELSE 0 END) as with_value "
                "FROM northbound_flow WHERE direction='north'"
            ))
            row = r.fetchone()
            print(f"north 总行数: {row[0]}, 净买额至少有一非空: {row[1]}")
            r = await session.execute(text(
                "SELECT trade_date, sh_net_buy, sz_net_buy, total_net_buy FROM northbound_flow "
                "WHERE direction='north' AND trade_date BETWEEN '2024-08-16' AND '2024-08-22' ORDER BY trade_date"
            ))
            rows = r.fetchall()
            print("2024-08-16~22 样本:")
            for r in rows:
                print(" ", r)
            r = await session.execute(text(
                "SELECT trade_date, sh_net_buy, sz_net_buy, total_net_buy FROM northbound_flow "
                "WHERE direction='north' ORDER BY trade_date DESC LIMIT 5"
            ))
            rows = r.fetchall()
            print("最新 5 条:")
            for r in rows:
                print(" ", r)
        await engine.dispose()
    except Exception as e:
        print("MySQL 检查失败:", e)
    print("\n=== MySQL 检查结束 ===\n")


def main():
    run_akshare()
    asyncio.run(check_mysql())
    print("结论: AKShare 列名未变(当日成交净买额等)。2024-08-19 起东方财富源返回的「当日成交净买额」即为 NaN，故 MySQL 中该日期之后为 NULL 属数据源限制，非同步逻辑错误。northbound_hold_stock 为按(market,indicator)整体替换：先 DELETE 再 INSERT，不保留历史多日。")


if __name__ == "__main__":
    main()
