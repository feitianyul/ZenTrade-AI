#!/usr/bin/env python3
"""
财务指标独立测试脚本：拉取 AKShare 数据与 MySQL stock_financial 对比，定位「已处理很多但成功 0 失败 0」问题。

用法（在 backend 目录或项目根执行）:
  python backend/scripts/test_financial_sync_standalone.py
  python backend/scripts/test_financial_sync_standalone.py --symbols 000001,600519 --write-one

环境：不依赖 FastAPI/Worker，仅需 akshare、pymysql。
数据库：默认 127.0.0.1:3308 / root / hatech@1618 / retail_lowfreq（与 create_db 一致）。
可通过环境变量覆盖：MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
或直接设置 MYSQL_DSN（mysql+pymysql://user:pass@host:port/db）。

说明：财务指标同步仅写 MySQL 表 stock_financial，不涉及 Redis (trading_redis) 与 ClickHouse (trading_clickhouse)。
"""
from __future__ import annotations

import os
import sys
from typing import Any, Optional

# 确保能 import 项目模块
_backend = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _backend not in sys.path:
    sys.path.insert(0, _backend)
# 可选加载 .env（若存在）
_env = os.path.join(_backend, ".env")
if os.path.isfile(_env):
    with open(_env, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v

# 脚本内不强制代理，便于对比「直连 vs 代理」
for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_k, None)


def get_mysql_conn():
    """使用与 create_db 一致的 Docker MySQL：127.0.0.1:3308 root/hatech@1618 retail_lowfreq。"""
    import pymysql

    dsn = os.environ.get("MYSQL_DSN", "").strip()
    if dsn:
        # 解析 mysql+pymysql://user:pass@host:port/db 或 mysql://...
        if "mysql+pymysql://" in dsn:
            dsn = dsn.replace("mysql+pymysql://", "", 1)
        elif "mysql+asyncmy://" in dsn:
            dsn = dsn.replace("mysql+asyncmy://", "", 1)
        elif dsn.startswith("mysql://"):
            dsn = dsn.replace("mysql://", "", 1)
        # user:pass@host:port/db
        try:
            rest = dsn
            if "@" in rest:
                user_part, rest = rest.rsplit("@", 1)
                if ":" in user_part:
                    user, password = user_part.split(":", 1)
                else:
                    user, password = user_part, ""
            else:
                user, password = "", ""
            if "/" in rest:
                host_part, db = rest.rsplit("/", 1)
                db = db.split("?")[0] if "?" in db else db
            else:
                host_part, db = rest, ""
            if ":" in host_part:
                host, port = host_part.rsplit(":", 1)
                try:
                    port = int(port)
                except ValueError:
                    port = 3306
            else:
                host, port = host_part, 3306
            return pymysql.connect(
                host=host or "127.0.0.1",
                port=port,
                user=user or "root",
                password=password or os.environ.get("MYSQL_PASSWORD", "hatech@1618"),
                database=db or os.environ.get("MYSQL_DATABASE", "retail_lowfreq"),
                charset="utf8mb4",
            )
        except Exception as e:
            print("WARN: 解析 MYSQL_DSN 失败，使用默认连接:", e)
    host = os.environ.get("MYSQL_HOST", "127.0.0.1")
    port = int(os.environ.get("MYSQL_PORT", "3308"))
    user = os.environ.get("MYSQL_USER", "root")
    password = os.environ.get("MYSQL_PASSWORD", "hatech@1618")
    database = os.environ.get("MYSQL_DATABASE", "retail_lowfreq")
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
    )


def fetch_api(symbol: str):
    """直连调用 AKShare，不走应用内代理。"""
    import akshare as ak
    return ak.stock_financial_analysis_indicator(symbol=symbol)


def _safe_float(row: Any, col: str) -> Optional[float]:
    try:
        v = row.get(col)
        if v is None or str(v).strip() in ("", "nan", "NaN", "--", "-"):
            return None
        return float(v)
    except (ValueError, TypeError):
        return None


def main():
    import argparse
    parser = argparse.ArgumentParser(description="财务指标拉取与库表对比")
    parser.add_argument("--symbols", type=str, default="000001,000002,600519",
                        help="逗号分隔股票代码，默认 000001,000002,600519")
    parser.add_argument("--write-one", action="store_true",
                        help="对第一个 symbol 尝试写入一条到 stock_financial（与 sync_financial 相同逻辑）")
    args = parser.parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        symbols = ["000001"]

    print("=" * 60)
    print("财务指标独立测试：AKShare vs MySQL stock_financial")
    print("=" * 60)

    # ---------- 1. 拉取 API ----------
    print("\n【1】AKShare stock_financial_analysis_indicator 拉取（直连，无代理）")
    api_columns: Optional[list] = None
    api_first_row: Optional[dict] = None
    for sym in symbols:
        try:
            df = fetch_api(sym)
            if df is None:
                print(f"  symbol={sym!r}: 返回 None")
                continue
            if df.empty:
                print(f"  symbol={sym!r}: 返回空 DataFrame（行数=0）—— 若全部如此，sync 会显示「已处理 N」但成功 0 失败 0")
                continue
            if api_columns is None:
                api_columns = list(df.columns)
                print(f"  symbol={sym!r}: 行数={len(df)}, 列数={len(df.columns)}")
                print("  列名:", api_columns)
                first = df.iloc[0]
                api_first_row = first.to_dict() if hasattr(first, "to_dict") else {}
                print("  第一行(报告期/前几列):", {k: api_first_row.get(k) for k in list(api_first_row.keys())[:5]})
            else:
                print(f"  symbol={sym!r}: 行数={len(df)}")
        except Exception as e:
            print(f"  symbol={sym!r}: 异常 {type(e).__name__}: {e}")

    # sync_financial 使用的列名（必须与 AKShare 返回一致）
    expected_cols = [
        "报告期",  # 或第一列 iloc[0] 作为 report_date
        "净资产收益率(%)",
        "销售毛利率(%)",
        "销售净利率(%)",
        "基本每股收益(元)",
        "资产负债比率(%)",
        "流动比率",
    ]
    print("\n  sync_financial 期望的列名:", expected_cols)
    if api_columns:
        missing = [c for c in expected_cols if c not in api_columns]
        if missing:
            print("  >>> 缺失列（会导致 _safe_float 取到 None，但不至于 0 成功）:", missing)
        else:
            print("  >>> 列名匹配 OK")

    # ---------- 2. 读 MySQL ----------
    print("\n【2】MySQL stock_financial 表")
    try:
        import pymysql
        conn = get_mysql_conn()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("SELECT COUNT(*) AS cnt FROM stock_financial")
        total = cur.fetchone()["cnt"]
        print(f"  总行数: {total}")
        cur.execute(
            "SELECT symbol, report_date, roe, eps, updated_at FROM stock_financial ORDER BY updated_at DESC LIMIT 5"
        )
        rows = cur.fetchall()
        if rows:
            print("  最近 5 条:", rows)
        else:
            print("  表为空或无数据")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"  读库失败: {type(e).__name__}: {e}")
        print("  请确认 Docker panda_mysql 已启动、端口 3308、库 retail_lowfreq 存在。")

    # ---------- 3. 可选：写入一条（与 sync_financial 相同逻辑）----------
    if args.write_one and symbols and api_first_row is not None:
        print("\n【3】尝试写入一条（与 sync_financial 一致逻辑）")
        try:
            conn = get_mysql_conn()
            cur = conn.cursor()
            # 使用与 data_sync_service 相同的列名与 _safe_float
            report_date = str(api_first_row.get(api_columns[0] if api_columns else "报告期", "")) if api_columns else ""
            if not report_date and api_first_row:
                report_date = str(list(api_first_row.values())[0]) if api_first_row else ""
            roe = _safe_float(api_first_row, "净资产收益率(%)")
            gross_margin = _safe_float(api_first_row, "销售毛利率(%)")
            net_margin = _safe_float(api_first_row, "销售净利率(%)")
            eps = _safe_float(api_first_row, "基本每股收益(元)")
            debt_ratio = _safe_float(api_first_row, "资产负债比率(%)")
            current_ratio = _safe_float(api_first_row, "流动比率")
            import json
            raw_data = json.dumps(api_first_row, ensure_ascii=False) if api_first_row else None
            sym = symbols[0]
            cur.execute(
                """INSERT INTO stock_financial (symbol, report_date, roe, gross_margin, net_margin, eps, debt_ratio, current_ratio, raw_data)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE roe=VALUES(roe), raw_data=VALUES(raw_data)""",
                (sym, report_date, roe, gross_margin, net_margin, eps, debt_ratio, current_ratio, raw_data),
            )
            conn.commit()
            print(f"  已写入/更新 symbol={sym} report_date={report_date}")
            cur.close()
            conn.close()
        except Exception as e:
            print(f"  写入失败: {type(e).__name__}: {e}")

    print("\n【结论建议】")
    if api_columns is None or (api_first_row is None and not symbols):
        print("  - API 未返回任何数据或全部空 DataFrame → 检查网络/代理/东方财富限流，或 AKShare 接口变更。")
    if api_first_row is not None and total == 0:
        print("  - API 有数据但库表为空 → 正常同步应能写入；若任务里成功 0，多为每批拉取结果都为空（代理/超时导致）。")
    print("  - 「已处理 138/5484 成功 0 失败 0」：sync_financial 对 df 为空或 None 的项只 continue 不计数，异常也未累加 error_count；")
    print("    若大量 _pull_financial_one 返回空或异常，就会看到已处理数增加但成功/失败均为 0。")
    print("  - 建议：本脚本直连拉取若正常，则问题在应用内代理/超时；若本脚本也空，则问题在源站或 AKShare。")
    print()


if __name__ == "__main__":
    main()
