#!/usr/bin/env python3
"""临时脚本：查询 exchange_trading_dates 表，排查 2026-01/02 无数据问题。"""
import os
import pymysql

def main():
    conn = pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3308")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "hatech@1618"),
        database=os.getenv("DB_NAME", "retail_lowfreq"),
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM exchange_trading_dates"
            )
            row = cur.fetchone()
            print("exchange_trading_dates: min=%s max=%s count=%s" % (row[0], row[1], row[2]))
            for prefix in ("2026-01", "2026-02", "2026-03"):
                cur.execute(
                    "SELECT trade_date FROM exchange_trading_dates WHERE trade_date LIKE %s ORDER BY trade_date LIMIT 5",
                    (prefix + "%",),
                )
                rows = cur.fetchall()
                print("%s: sample=%s" % (prefix, [r[0] for r in rows]))
            # 与 API 相同的条件：2026年1月
            cur.execute(
                "SELECT trade_date FROM exchange_trading_dates WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date",
                ("2026-01-01", "2026-01-31"),
            )
            jan = cur.fetchall()
            print("API-style 2026-01 (>=2026-01-01 <=2026-01-31): count=%d sample=%s" % (len(jan), [r[0] for r in jan[:5]]))
            cur.execute(
                "SELECT trade_date FROM exchange_trading_dates WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date",
                ("2026-02-01", "2026-02-28"),
            )
            feb = cur.fetchall()
            print("API-style 2026-02 (>=2026-02-01 <=2026-02-28): count=%d sample=%s" % (len(feb), [r[0] for r in feb[:5]]))
    finally:
        conn.close()

if __name__ == "__main__":
    main()
