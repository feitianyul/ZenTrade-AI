"""临时脚本：连接指定 MySQL，查看 northbound_hold_stock / northbound_flow 数据，对比接口预期。"""
import os
import sys

# 使用用户提供的连接信息（端口 3308）
host = os.getenv("DB_HOST", "127.0.0.1")
port = int(os.getenv("DB_PORT", "3308"))
user = os.getenv("DB_USER", "root")
password = os.getenv("DB_PASSWORD", "hatech@1618")
database = os.getenv("DB_NAME", "retail_lowfreq")

def main():
    try:
        import pymysql
    except ImportError:
        print("请安装 pymysql: pip install pymysql")
        sys.exit(1)

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
    )
    print(f"已连接 {host}:{port}/{database}\n")

    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        # ---------- northbound_hold_stock ----------
        print("=== northbound_hold_stock ===")
        cur.execute("SELECT COUNT(*) AS cnt FROM northbound_hold_stock")
        row = cur.fetchone()
        print(f"总行数: {row['cnt']}")

        cur.execute("""
            SELECT market, indicator, trade_date, COUNT(*) AS cnt
            FROM northbound_hold_stock
            GROUP BY market, indicator, trade_date
            ORDER BY trade_date DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        print("(market, indicator, trade_date) 组合及行数:")
        for r in rows:
            print(f"  market={repr(r['market'])}, indicator={repr(r['indicator'])}, trade_date={repr(r['trade_date'])}, cnt={r['cnt']}")

        # 接口查询条件: market='北向', indicator='今日排行'
        cur.execute("""
            SELECT COUNT(*) AS cnt, MAX(trade_date) AS max_date
            FROM northbound_hold_stock
            WHERE market = '北向' AND indicator = '今日排行'
        """)
        r = cur.fetchone()
        print(f"\nWHERE market='北向' AND indicator='今日排行' -> cnt={r['cnt']}, max_date={repr(r['max_date'])}")

        if r["cnt"] and r["cnt"] > 0:
            cur.execute("""
                SELECT id, trade_date, market, indicator, code, name, close, hold_value
                FROM northbound_hold_stock
                WHERE market = '北向' AND indicator = '今日排行' AND trade_date = %s
                ORDER BY hold_value DESC
                LIMIT 3
            """, (r["max_date"],))
            sample = cur.fetchall()
            print("该日 3 条样本:")
            for s in sample:
                print(f"  {s}")
        else:
            # 可能字段值有空格或编码差异，看原始 distinct
            cur.execute("SELECT DISTINCT market FROM northbound_hold_stock LIMIT 10")
            print("DISTINCT market 样本:", [x["market"] for x in cur.fetchall()])
            cur.execute("SELECT DISTINCT indicator FROM northbound_hold_stock LIMIT 10")
            print("DISTINCT indicator 样本:", [x["indicator"] for x in cur.fetchall()])

        # ---------- northbound_flow ----------
        print("\n=== northbound_flow ===")
        cur.execute("SELECT COUNT(*) AS cnt FROM northbound_flow")
        row = cur.fetchone()
        print(f"总行数: {row['cnt']}")
        cur.execute("""
            SELECT trade_date, direction, total_net_buy
            FROM northbound_flow
            ORDER BY trade_date DESC
            LIMIT 5
        """)
        for r in cur.fetchall():
            print(f"  {r}")

    conn.close()

    # 提示：后端若未用 3308，会连到别的库导致“无数据”
    try:
        from pathlib import Path
        env_path = Path(__file__).resolve().parent.parent / ".env"
        if env_path.exists():
            dsn = None
            for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if line.startswith("MYSQL_DSN="):
                    dsn = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
            if dsn:
                # 隐藏密码
                if "@" in dsn and "://" in dsn:
                    pre = dsn.split("//", 1)[0] + "//"
                    rest = dsn.split("//", 1)[1]
                    if "@" in rest:
                        user_part, host_part = rest.rsplit("@", 1)
                        if ":" in user_part:
                            u, _ = user_part.split(":", 1)
                            user_part = u + ":****"
                        safe_dsn = pre + user_part + "@" + host_part
                    else:
                        safe_dsn = dsn
                else:
                    safe_dsn = dsn
                print("\n后端 .env 中 MYSQL_DSN（密码已隐藏）:", safe_dsn)
                if "3308" not in dsn:
                    print("  -> 当前库为 3308，若 DSN 为 3306 会连到不同库，请改为 3308 后重启后端")
            else:
                print("\n未在 .env 中找到 MYSQL_DSN，后端将使用默认（多为 3306）")
        else:
            print("\n未找到 backend/.env，请确认 MYSQL_DSN 指向 127.0.0.1:3308")
    except Exception as e:
        print("\n检查 .env 时出错:", e)

    print("\n完成")

if __name__ == "__main__":
    main()
