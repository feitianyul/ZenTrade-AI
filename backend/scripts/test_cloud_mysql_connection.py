#!/usr/bin/env python3
"""从本机测试连接云端 MySQL（49.235.165.113:3308）。"""
import sys

try:
    import pymysql
except ImportError:
    print("FAIL: pymysql not installed. Run: pip install pymysql")
    sys.exit(1)

def main():
    host = "49.235.165.113"
    port = 3308
    user = "root"
    password = "hatech@1618"
    database = "retail_lowfreq"
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            print("OK: MySQL connection from local to cloud succeeded. Result:", cur.fetchone())
            cur.execute("SELECT DATABASE()")
            print("Current database:", cur.fetchone())
        conn.close()
        return 0
    except Exception as e:
        print("FAIL:", type(e).__name__, str(e))
        return 1

if __name__ == "__main__":
    sys.exit(main())
