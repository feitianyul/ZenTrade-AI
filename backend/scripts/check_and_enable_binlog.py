#!/usr/bin/env python3
"""查询 MySQL 是否开启 binlog；若未开启则提示如何开启（需改配置并重启）。"""
import sys

import pymysql

HOST = "127.0.0.1"
PORT = 3308
USER = "root"
PASSWORD = "hatech@1618"


def main():
    try:
        conn = pymysql.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database="mysql"
        )
    except Exception as e:
        print(f"连接失败: {e}", file=sys.stderr)
        sys.exit(1)
    with conn.cursor() as cur:
        cur.execute("SHOW VARIABLES LIKE 'log_bin'")
        row = cur.fetchone()
        log_bin = (row[1] or "").strip().upper() if row else ""
        cur.execute("SHOW VARIABLES LIKE 'server_id'")
        row2 = cur.fetchone()
        server_id = row2[1] if row2 else None
    conn.close()

    if log_bin == "ON":
        print("binlog 已开启，无需操作。")
        return
    print("binlog 未开启。")
    print("开启方式：在 MySQL 配置文件（my.ini 或 my.cnf）的 [mysqld] 下添加并重启服务：")
    print("  log_bin = mysql-bin")
    print("  server_id = 1")
    print("  binlog_format = ROW")
    print("  binlog_expire_logs_seconds = 604800")
    sys.exit(0)


if __name__ == "__main__":
    main()
