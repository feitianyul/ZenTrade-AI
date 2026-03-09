#!/usr/bin/env python3
"""
将本地 ClickHouse market_kline 表导出为 TSV，供上传到云端后导入。
用法:
  1) 本机执行（确保本地 trading_clickhouse 或 ClickHouse 在 8123 端口）:
     set CLICKHOUSE_URL=http://127.0.0.1:8123
     python backend/scripts/sync_clickhouse_to_cloud.py
  2) 会生成 backend/scripts/clickhouse_export.tsv
  3) 上传该文件到云端后执行云端导入脚本。
"""
import os
import sys

def main():
    try:
        import httpx
    except ImportError:
        print("pip install httpx")
        sys.exit(1)

    url = os.getenv("CLICKHOUSE_URL", "http://127.0.0.1:8123").rstrip("/")
    db = os.getenv("CLICKHOUSE_DB", "default")
    out = os.path.join(os.path.dirname(__file__), "clickhouse_export.tsv")

    query = "SELECT * FROM market_kline FORMAT TabSeparatedWithNames"
    try:
        with httpx.Client(timeout=120.0) as client:
            r = client.post(
                f"{url}/",
                params={"database": db, "query": query},
            )
            r.raise_for_status()
        with open(out, "wb") as f:
            f.write(r.content)
        rows = r.text.count("\n") - 1 if r.text else 0
        print(f"Exported to {out} ({rows} rows, {len(r.content)} bytes)")
        return 0
    except Exception as e:
        print("Export failed:", e, file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
