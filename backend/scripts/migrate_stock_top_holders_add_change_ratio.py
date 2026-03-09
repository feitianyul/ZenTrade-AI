#!/usr/bin/env python3
"""为 stock_top_holders 表增加 change_ratio 列并扩展 holder_type 长度，与东财接口一致。
已有库需执行本脚本或手动 ALTER；新库由 create_all 直接建表即可。

用法（在 backend 目录下）:
  python scripts/migrate_stock_top_holders_add_change_ratio.py

若无法运行 Python，可直接在 MySQL 中执行:
  migrations/add_change_ratio_stock_top_holders.sql
"""
import asyncio
import os
import sys

_backend = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _backend not in sys.path:
    sys.path.insert(0, _backend)
os.chdir(_backend)


async def main():
    from sqlalchemy import text
    from src.core.db import get_engine

    engine = get_engine()
    dsn = os.environ.get("MYSQL_DSN", "")

    async with engine.begin() as conn:
        if "sqlite" in dsn:
            # SQLite: 无 MODIFY，需检查列是否存在
            r = await conn.execute(text("PRAGMA table_info(stock_top_holders)"))
            rows = r.fetchall()
            names = [row[1] for row in rows]
            if "change_ratio" not in names:
                await conn.execute(text("ALTER TABLE stock_top_holders ADD COLUMN change_ratio FLOAT NULL"))
                print("SQLite: 已添加列 change_ratio")
            else:
                print("SQLite: change_ratio 已存在，跳过")
        else:
            try:
                await conn.execute(text("ALTER TABLE stock_top_holders ADD COLUMN change_ratio FLOAT NULL"))
                print("MySQL: 已添加列 change_ratio")
            except Exception as e:
                if "Duplicate column" in str(e) or "1060" in str(e):
                    print("MySQL: change_ratio 已存在，跳过")
                else:
                    raise
            try:
                await conn.execute(text("ALTER TABLE stock_top_holders MODIFY COLUMN holder_type VARCHAR(64)"))
                print("MySQL: 已扩展 holder_type 为 VARCHAR(64)")
            except Exception as e:
                print("MySQL: holder_type 修改跳过:", e)

    print("迁移完成。")


if __name__ == "__main__":
    asyncio.run(main())
