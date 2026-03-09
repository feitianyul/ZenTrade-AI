#!/usr/bin/env python3
"""为 data_sync_tasks 表增加 updated_at 列，用于「长时间未更新」stale 判定。

用法（在 backend 目录下）:
  python scripts/migrate_data_sync_task_updated_at.py

若无法运行 Python，可直接在 MySQL 中执行:
  migrations/add_data_sync_task_updated_at.sql
"""
import asyncio
import os
import sys

_backend = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _backend not in sys.path:
    sys.path.insert(0, _backend)
os.chdir(_backend)

try:
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(_backend) / ".env", override=False)
except Exception:
    pass


async def main():
    from sqlalchemy import text
    from src.core.db import get_engine

    engine = get_engine()
    dsn = os.environ.get("MYSQL_DSN", "")

    async with engine.begin() as conn:
        if "sqlite" in dsn:
            r = await conn.execute(text("PRAGMA table_info(data_sync_tasks)"))
            rows = r.fetchall()
            names = [row[1] for row in rows]
            if "updated_at" not in names:
                await conn.execute(text(
                    "ALTER TABLE data_sync_tasks ADD COLUMN updated_at DATETIME NULL"
                ))
                print("SQLite: 已添加列 updated_at")
            else:
                print("SQLite: updated_at 已存在，跳过")
        else:
            try:
                await conn.execute(text(
                    "ALTER TABLE data_sync_tasks "
                    "ADD COLUMN updated_at DATETIME(6) NULL "
                    "COMMENT '最后活动时间，用于长时间未更新(stale)判定'"
                ))
                print("MySQL: 已添加列 updated_at")
            except Exception as e:
                if "Duplicate column" in str(e) or "1060" in str(e):
                    print("MySQL: updated_at 已存在，跳过")
                else:
                    raise

    print("迁移完成。")


if __name__ == "__main__":
    asyncio.run(main())
