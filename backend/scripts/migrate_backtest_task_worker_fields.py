#!/usr/bin/env python3
"""为 backtest_tasks 表增加 Worker 与真实行情所需字段。

新增: request_params_json, error_detail, started_at, finished_at
修改: status 默认改为 pending，已有记录保持 completed

用法（在 backend 目录下）:
  python scripts/migrate_backtest_task_worker_fields.py
"""
import asyncio
import os
import sys

_backend = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _backend not in sys.path:
    sys.path.insert(0, _backend)
os.chdir(_backend)

# 加载 .env，使 MYSQL_DSN 指向 Docker 等配置
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
            r = await conn.execute(text("PRAGMA table_info(backtest_tasks)"))
            rows = r.fetchall()
            names = [row[1] for row in rows]
            for col, sql in [
                ("request_params_json", "ADD COLUMN request_params_json TEXT DEFAULT '{}'"),
                ("error_detail", "ADD COLUMN error_detail VARCHAR(512)"),
                ("started_at", "ADD COLUMN started_at DATETIME"),
                ("finished_at", "ADD COLUMN finished_at DATETIME"),
            ]:
                if col not in names:
                    await conn.execute(text(f"ALTER TABLE backtest_tasks {sql}"))
                    print(f"SQLite: 已添加列 {col}")
            if "status" in names:
                # SQLite 无法直接改 default，已有数据不变
                print("SQLite: status 列已存在，跳过")
        else:
            for col, sql in [
                ("request_params_json", "ADD COLUMN request_params_json JSON DEFAULT (JSON_OBJECT())"),
                ("error_detail", "ADD COLUMN error_detail VARCHAR(512) NULL"),
                ("started_at", "ADD COLUMN started_at DATETIME NULL"),
                ("finished_at", "ADD COLUMN finished_at DATETIME NULL"),
            ]:
                try:
                    await conn.execute(text(f"ALTER TABLE backtest_tasks {sql}"))
                    print(f"MySQL: 已添加列 {col}")
                except Exception as e:
                    if "Duplicate column" in str(e) or "1060" in str(e):
                        print(f"MySQL: {col} 已存在，跳过")
                    else:
                        raise
            try:
                await conn.execute(text(
                    "ALTER TABLE backtest_tasks MODIFY COLUMN status VARCHAR(32) DEFAULT 'pending'"
                ))
                print("MySQL: 已更新 status 默认值为 pending")
            except Exception as e:
                print("MySQL: status 修改跳过:", e)
            try:
                await conn.execute(text(
                    "CREATE INDEX ix_backtest_tasks_status ON backtest_tasks (status)"
                ))
                print("MySQL: 已添加 status 索引")
            except Exception as e:
                if "Duplicate" in str(e) or "1061" in str(e):
                    print("MySQL: status 索引已存在，跳过")
                else:
                    raise

    print("迁移完成。")


if __name__ == "__main__":
    asyncio.run(main())
