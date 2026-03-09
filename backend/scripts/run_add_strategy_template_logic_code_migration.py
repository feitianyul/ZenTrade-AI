"""
执行 strategy_templates 表 logic_code 列迁移。

用法: 在 backend 目录执行
  python scripts/run_add_strategy_template_logic_code_migration.py
"""
import asyncio
import os
import sys

try:
    import dotenv

    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(backend_dir, ".env")
    if os.path.isfile(env_path):
        dotenv.load_dotenv(env_path)
    root_env = os.path.join(backend_dir, "..", ".env")
    if os.path.isfile(root_env):
        dotenv.load_dotenv(root_env)
except Exception:
    print("WARN: .env 加载失败，继续使用当前环境变量")

if __name__ == "__main__":
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)


async def main() -> None:
    from sqlalchemy import inspect, text
    from src.core.db import get_engine

    engine = get_engine()
    async with engine.begin() as conn:
        has_table = await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).has_table("strategy_templates")
        )
        if not has_table:
            print("SKIP: strategy_templates 表不存在，无需迁移")
            return
        columns = await conn.run_sync(
            lambda sync_conn: [col["name"] for col in inspect(sync_conn).get_columns("strategy_templates")]
        )
        if "logic_code" in columns:
            print("OK: strategy_templates.logic_code 已存在，无需重复添加")
            return
        await conn.execute(
            text("ALTER TABLE strategy_templates ADD COLUMN logic_code TEXT NULL")
        )
        print("OK: strategy_templates.logic_code 已添加")


if __name__ == "__main__":
    asyncio.run(main())
