"""
执行 strategies 表 logic_desc 列迁移。
解决: (1054, "Unknown column 'logic_desc' in 'field list'")
用法: 在 backend 目录执行
  python scripts/run_add_logic_desc_migration.py
或（若在项目根）:
  python backend/scripts/run_add_logic_desc_migration.py
需设置 MYSQL_DSN 或 backend/.env 中存在 MYSQL_DSN。
"""
import asyncio
import os
import sys

# 确保 backend 在 path 且加载 .env
try:
    import dotenv
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(backend_dir, ".env")
    if os.path.isfile(env_path):
        dotenv.load_dotenv(env_path)
    # 项目根 .env
    root_env = os.path.join(backend_dir, "..", ".env")
    if os.path.isfile(root_env):
        dotenv.load_dotenv(root_env)
except Exception:
    pass

# 先加 backend 到 path 再导 db
if __name__ == "__main__":
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)


async def main():
    from sqlalchemy import text
    from src.core.db import get_engine

    engine = get_engine()
    async with engine.begin() as conn:
        try:
            await conn.execute(text("ALTER TABLE strategies ADD COLUMN logic_desc TEXT NULL"))
            print("OK: strategies.logic_desc 已添加")
        except Exception as e:
            msg = str(e).lower()
            if "duplicate column" in msg or "1060" in msg:
                print("OK: logic_desc 列已存在，无需重复添加")
            else:
                raise


if __name__ == "__main__":
    asyncio.run(main())
