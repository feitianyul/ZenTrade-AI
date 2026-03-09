"""直接调用 get_institutional_data，复现接口逻辑并打印结果（使用 backend/.env 的 MYSQL_DSN）。"""
import asyncio
import os
import sys
from pathlib import Path

# 加载 backend/.env
_backend = Path(__file__).resolve().parent.parent
_env = _backend / ".env"
if _env.exists():
    from dotenv import load_dotenv
    load_dotenv(_env, override=True)
dsn = os.getenv("MYSQL_DSN", "")
print("MYSQL_DSN (masked):", dsn.replace(dsn.split("//", 1)[1].split("@", 1)[0].split(":", 1)[1], "****") if "//" in dsn and "@" in dsn else dsn or "(not set)")

# 插入 backend 为 path 以便 import src
sys.path.insert(0, str(_backend))

async def main():
    from src.services.data_service.hot_rank_service import get_institutional_data
    items, updated_at = await get_institutional_data(market="北向", indicator="今日排行")
    print("get_institutional_data result: len(items)=%s, data_updated_at=%r" % (len(items), updated_at))
    if items:
        print("first item keys:", list(items[0].keys()))
        print("first item sample:", {k: items[0][k] for k in list(items[0].keys())[:5]})
    else:
        print("(empty list - 会显示「暂无北向资金数据」)")

if __name__ == "__main__":
    asyncio.run(main())
