#!/usr/bin/env python3
"""仅创建 stock_peer_comparison 表（同行比较）。不修改其他表。"""
import asyncio
import os
import sys

# 允许从 backend 或 backend/scripts 运行
_scripts_dir = os.path.dirname(os.path.abspath(__file__))
_backend = os.path.dirname(_scripts_dir)
sys.path.insert(0, _backend)
os.chdir(_backend)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_backend, ".env"), override=False)
except ImportError:
    pass

from src.models.base import Base
from src.models.market_sync import StockPeerComparison


async def main() -> None:
    try:
        from src.core.db import get_engine
        engine = get_engine()
    except Exception:
        dsn = os.getenv("MYSQL_DSN")
        if not dsn:
            print("请设置环境变量 MYSQL_DSN，或在 backend 目录下配置 .env")
            sys.exit(1)
        from sqlalchemy.ext.asyncio import create_async_engine
        if dsn.startswith("mysql+pymysql://"):
            dsn = "mysql+asyncmy://" + dsn.split("mysql+pymysql://", 1)[1]
        engine = create_async_engine(dsn, pool_pre_ping=True)

    async with engine.begin() as conn:
        await conn.run_sync(lambda c: StockPeerComparison.__table__.create(c, checkfirst=True))
    print("表 stock_peer_comparison 已创建或已存在")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
