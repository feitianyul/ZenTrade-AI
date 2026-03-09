import asyncio
import os

import asyncmy
from sqlalchemy.ext.asyncio import create_async_engine
from src.models.base import Base


async def main() -> None:
    db_name = os.getenv("DB_NAME", "panda")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", "3308"))
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")

    conn = await asyncmy.connect(host=host, port=port, user=user, password=password)
    try:
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET utf8mb4"
                )
            await conn.commit()
        except Exception:
            pass
    finally:
        conn.close()

    engine = create_async_engine(os.getenv("MYSQL_DSN"))
    async with engine.begin() as db:
        await db.run_sync(Base.metadata.create_all)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
