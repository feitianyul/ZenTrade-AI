import asyncio
import os

import asyncmy


async def main() -> None:
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", "3308"))
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    database = os.getenv("DB_NAME", "panda")

    conn = await asyncmy.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    async with conn.cursor() as cur:
        await cur.execute("SELECT COUNT(*) FROM orders")
        orders_count = (await cur.fetchone())[0]
        await cur.execute("SELECT COUNT(*) FROM positions")
        positions_count = (await cur.fetchone())[0]
        await cur.execute(
            "SELECT id, symbol, direction, status FROM orders ORDER BY created_at DESC LIMIT 1"
        )
        latest_order = await cur.fetchone()
        await cur.execute(
            "SELECT id, symbol, volume, pnl FROM positions ORDER BY created_at DESC LIMIT 1"
        )
        latest_position = await cur.fetchone()
    conn.close()

    print(
        {
            "orders_count": orders_count,
            "positions_count": positions_count,
            "latest_order": latest_order,
            "latest_position": latest_position,
        }
    )


if __name__ == "__main__":
    asyncio.run(main())
