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
        await cur.execute("SELECT COUNT(*) FROM users")
        users_count = (await cur.fetchone())[0]
        await cur.execute("SELECT COUNT(*) FROM strategies")
        strategies_count = (await cur.fetchone())[0]
        await cur.execute("SELECT COUNT(*) FROM backtest_tasks")
        backtests_count = (await cur.fetchone())[0]
        await cur.execute(
            "SELECT phone, password_hash, tenant_id FROM users ORDER BY created_at DESC LIMIT 1"
        )
        latest_user = await cur.fetchone()
        await cur.execute(
            "SELECT id, name, status FROM strategies ORDER BY created_at DESC LIMIT 1"
        )
        latest_strategy = await cur.fetchone()
        await cur.execute(
            "SELECT id, status FROM backtest_tasks ORDER BY created_at DESC LIMIT 1"
        )
        latest_backtest = await cur.fetchone()
    conn.close()

    print(
        {
            "users_count": users_count,
            "strategies_count": strategies_count,
            "backtests_count": backtests_count,
            "latest_user": latest_user,
            "latest_strategy": latest_strategy,
            "latest_backtest": latest_backtest,
        }
    )


if __name__ == "__main__":
    asyncio.run(main())
