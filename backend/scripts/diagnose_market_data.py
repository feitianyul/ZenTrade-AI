"""
行情数据页无数据排查脚本：按调用链路检查 Redis -> MySQL。
- 大盘指数：前端 /market/indices -> L1 内存 -> Redis market:indices:all -> 快照表/AKShare/新浪
- 北向资金近30日：前端 /market/northbound-flow -> Redis market:northbound_flow:north:30 -> MySQL northbound_flow
- 热门排行：/market/hot -> Redis market:hot_rank:hot -> 快照表/AKShare/stock_info
- 个股排行：/market/ranking -> Redis market:ranking:{sort_by}:{order}:{limit} -> 快照表 market_spot_snapshot -> stock_info

使用方式（在 backend 目录或项目根）：
  python backend/scripts/diagnose_market_data.py
  或设置环境变量后运行（密码含 @ 请用 %40）：
  set MYSQL_DSN=mysql+asyncmy://root:hatech%%401618@127.0.0.1:3308/retail_lowfreq
  set REDIS_URL=redis://localhost:6379/0
"""
import asyncio
import json
import os
import sys

# 加载 backend/.env
def _load_dotenv():
    for d in [os.path.dirname(__file__), os.path.join(os.path.dirname(__file__), "..")]:
        env_path = os.path.join(d, ".env")
        if os.path.isfile(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip('"').strip("'")
                        if k and k not in os.environ:
                            os.environ[k] = v
            return
_load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MYSQL_DSN = os.getenv("MYSQL_DSN", "").strip() or "mysql+asyncmy://root:hatech%401618@127.0.0.1:3308/retail_lowfreq"


async def check_redis():
    """检查行情相关 Redis 键"""
    print("\n=== Redis (REDIS_URL) ===")
    print(f"  REDIS_URL = {REDIS_URL}")
    try:
        import redis.asyncio as redis
        client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        await client.ping()
        print("  连接: OK")
    except Exception as e:
        print(f"  连接: 失败 - {e}")
        return

    keys_check = [
        "market:indices:all",
        "market:northbound_flow:north:30",
        "market:hot_rank:hot",
        "market:ranking:change_pct:desc:30",
    ]
    for key in keys_check:
        try:
            val = await client.get(key)
            if val is None:
                print(f"  {key}: (无)")
            else:
                preview = val[:200] + "..." if len(val) > 200 else val
                try:
                    obj = json.loads(val)
                    if isinstance(obj, list):
                        print(f"  {key}: 有, 条数={len(obj)}")
                    elif isinstance(obj, dict) and "items" in obj:
                        print(f"  {key}: 有, items 条数={len(obj.get('items') or [])}")
                    else:
                        print(f"  {key}: 有, 预览={preview[:80]}...")
                except Exception:
                    print(f"  {key}: 有, 长度={len(val)}")
        except Exception as e:
            print(f"  {key}: 错误 - {e}")
    await client.aclose()


async def check_mysql():
    """检查 MySQL northbound_flow 表"""
    print("\n=== MySQL (northbound_flow) ===")
    if not MYSQL_DSN:
        print("  MYSQL_DSN 未设置，跳过")
        return
    # 兼容 asyncmy
    dsn = MYSQL_DSN
    if dsn.startswith("mysql+pymysql://"):
        dsn = "mysql+asyncmy://" + dsn.split("mysql+pymysql://", 1)[1]
    print(f"  DSN = {dsn.split('@')[1] if '@' in dsn else dsn[:50]}...")
    try:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
        from sqlalchemy.orm import sessionmaker
        engine = create_async_engine(dsn, pool_pre_ping=True)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with async_session() as session:
            # 表是否存在
            r = await session.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = 'northbound_flow'"
            ))
            if r.scalar() == 0:
                print("  表 northbound_flow: 不存在")
                await engine.dispose()
                return
            # 总行数
            r = await session.execute(text("SELECT COUNT(*) FROM northbound_flow"))
            total = r.scalar()
            print(f"  表 northbound_flow: 总行数 = {total}")
            if total == 0:
                await engine.dispose()
                return
            # direction=north 的行数及有净买额的行数
            r = await session.execute(text(
                "SELECT COUNT(*) FROM northbound_flow WHERE direction = 'north'"
            ))
            north_count = r.scalar()
            print(f"  direction=north 行数 = {north_count}")
            r = await session.execute(text(
                "SELECT COUNT(*) FROM northbound_flow WHERE direction = 'north' "
                "AND (sh_net_buy IS NOT NULL OR sz_net_buy IS NOT NULL OR total_net_buy IS NOT NULL)"
            ))
            with_value = r.scalar()
            print(f"  其中净买额非空行数 = {with_value}")
            # 最近 5 条
            r = await session.execute(text(
                "SELECT trade_date, direction, sh_net_buy, sz_net_buy, total_net_buy "
                "FROM northbound_flow ORDER BY trade_date DESC LIMIT 5"
            ))
            rows = r.fetchall()
            print("  最近 5 条:")
            for row in rows:
                print(f"    {row[0]} | dir={row[1]} | sh={row[2]} | sz={row[3]} | total={row[4]}")
        await engine.dispose()
    except Exception as e:
        print(f"  连接/查询失败: {e}")


async def check_market_spot_snapshot():
    """检查排行兜底表 market_spot_snapshot（预热写入，Redis 无时读此处）"""
    print("\n=== MySQL (market_spot_snapshot，排行兜底) ===")
    if not MYSQL_DSN:
        print("  MYSQL_DSN 未设置，跳过")
        return
    dsn = MYSQL_DSN
    if dsn.startswith("mysql+pymysql://"):
        dsn = "mysql+asyncmy://" + dsn.split("mysql+pymysql://", 1)[1]
    try:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
        from sqlalchemy.orm import sessionmaker
        engine = create_async_engine(dsn, pool_pre_ping=True)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with async_session() as session:
            r = await session.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = 'market_spot_snapshot'"
            ))
            if r.scalar() == 0:
                print("  表 market_spot_snapshot: 不存在（需执行建表或 python _setup_mysql.py）")
                await engine.dispose()
                return
            r = await session.execute(text("SELECT COUNT(*) FROM market_spot_snapshot"))
            total = r.scalar()
            print(f"  表 market_spot_snapshot: 总行数 = {total}")
            if total > 0:
                r = await session.execute(text(
                    "SELECT snapshot_date, COUNT(*) AS cnt FROM market_spot_snapshot GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 5"
                ))
                rows = r.fetchall()
                print("  按日期统计（最近5个日期）:")
                for row in rows:
                    print(f"    {row[0]} -> {row[1]} 条")
            await engine.dispose()
    except Exception as e:
        print(f"  连接/查询失败: {e}")


def main():
    print("行情数据链路诊断（Redis -> MySQL）")
    asyncio.run(check_redis())
    asyncio.run(check_mysql())
    asyncio.run(check_market_spot_snapshot())
    print("\n结论建议:")
    print("  - 若 Redis market:indices:all 无：需「数据预热」或等首请求触发拉取并写入 Redis")
    print("  - 若 Redis market:northbound_flow:north:30 无 且 MySQL northbound_flow 无/净买额为 NULL：需在「配置中心-数据拉取」执行北向资金全量/增量同步")
    print("  - 排行无数据：先看 Redis market:ranking:change_pct:desc:30 和表 market_spot_snapshot。")
    print("    排行读取顺序：L1 内存 -> Redis -> (非交易时)快照表 market_spot_snapshot -> stock_info。")
    print("    若 Redis 无且快照表无/空：会退化为 stock_info（只有代码名称，价格等为 0）。请确保已建表并执行预热「个股排行」。")


if __name__ == "__main__":
    main()
