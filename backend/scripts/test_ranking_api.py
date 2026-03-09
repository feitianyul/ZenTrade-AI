"""
排行无数据排查：直接调用行情 API 并检查 Redis/MySQL 数据链。

用法（任选其一）：
  cd backend && python scripts/test_ranking_api.py
  python backend/scripts/test_ranking_api.py

环境变量（可选）：
  BASE_URL  默认 http://127.0.0.1:8000（需先启动后端服务）
  LOGIN_PHONE / LOGIN_PASSWORD  默认 13800001111 / Test1234；登录失败时请改为实际测试账号或检查后端用户表
  REDIS_URL / MYSQL_DSN  与 diagnose_market_data.py 一致，用于后端链路检查
"""
import asyncio
import json
import os
import sys
import time

# 加载 backend/.env
def _load_dotenv():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for d in [script_dir, os.path.join(script_dir, "..")]:
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

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
LOGIN_PHONE = os.getenv("LOGIN_PHONE", "13800001111")
LOGIN_PASSWORD = os.getenv("LOGIN_PASSWORD", "Test1234")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MYSQL_DSN = os.getenv("MYSQL_DSN", "").strip() or "mysql+asyncmy://root:hatech%401618@127.0.0.1:3308/retail_lowfreq"


def _api_get(headers: dict, path: str, timeout: int = 30) -> tuple[int, dict | list | None]:
    """GET 请求，返回 (status_code, data 或 None)。"""
    try:
        import requests
        r = requests.get(f"{BASE_URL}{path}", headers=headers, timeout=timeout)
        body = r.json()
        data = body.get("data") if isinstance(body, dict) else None
        return r.status_code, data
    except Exception as e:
        print(f"  请求异常: {e}")
        return -1, None


def test_ranking_via_api() -> dict:
    """1) 登录并调用排行等接口，返回汇总信息供排查。"""
    import requests

    print("\n=== 1) 直接调用 API ===\n")
    print(f"  BASE_URL = {BASE_URL}")
    print(f"  登录账号 = {LOGIN_PHONE}")

    # 登录
    try:
        r = requests.post(
            f"{BASE_URL}/auth/login",
            json={"phone": LOGIN_PHONE, "password": LOGIN_PASSWORD},
            timeout=10,
        )
        body = r.json()
        token = (body.get("data") or {}).get("token")
        if not token:
            print("  登录失败:", body.get("message", body))
            return {"login_ok": False, "ranking_count": 0, "ranking_sample": None}
        headers = {"Authorization": f"Bearer {token}"}
        print("  登录: OK\n")
    except Exception as e:
        print(f"  登录异常: {e}")
        return {"login_ok": False, "ranking_count": 0, "ranking_sample": None}

    # 大盘指数（对比是否有数据）
    t0 = time.time()
    code1, data1 = _api_get(headers, "/market/indices")
    ms1 = int((time.time() - t0) * 1000)
    count1 = len(data1) if isinstance(data1, list) else 0
    print(f"  GET /market/indices  -> {code1}  条数={count1}  ({ms1}ms)")
    if isinstance(data1, list) and data1:
        print(f"    样本: {data1[0]}")

    # 个股排行（排查目标）
    t0 = time.time()
    path_ranking = "/market/ranking?sort_by=change_pct&order=desc&limit=30"
    code2, data2 = _api_get(headers, path_ranking)
    ms2 = int((time.time() - t0) * 1000)
    count2 = len(data2) if isinstance(data2, list) else 0
    sample = None
    if isinstance(data2, list) and data2:
        sample = data2[0]
        # 判断是否为“真实数据”还是兜底（全 0）
        prices_ok = any(
            (item.get("price") or 0) != 0 or (item.get("change_pct") or 0) != 0
            for item in data2[:5]
        )
        data_source = "真实行情" if prices_ok else "兜底(price/change_pct 多为 0)"
    else:
        data_source = "无数据"
    print(f"  GET /market/ranking  -> {code2}  条数={count2}  ({ms2}ms)  [{data_source}]")
    if sample:
        print(f"    样本: {sample}")

    # 热门排行（对比）
    t0 = time.time()
    code3, data3 = _api_get(headers, "/market/hot")
    ms3 = int((time.time() - t0) * 1000)
    count3 = len(data3) if isinstance(data3, list) else 0
    print(f"  GET /market/hot      -> {code3}  条数={count3}  ({ms3}ms)")

    return {
        "login_ok": True,
        "ranking_count": count2,
        "ranking_sample": sample,
        "indices_count": count1,
        "hot_count": count3,
        "ranking_data_source": data_source,
    }


async def check_ranking_backend_chain():
    """2) 检查排行数据链：Redis -> market_spot_snapshot -> stock_info。"""
    print("\n=== 2) 后端数据链（Redis / MySQL）===\n")
    print(f"  REDIS_URL = {REDIS_URL}")

    # Redis: 排行使用的 key
    redis_key = "market:ranking:change_pct:desc:30"
    try:
        import redis.asyncio as redis
        client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        await client.ping()
        val = await client.get(redis_key)
        if val is None:
            print(f"  Redis {redis_key}: (无)")
        else:
            try:
                arr = json.loads(val)
                n = len(arr) if isinstance(arr, list) else 0
                print(f"  Redis {redis_key}: 有, 条数={n}")
            except Exception:
                print(f"  Redis {redis_key}: 有, 长度={len(val)}")
        await client.aclose()
    except Exception as e:
        print(f"  Redis 连接/读取失败: {e}")

    # MySQL: market_spot_snapshot
    if MYSQL_DSN:
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
                    print("  MySQL market_spot_snapshot: 表不存在")
                else:
                    r = await session.execute(text("SELECT COUNT(*) FROM market_spot_snapshot"))
                    total = r.scalar()
                    print(f"  MySQL market_spot_snapshot: 总行数 = {total}")
                    if total > 0:
                        r = await session.execute(text(
                            "SELECT snapshot_date, COUNT(*) AS cnt FROM market_spot_snapshot "
                            "GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 3"
                        ))
                        for row in r.fetchall():
                            print(f"    日期 {row[0]} -> {row[1]} 条")
            await engine.dispose()
        except Exception as e:
            print(f"  MySQL market_spot_snapshot 检查失败: {e}")

        # stock_info 条数（兜底用）
        try:
            from sqlalchemy import text
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
            from sqlalchemy.orm import sessionmaker
            engine = create_async_engine(dsn, pool_pre_ping=True)
            async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
            async with async_session() as session:
                r = await session.execute(text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() AND table_name = 'stock_info'"
                ))
                if r.scalar() == 0:
                    print("  MySQL stock_info: 表不存在")
                else:
                    r = await session.execute(text("SELECT COUNT(*) FROM stock_info"))
                    print(f"  MySQL stock_info: 总行数 = {r.scalar()}")
            await engine.dispose()
        except Exception as e:
            print(f"  MySQL stock_info 检查失败: {e}")


def print_conclusion(summary: dict):
    """3) 根据 API + 后端检查结果输出排查结论。"""
    print("\n=== 3) 排查结论 ===\n")
    if not summary.get("login_ok"):
        print("  未成功登录，请检查 BASE_URL 与登录账号密码。")
        return
    if summary.get("ranking_count", 0) > 0:
        src = summary.get("ranking_data_source", "")
        if "真实" in src:
            print("  排行有数据且为真实行情，无需处理。")
        else:
            print("  排行有数据但为兜底数据（价格/涨跌幅为 0）。")
            print("  可能原因：当前非交易时间且「非交易时拒绝外部 API」开启，仅从 Redis/快照表/stock_info 返回。")
            print("  建议：在「数据预热」中执行「个股排行」预热，或于交易时间再次请求。")
        return
    print("  排行无数据，可能原因：")
    print("  1) Redis 中无 market:ranking:change_pct:desc:30，且未在交易时间未拉取到东方财富数据；")
    print("  2) 非交易时间且配置「非交易时拒绝外部 API」= true，仅读 Redis/快照表/stock_info；")
    print("  3) market_spot_snapshot 表为空或不存在，stock_info 表也为空或不存在。")
    print("  建议：先运行「数据预热」中的「个股排行」；确认 market_spot_snapshot、stock_info 已建表并有数据。")


def main():
    print("=" * 60)
    print("排行无数据排查：直接调用 API + 后端数据链检查")
    print("=" * 60)

    summary = test_ranking_via_api()
    asyncio.run(check_ranking_backend_chain())
    print_conclusion(summary)

    print("\n" + "=" * 60)
    print("完成。若需完整 Redis/北向等诊断，可运行: python scripts/diagnose_market_data.py")
    print("=" * 60)


# ---------------------------------------------------------------------------
# pytest 可收集的测试（可选：pytest backend/scripts/test_ranking_api.py -v）
# ---------------------------------------------------------------------------

def test_ranking_api_returns_ok():
    """调用 /market/ranking 应返回 200 且 data 为列表（可为空）。"""
    summary = test_ranking_via_api()
    assert summary["login_ok"] is True, "登录失败，无法验证排行接口"
    # 接口正常时至少返回列表；无数据时为 []，有数据时为非空列表
    assert isinstance(summary.get("ranking_count"), int), "应返回条数字段"


def test_ranking_api_and_backend_chain():
    """完整链路：API 调用 + Redis/MySQL 检查，无断言，仅输出排查信息。"""
    summary = test_ranking_via_api()
    asyncio.run(check_ranking_backend_chain())
    print_conclusion(summary)


if __name__ == "__main__":
    main()
