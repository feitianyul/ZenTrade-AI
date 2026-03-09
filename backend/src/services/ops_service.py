"""Operations service: system status, component health probes, metrics summary, time sources."""

import asyncio
import logging
import os
import platform
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# Record process start time at module load
_PROCESS_START = time.time()

try:
    import psutil
except ImportError:
    psutil = None


# ---------------------------------------------------------------------------
# Helper: fast TCP port check
# ---------------------------------------------------------------------------

from concurrent.futures import ThreadPoolExecutor
_port_check_pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="port-check")


async def _quick_port_check(host: str, port: int, timeout: float = 2.0) -> bool:
    """Fast TCP port reachability check using synchronous socket (more reliable on Windows).

    ``asyncio.open_connection`` can be flaky under high concurrency on Windows
    (e.g. when 10 probes run in parallel), so we offload a plain blocking
    ``socket.create_connection`` to a dedicated thread-pool.
    """
    import socket

    def _sync_check() -> bool:
        try:
            s = socket.create_connection((host, port), timeout=timeout)
            s.close()
            return True
        except Exception:
            return False

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_port_check_pool, _sync_check)


# ---------------------------------------------------------------------------
# 1. System Status (expanded)
# ---------------------------------------------------------------------------

async def get_system_status() -> Dict[str, Any]:
    """Return system info, resources, process info, db pool, redis info."""
    # Resources
    if psutil:
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        try:
            disk = psutil.disk_usage('/')
        except Exception:
            disk = psutil.disk_usage('C:\\')
        proc = psutil.Process(os.getpid())
        proc_info = {
            "pid": os.getpid(),
            "threads": proc.num_threads(),
            "memory_rss_mb": round(proc.memory_info().rss / 1024 / 1024, 2),
            "cpu_percent": proc.cpu_percent(interval=0.05),
        }
        resources = {
            "cpu_percent": cpu_percent,
            "cpu_count": psutil.cpu_count(),
            "memory_percent": memory.percent,
            "memory_used_mb": round(memory.used / 1024 / 1024, 2),
            "memory_total_mb": round(memory.total / 1024 / 1024, 2),
            "disk_percent": disk.percent,
            "disk_used_gb": round(disk.used / 1024 / 1024 / 1024, 2),
            "disk_total_gb": round(disk.total / 1024 / 1024 / 1024, 2),
            "disk_free_gb": round(disk.free / 1024 / 1024 / 1024, 2),
        }
    else:
        resources = {
            "cpu_percent": 0.0, "cpu_count": 0,
            "memory_percent": 0.0, "memory_used_mb": 0.0, "memory_total_mb": 0.0,
            "disk_percent": 0.0, "disk_used_gb": 0.0, "disk_total_gb": 0.0, "disk_free_gb": 0.0,
            "note": "psutil not installed",
        }
        proc_info = {"pid": os.getpid(), "threads": 0, "memory_rss_mb": 0, "cpu_percent": 0}

    # DB pool info
    db_pool = await _get_db_pool_info()

    # Redis info
    redis_info = await _get_redis_info()

    uptime = time.time() - _PROCESS_START

    return {
        "system": {
            "os": platform.system(),
            "release": platform.release(),
            "hostname": platform.node(),
            "python_version": sys.version.split()[0],
            "app_version": os.getenv("APP_VERSION", "0.1.0"),
        },
        "uptime_seconds": int(uptime),
        "process": proc_info,
        "resources": resources,
        "db_pool": db_pool,
        "redis_info": redis_info,
    }


async def _get_db_pool_info() -> Dict[str, Any]:
    try:
        from src.core.db import get_engine
        engine = get_engine()
        pool = engine.pool
        return {
            "status": "connected",
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "pool_info": str(pool.status()),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)[:200]}


async def _get_redis_info() -> Dict[str, Any]:
    # Quick port check first to avoid long timeout on non-running Redis
    host = os.getenv("REDIS_HOST", "127.0.0.1")
    port = int(os.getenv("REDIS_PORT", "6379"))
    if not await _quick_port_check(host, port):
        return {"status": "error", "error": f"Redis port {port} unreachable"}
    try:
        import redis.asyncio as aioredis
        client = aioredis.Redis(host=host, port=port, socket_connect_timeout=2.0, socket_timeout=2.0)
        t0 = time.time()
        await client.ping()
        latency = int((time.time() - t0) * 1000)
        info = await client.info(section="memory")
        keys = await client.dbsize()
        await client.aclose()
        return {
            "status": "connected",
            "ping_ms": latency,
            "used_memory_mb": round(info.get("used_memory", 0) / 1024 / 1024, 2),
            "used_memory_peak_mb": round(info.get("used_memory_peak", 0) / 1024 / 1024, 2),
            "keys": keys,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)[:200]}


# ---------------------------------------------------------------------------
# 2. Component Health Probes
# ---------------------------------------------------------------------------

COMPONENTS = [
    {"id": "mysql", "name": "MySQL", "icon": "fa-database", "layer": "infra",
     "port": os.getenv("MYSQL_PORT", "3306")},
    {"id": "redis", "name": "Redis", "icon": "fa-bolt", "layer": "infra",
     "port": os.getenv("REDIS_PORT", "6379")},
    {"id": "clickhouse", "name": "ClickHouse", "icon": "fa-warehouse", "layer": "infra",
     "port": os.getenv("CLICKHOUSE_HTTP_PORT", "8123")},
    {"id": "qdrant", "name": "Qdrant", "icon": "fa-brain", "layer": "infra",
     "port": os.getenv("VECTOR_STORE_PORT", "6333")},
    {"id": "panda_server", "name": "主服务 (FastAPI)", "icon": "fa-server", "layer": "app",
     "port": os.getenv("PANDA_SERVER_PORT", "8400")},
    {"id": "panda_trading", "name": "交易网关", "icon": "fa-exchange-alt", "layer": "app",
     "port": os.getenv("PANDA_TRADING_HTTP_PORT", "8001")},
    {"id": "panda_llm", "name": "LLM 服务", "icon": "fa-robot", "layer": "app",
     "port": os.getenv("PANDA_LLM_PORT", "8002")},
    {"id": "panda_data", "name": "数据服务", "icon": "fa-chart-line", "layer": "app",
     "port": os.getenv("PANDA_DATA_PORT", "8003")},
    {"id": "prometheus", "name": "Prometheus", "icon": "fa-fire", "layer": "monitor",
     "port": os.getenv("PROMETHEUS_PORT", "9090")},
    {"id": "grafana", "name": "Grafana", "icon": "fa-tachometer-alt", "layer": "monitor",
     "port": os.getenv("GRAFANA_PORT", "3001")},
]


# ---------------------------------------------------------------------------
# Probe cache + circuit-breaker
# ---------------------------------------------------------------------------
_probe_cache: Optional[List[Dict[str, Any]]] = None
_probe_cache_ts: float = 0.0
_PROBE_CACHE_TTL: float = 30.0  # seconds — cache sequential probe results longer

# Circuit-breaker: track consecutive failures per component.
# After _CB_THRESHOLD consecutive failures, skip probing for _CB_COOLDOWN seconds.
_cb_fail_count: Dict[str, int] = {}
_cb_last_fail_ts: Dict[str, float] = {}
_CB_THRESHOLD = 100  # Effectively disabled — Windows probe timing is flaky
_CB_COOLDOWN = 10.0  # Short cooldown when threshold is hit
_PROBE_HARD_TIMEOUT = 8.0  # hard per-probe timeout (seconds)


async def probe_all_components() -> List[Dict[str, Any]]:
    """Probe all components **in parallel** with per-probe timeout + circuit-breaker.

    Results are cached for ``_PROBE_CACHE_TTL`` seconds so that two
    concurrent callers share the same probe round.
    """
    global _probe_cache, _probe_cache_ts

    now = time.time()
    if _probe_cache is not None and (now - _probe_cache_ts) < _PROBE_CACHE_TTL:
        return _probe_cache

    async def _run_one(comp: Dict) -> Dict[str, Any]:
        cid = comp["id"]

        # Circuit-breaker: skip if recently failed too many times
        if _cb_fail_count.get(cid, 0) >= _CB_THRESHOLD:
            last_ts = _cb_last_fail_ts.get(cid, 0)
            if (now - last_ts) < _CB_COOLDOWN:
                return {**comp, "status": "down", "latency_ms": 0, "version": "",
                        "details": f"circuit-breaker: skipped ({_cb_fail_count[cid]} failures)"}
            else:
                # Cooldown expired — reset and retry
                _cb_fail_count[cid] = 0

        probe_fn = _PROBE_MAP.get(cid, _probe_skip)
        try:
            result = await asyncio.wait_for(probe_fn(comp), timeout=_PROBE_HARD_TIMEOUT)
        except asyncio.TimeoutError:
            result = {"status": "down", "latency_ms": int(_PROBE_HARD_TIMEOUT * 1000),
                      "version": "", "details": f"probe timeout ({_PROBE_HARD_TIMEOUT}s)"}
        except Exception as exc:
            result = {"status": "down", "latency_ms": 0, "version": "",
                      "details": str(exc)[:200]}

        # Update circuit-breaker counters
        if result.get("status") == "down":
            _cb_fail_count[cid] = _cb_fail_count.get(cid, 0) + 1
            _cb_last_fail_ts[cid] = time.time()
        else:
            _cb_fail_count[cid] = 0

        return {**comp, **result}

    # Run probes SEQUENTIALLY on Windows to avoid asyncio/thread-pool contention.
    # Total time ~10-15s, but results are 100% reliable.
    # Cached for _PROBE_CACHE_TTL seconds so API responses are still fast.
    results = []
    for comp in COMPONENTS:
        result = await _run_one(comp)
        results.append(result)

    _probe_cache = results
    _probe_cache_ts = time.time()
    return results


async def _probe_mysql(comp: Dict) -> Dict[str, Any]:
    try:
        from src.core.db import get_engine
        from sqlalchemy import text
        engine = get_engine()
        t0 = time.time()
        async with engine.connect() as conn:
            row = await conn.execute(text("SELECT 1"))
            row.close()
        latency = int((time.time() - t0) * 1000)
        # Get version
        async with engine.connect() as conn:
            ver = await conn.execute(text("SELECT VERSION()"))
            version = str(ver.scalar())
        return {"status": "running", "latency_ms": latency, "version": version, "details": "SELECT 1 OK"}
    except Exception as e:
        return {"status": "down", "latency_ms": 0, "version": "", "details": str(e)[:200]}


async def _probe_redis(comp: Dict) -> Dict[str, Any]:
    """Redis probe using synchronous redis client in executor to avoid event loop contention."""
    redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

    def _sync_redis_check() -> Dict[str, Any]:
        import redis as sync_redis
        try:
            client = sync_redis.from_url(
                redis_url,
                socket_connect_timeout=3.0,
                socket_timeout=3.0,
                decode_responses=True,
            )
            t0 = time.time()
            pong = client.ping()
            latency = int((time.time() - t0) * 1000)
            info = client.info(section="server")
            version = info.get("redis_version", "")
            client.close()
            return {"status": "running", "latency_ms": latency, "version": version, "details": f"PONG={pong}"}
        except Exception as e:
            return {"status": "down", "latency_ms": 0, "version": "", "details": str(e)[:200]}

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_port_check_pool, _sync_redis_check)


async def _probe_http(url: str, comp: Dict) -> Dict[str, Any]:
    """HTTP health probe — directly attempt HTTP GET without pre-checking TCP port.

    On Windows, concurrent TCP port checks via thread pool cause contention.
    httpx handles connection errors fast enough on its own.
    """
    try:
        t0 = time.time()
        timeout = httpx.Timeout(connect=2.0, read=3.0, write=1.0, pool=1.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url)
        latency = int((time.time() - t0) * 1000)
        if resp.status_code < 400:
            return {"status": "running", "latency_ms": latency, "version": "", "details": f"HTTP {resp.status_code}"}
        return {"status": "degraded", "latency_ms": latency, "version": "", "details": f"HTTP {resp.status_code}"}
    except httpx.ConnectError:
        # Extract port from URL for better error message
        try:
            from urllib.parse import urlparse
            port = urlparse(url).port or 80
        except Exception:
            port = "?"
        return {"status": "down", "latency_ms": 0, "version": "", "details": f"port {port} unreachable"}
    except Exception as e:
        return {"status": "down", "latency_ms": 0, "version": "", "details": str(e)[:200]}


async def _probe_clickhouse(comp: Dict) -> Dict[str, Any]:
    ch_url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
    return await _probe_http(f"{ch_url}/ping", comp)


async def _probe_qdrant(comp: Dict) -> Dict[str, Any]:
    qdrant_url = os.getenv("VECTOR_STORE_URL", "http://127.0.0.1:6333")
    try:
        t0 = time.time()
        timeout = httpx.Timeout(connect=2.0, read=3.0, write=1.0, pool=1.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(f"{qdrant_url}/collections")
        latency = int((time.time() - t0) * 1000)
        if resp.status_code == 200:
            data = resp.json()
            collections = data.get("result", {}).get("collections", [])
            return {"status": "running", "latency_ms": latency, "version": "",
                    "details": f"{len(collections)} collections"}
        return {"status": "degraded", "latency_ms": latency, "version": "", "details": f"HTTP {resp.status_code}"}
    except httpx.ConnectError:
        port = os.getenv("VECTOR_STORE_PORT", "6333")
        return {"status": "down", "latency_ms": 0, "version": "", "details": f"port {port} unreachable"}
    except Exception as e:
        return {"status": "down", "latency_ms": 0, "version": "", "details": str(e)[:200]}


async def _probe_panda_server(comp: Dict) -> Dict[str, Any]:
    """Self-check: the main server is always running if we can respond."""
    uptime = int(time.time() - _PROCESS_START)
    return {"status": "running", "latency_ms": 0, "version": os.getenv("APP_VERSION", "0.1.0"),
            "details": f"Self, uptime {uptime}s"}


async def _probe_trading(comp: Dict) -> Dict[str, Any]:
    url = os.getenv("TRADING_HEALTH_URL", "http://localhost:8001/health")
    return await _probe_http(url, comp)


async def _probe_llm(comp: Dict) -> Dict[str, Any]:
    url = os.getenv("LLM_HEALTH_URL", "http://localhost:8002/health")
    return await _probe_http(url, comp)


async def _probe_data(comp: Dict) -> Dict[str, Any]:
    url = os.getenv("DATA_HEALTH_URL", "http://localhost:8003/health")
    return await _probe_http(url, comp)


async def _probe_prometheus(comp: Dict) -> Dict[str, Any]:
    url = os.getenv("PROMETHEUS_URL", "http://localhost:9090") + "/-/healthy"
    return await _probe_http(url, comp)


async def _probe_grafana(comp: Dict) -> Dict[str, Any]:
    port = os.getenv("GRAFANA_PORT", "3001")
    url = f"http://127.0.0.1:{port}/api/health"
    return await _probe_http(url, comp)


async def _probe_skip(comp: Dict) -> Dict[str, Any]:
    return {"status": "unknown", "latency_ms": 0, "version": "", "details": "No probe configured"}


_PROBE_MAP = {
    "mysql": _probe_mysql,
    "redis": _probe_redis,
    "clickhouse": _probe_clickhouse,
    "qdrant": _probe_qdrant,
    "panda_server": _probe_panda_server,
    "panda_trading": _probe_trading,
    "panda_llm": _probe_llm,
    "panda_data": _probe_data,
    "prometheus": _probe_prometheus,
    "grafana": _probe_grafana,
}


# ---------------------------------------------------------------------------
# 3. Metrics Summary (parse Prometheus output)
# ---------------------------------------------------------------------------

async def get_metrics_summary() -> Dict[str, Any]:
    """Parse in-process Prometheus metrics into a frontend-friendly JSON summary."""
    from prometheus_client import REGISTRY

    summary: Dict[str, Any] = {
        "http": {"total_requests": 0, "error_count": 0, "error_rate": 0.0, "avg_latency_ms": 0,
                 "top_slow_paths": []},
        "orders": {"total": 0, "filled": 0, "canceled": 0},
        "data": {"ingest_total": 0},
        "business": {"active_users": 0, "active_strategies": 0},
    }

    # Collect from registry
    path_latency: Dict[str, Dict] = {}  # path -> {count, sum}
    total_req = 0
    error_req = 0
    total_latency_sum = 0.0
    total_latency_count = 0
    order_total = 0
    order_filled = 0
    order_canceled = 0
    data_ingest = 0

    for metric in REGISTRY.collect():
        for sample in metric.samples:
            name = sample.name
            labels = sample.labels
            value = sample.value

            if name == "http_requests_total":
                total_req += value
                status = labels.get("status", "200")
                if status.startswith("4") or status.startswith("5"):
                    error_req += value

            elif name == "http_request_duration_seconds_sum":
                path = labels.get("path", "")
                if path not in path_latency:
                    path_latency[path] = {"count": 0, "sum": 0.0}
                path_latency[path]["sum"] += value
                total_latency_sum += value

            elif name == "http_request_duration_seconds_count":
                path = labels.get("path", "")
                if path not in path_latency:
                    path_latency[path] = {"count": 0, "sum": 0.0}
                path_latency[path]["count"] += value
                total_latency_count += value

            elif name == "order_total_total":
                order_total += value
            elif name == "order_filled_total_total":
                order_filled += value
            elif name == "order_cancel_total_total":
                order_canceled += value
            elif name == "data_ingest_total_total":
                data_ingest += value
            elif name == "active_users":
                summary["business"]["active_users"] = int(value)
            elif name == "active_strategies":
                summary["business"]["active_strategies"] = int(value)

    # Compute summaries
    summary["http"]["total_requests"] = int(total_req)
    summary["http"]["error_count"] = int(error_req)
    summary["http"]["error_rate"] = round(error_req / max(total_req, 1) * 100, 2)
    summary["http"]["avg_latency_ms"] = round(
        total_latency_sum / max(total_latency_count, 1) * 1000, 1
    )

    # Top 10 slowest paths
    top_slow = []
    for path, data in path_latency.items():
        if data["count"] > 0 and path not in ("/metrics", "/system/health"):
            avg = data["sum"] / data["count"] * 1000
            top_slow.append({"path": path, "avg_ms": round(avg, 1), "count": int(data["count"])})
    top_slow.sort(key=lambda x: x["avg_ms"], reverse=True)
    summary["http"]["top_slow_paths"] = top_slow[:10]

    summary["orders"] = {"total": int(order_total), "filled": int(order_filled),
                         "canceled": int(order_canceled)}
    summary["data"]["ingest_total"] = int(data_ingest)

    return summary


# ---------------------------------------------------------------------------
# 4. Health with component quick-check
# ---------------------------------------------------------------------------

async def get_health_with_components() -> Dict[str, Any]:
    """Quick health check with overall rating."""
    components = await probe_all_components()
    running = sum(1 for c in components if c.get("status") == "running")
    down = sum(1 for c in components if c.get("status") == "down")
    total = len(components)

    if down == 0:
        overall = "healthy"
    elif down <= 2:
        overall = "degraded"
    else:
        overall = "unhealthy"

    return {
        "status": overall,
        "version": os.getenv("APP_VERSION", "0.1.0"),
        "uptime_seconds": int(time.time() - _PROCESS_START),
        "components_summary": {"total": total, "running": running, "down": down},
        "components": [
            {"id": c["id"], "name": c["name"], "status": c.get("status", "unknown"),
             "latency_ms": c.get("latency_ms", 0), "details": c.get("details", ""),
             "version": c.get("version", ""), "port": c.get("port", "")}
            for c in components
        ],
    }


# ---------------------------------------------------------------------------
# 5. Time sources (UTC) and consistency check
# ---------------------------------------------------------------------------

_DRIFT_THRESHOLD_SEC = 2.0


async def get_time_sources() -> Dict[str, Any]:
    """Return current time (UTC) from app, MySQL, Redis, ClickHouse; set inconsistent if any drift > 2s."""
    baseline = datetime.now(timezone.utc)
    baseline_ts = baseline.timestamp()
    sources: List[Dict[str, Any]] = []
    drifts: List[str] = []

    # App (benchmark)
    app_ts = baseline_ts
    sources.append({
        "id": "app",
        "name": "应用进程",
        "time_utc": baseline.isoformat(),
        "timezone": "UTC",
        "note": "",
        "status": "ok",
    })

    # MySQL: UTC_TIMESTAMP() + session/global time_zone
    try:
        from src.core.db import get_engine
        from sqlalchemy import text
        engine = get_engine()
        async with engine.connect() as conn:
            row = await conn.execute(
                text("SELECT UTC_TIMESTAMP() AS now_utc, @@session.time_zone AS session_tz, @@global.time_zone AS global_tz")
            )
            r = row.mappings().first()
        if r:
            now_utc = r["now_utc"]
            if hasattr(now_utc, "isoformat"):
                time_utc_str = now_utc.isoformat()
                dt = now_utc.replace(tzinfo=timezone.utc) if getattr(now_utc, "tzinfo", None) is None else now_utc
                mysql_ts = dt.timestamp() if hasattr(dt, "timestamp") else baseline_ts
            else:
                time_utc_str = str(now_utc)
                mysql_ts = baseline_ts
            session_tz = r.get("session_tz") or ""
            global_tz = r.get("global_tz") or ""
            tz_note = f"session={session_tz}, global={global_tz}"
            drift = abs(mysql_ts - baseline_ts) if mysql_ts else 0
            if drift > _DRIFT_THRESHOLD_SEC:
                drifts.append(f"MySQL 与基准偏差 {drift:.1f}s")
            sources.append({
                "id": "mysql",
                "name": "MySQL",
                "time_utc": time_utc_str,
                "timezone": tz_note,
                "note": "",
                "status": "ok",
            })
        else:
            sources.append({"id": "mysql", "name": "MySQL", "time_utc": "", "timezone": "", "note": "无结果", "status": "error"})
    except Exception as e:
        sources.append({"id": "mysql", "name": "MySQL", "time_utc": "", "timezone": "", "note": str(e)[:120], "status": "unreachable"})

    # Redis: TIME -> [sec, usec], no TZ (server time)
    try:
        redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

        def _sync_redis_time() -> Tuple[Optional[float], Optional[str]]:
            import redis as sync_redis
            try:
                client = sync_redis.from_url(redis_url, socket_connect_timeout=2.0, socket_timeout=2.0, decode_responses=False)
                t = client.time()
                client.close()
                if isinstance(t, (list, tuple)) and len(t) >= 2:
                    sec, usec = int(t[0]), int(t[1]) if len(t) > 1 else 0
                    ts = sec + usec / 1e6
                    dt = datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc)
                    return ts, dt.isoformat()
                return None, None
            except Exception:
                return None, None

        loop = asyncio.get_running_loop()
        redis_ts, time_utc_str = await loop.run_in_executor(_port_check_pool, _sync_redis_time)
        if redis_ts is not None and time_utc_str:
            drift = abs(redis_ts - baseline_ts)
            if drift > _DRIFT_THRESHOLD_SEC:
                drifts.append(f"Redis 与基准偏差 {drift:.1f}s（Redis 无时区，为服务器时间）")
            sources.append({
                "id": "redis",
                "name": "Redis",
                "time_utc": time_utc_str,
                "timezone": "",
                "note": "Redis 无时区，返回服务器时间",
                "status": "ok",
            })
        else:
            sources.append({"id": "redis", "name": "Redis", "time_utc": "", "timezone": "", "note": "TIME 失败", "status": "unreachable"})
    except Exception as e:
        sources.append({"id": "redis", "name": "Redis", "time_utc": "", "timezone": "", "note": str(e)[:120], "status": "unreachable"})

    # ClickHouse: HTTP SELECT now()
    try:
        ch_url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
        timeout = httpx.Timeout(connect=2.0, read=3.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(f"{ch_url}/?query=SELECT%20now()%20AS%20t")
        if resp.status_code == 200:
            line = (resp.text or "").strip().split("\n")
            time_utc_str = line[0] if line else ""
            # CH now() is typically server local; treat as comparable moment for drift
            try:
                s = time_utc_str.replace("Z", "+00:00").strip()
                ch_dt = datetime.fromisoformat(s) if s else None
                if ch_dt is not None:
                    if ch_dt.tzinfo is None:
                        ch_dt = ch_dt.replace(tzinfo=timezone.utc)
                    ch_ts = ch_dt.timestamp()
                else:
                    ch_ts = baseline_ts
            except Exception:
                ch_ts = baseline_ts
            drift = abs(ch_ts - baseline_ts)
            if drift > _DRIFT_THRESHOLD_SEC:
                drifts.append(f"ClickHouse 与基准偏差 {drift:.1f}s")
            sources.append({
                "id": "clickhouse",
                "name": "ClickHouse",
                "time_utc": time_utc_str,
                "timezone": "服务器时区",
                "note": "",
                "status": "ok",
            })
        else:
            sources.append({"id": "clickhouse", "name": "ClickHouse", "time_utc": "", "timezone": "", "note": f"HTTP {resp.status_code}", "status": "unreachable"})
    except Exception as e:
        sources.append({"id": "clickhouse", "name": "ClickHouse", "time_utc": "", "timezone": "", "note": str(e)[:120], "status": "unreachable"})

    inconsistent = len(drifts) > 0
    return {
        "sources": sources,
        "inconsistent": inconsistent,
        "details": drifts,
    }
