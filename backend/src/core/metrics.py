import time
from typing import Awaitable, Callable

from fastapi import FastAPI, Request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.responses import Response

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
)
ORDER_LATENCY = Histogram(
    "order_latency_seconds",
    "Order processing latency in seconds",
    ["stage"],
)
ORDER_TOTAL = Counter("order_total", "Total orders", ["env", "direction"])
ORDER_FILLED = Counter("order_filled_total", "Filled orders", ["env"])
ORDER_CANCEL = Counter("order_cancel_total", "Canceled orders", ["env"])
DATA_INGEST = Counter("data_ingest_total", "Market data ingested", ["source"])
DATA_QUERY_LATENCY = Histogram(
    "data_query_latency_seconds",
    "Market data query latency in seconds",
    ["source"],
)
STRATEGY_CPU = Counter("strategy_cpu_seconds_total", "Strategy CPU seconds", ["strategy_id"])
STRATEGY_MEMORY = Gauge("strategy_memory_bytes", "Strategy memory usage", ["strategy_id"])
GPU_MEMORY_USED = Gauge("gpu_memory_used_bytes", "GPU memory used", ["device"])
GPU_MEMORY_TOTAL = Gauge("gpu_memory_total_bytes", "GPU memory total", ["device"])

# System Metrics
SYSTEM_CPU_USAGE = Gauge("system_cpu_usage_percent", "System CPU usage percent")
SYSTEM_MEMORY_USAGE = Gauge("system_memory_usage_bytes", "System memory usage bytes")
SYSTEM_DISK_USAGE = Gauge("system_disk_usage_percent", "System disk usage percent")

# Business Metrics
ACTIVE_USERS = Gauge("active_users", "Number of active users")
ACTIVE_STRATEGIES = Gauge("active_strategies", "Number of active strategies")
TOTAL_ASSETS = Gauge("total_assets", "Total assets under management")


def record_order_latency(stage: str, duration: float) -> None:
    ORDER_LATENCY.labels(stage).observe(duration)


def record_order_event(env: str, direction: str, status: str) -> None:
    ORDER_TOTAL.labels(env, direction).inc()
    if status == "filled":
        ORDER_FILLED.labels(env).inc()
    if status == "canceled":
        ORDER_CANCEL.labels(env).inc()


def record_data_ingest(source: str, count: int) -> None:
    DATA_INGEST.labels(source).inc(count)


def record_data_query(source: str, duration: float) -> None:
    DATA_QUERY_LATENCY.labels(source).observe(duration)

def setup_metrics(app: FastAPI) -> None:
    @app.middleware("http")
    async def record_metrics(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start
        REQUEST_LATENCY.labels(request.method, request.url.path).observe(duration)
        REQUEST_COUNT.labels(request.method, request.url.path, str(response.status_code)).inc()
        return response

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
