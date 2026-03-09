"""数据同步 API — AKShare 全量/增量数据拉取

端点:
  GET  /data-sync/categories     获取所有数据分类及同步状态
  GET  /data-sync/tasks          获取最近同步任务列表
  POST /data-sync/run            执行单分类同步
  POST /data-sync/run-all        执行全量同步 (所有分类)
  POST /data-sync/cancel/{id}    取消同步任务
"""

import asyncio
import logging
from typing import List, Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from src.schemas.response import BaseResponse, ok
from src.services.auth_service import verify_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data-sync", tags=["DataSync"])


# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------

class RunSyncRequest(BaseModel):
    category: str = Field(..., description="数据分类 ID")
    sync_type: str = Field(default="full", description="full=全量, incremental=增量, resume=续传(仅支持 kline/financial/peer_comparison/news/capital_flow/top_holder/holder_count)")
    symbols: Optional[list] = Field(default=None, description="指定股票代码列表 (可选, 为空则全量)")


class RunAllSyncRequest(BaseModel):
    sync_type: str = Field(default="full", description="full=全量, incremental=增量")


class SetIntervalRequest(BaseModel):
    intervals: dict = Field(default_factory=dict, description="分类ID → 频率(秒) 映射, 如 {\"kline\": 3600, \"margin\": 86400}")
    use_proxy: Optional[dict] = Field(default=None, description="分类ID → 是否使用代理 映射, 如 {\"kline\": true, \"stock_list\": false}")
    schedule_enabled: Optional[bool] = Field(default=None, description="是否启用定时增量（每60秒按频率自动触发），默认 True")
    worker_concurrency: Optional[int] = Field(default=None, description="Worker 最大并发数，1~8，默认 3")
    sync_ak_config: Optional[dict] = Field(default=None, description="AK 重试配置: retry_count, timeout_seconds, replace_after_failures")


class DeleteTasksRequest(BaseModel):
    task_ids: List[int] = Field(..., description="要删除的任务 ID 列表")


class KlineRetryFailedRequest(BaseModel):
    task_id: int = Field(..., description="K 线任务 ID，将仅重试该任务的 failed_symbols")


class FinancialRetryFailedRequest(BaseModel):
    task_id: int = Field(..., description="财务指标任务 ID，将仅重试该任务的 failed_symbols")


class ProxyPoolConfigBody(BaseModel):
    """所有字段 Optional 且默认 None，使只传部分字段时 model_dump(exclude_none=True) 仅包含已传字段，避免「保存周期」覆盖「保存配置」等未传字段。"""
    url: Optional[str] = Field(default=None, description="代理池 API 根地址，如 http://127.0.0.1:5010")
    enabled: Optional[bool] = Field(default=None, description="是否启用代理池")
    concurrent: Optional[int] = Field(default=None, description="并发数，为数据拉取预留")
    schedule_enabled: Optional[bool] = Field(default=None, description="是否启用定时更新（第一条线）")
    schedule_interval_seconds: Optional[int] = Field(default=None, description="定时更新周期（秒），默认 1 小时")
    business_test_url: Optional[str] = Field(default=None, description="业务校验 URL，留空用 httpbin")
    redis_url: Optional[str] = Field(default=None, description="validated_proxy 使用的 Redis 连接 URL，留空则用环境变量")
    verbose_log: Optional[bool] = Field(default=None, description="开启详细日志时接口返回 log_entries")
    proxy_file_urls: Optional[List[str]] = Field(default=None, description="代理文件 URL 列表，每行一个或逗号分隔，拉取后解析为 raw 候选")
    download_proxy: Optional[str] = Field(default=None, description="下载代理文件时使用的 HTTP 代理，如 http://127.0.0.1:10809")
    fast_schedule_enabled: Optional[bool] = Field(default=None, description="是否启用快速更新代理池（第二条线）")
    fast_schedule_interval_seconds: Optional[int] = Field(default=None, description="快速更新周期（秒），默认 600")
    fast_schedule_last_run_at: Optional[int] = Field(default=None, description="快速更新上次执行时间戳（只读由后端更新）")
    fast_concurrency: Optional[int] = Field(default=None, description="快速更新并发上限，实际并发=min(池内IP数, 此值)，默认 500，范围 1–500")


class ProxyPoolRefreshBody(BaseModel):
    verbose: Optional[bool] = Field(default=False, description="为 true 时返回详细日志 log_entries")


class ProxyPoolTestBatchBody(BaseModel):
    limit: Optional[int] = Field(default=20, description="测试代理数量上限")
    verbose: Optional[bool] = Field(default=False, description="为 true 时返回详细日志 log_entries")
    sorted_by_latency: Optional[bool] = Field(default=False, description="为 true 时按延迟升序取代理再测试，用于合一表格")


class ProxyPoolDeleteInvalidBody(BaseModel):
    proxies: List[str] = Field(..., description="要剔除的代理列表 ip:port")
    verbose: Optional[bool] = Field(default=False, description="为 true 时返回详细日志 log_entries")


# ---------------------------------------------------------------------------
# 权限检查
# ---------------------------------------------------------------------------

async def _require_admin(authorization: str = Header(None)):
    """只有 admin 角色才能触发数据同步"""
    if not authorization:
        raise HTTPException(401, "未提供认证令牌")
    user = await verify_token(authorization.replace("Bearer ", ""))
    if not user:
        raise HTTPException(401, "认证失败")
    return user


# ---------------------------------------------------------------------------
# 端点
# ---------------------------------------------------------------------------

@router.get("/queue-status", response_model=BaseResponse)
async def queue_status():
    """获取 Redis 队列长度，用于诊断：若 queue_len>0 且无任务在执行，说明 Worker 未启动或异常。"""
    try:
        from src.core.streams import get_redis_client
        from src.services.data_service.sync_task_record_service import DATA_SYNC_QUEUE_KEY
        client = await get_redis_client()
        queue_len = await client.llen(DATA_SYNC_QUEUE_KEY)
        return ok(data={"queue_len": queue_len})
    except Exception as e:
        logger.warning("queue_status failed: %s", e)
        return ok(data={"queue_len": -1, "error": str(e)[:100]})


@router.get("/categories", response_model=BaseResponse)
async def list_categories():
    """获取所有数据分类及同步状态（含 log_entries 聚合日志）"""
    from src.services.data_service.data_sync_service import get_sync_status
    status = await get_sync_status()
    return ok(data=status)


@router.get("/logs", response_model=BaseResponse)
async def get_sync_logs_route(limit: int = 200):
    """获取数据拉取聚合日志，供「历史请求」展示。默认最多 200 条。"""
    from src.services.data_service.data_sync_service import get_sync_log_entries
    entries = await get_sync_log_entries(limit=min(max(1, limit), 500))
    return ok(data={"log_entries": entries})


@router.get("/trade-calendar-dates", response_model=BaseResponse)
async def get_trade_calendar_dates(year: int, month: int):
    """获取指定年月的交易日列表，用于前端日历展示开市/休市。返回 { dates: [\"YYYY-MM-DD\", ...] }"""
    from sqlalchemy import select
    from src.core.db import get_session
    from src.models.market_sync import ExchangeTradingDate

    if not (1 <= month <= 12):
        raise HTTPException(400, "month 须在 1–12")
    start = f"{year}-{month:02d}-01"
    if month == 12:
        end = f"{year}-12-31"
    else:
        end = f"{year}-{month+1:02d}-01"
    # 当月最后一天：下月1日减1天
    from datetime import date
    try:
        end_d = date(year, month + 1, 1) if month < 12 else date(year + 1, 1, 1)
        from datetime import timedelta
        end_d = end_d - timedelta(days=1)
        end = end_d.strftime("%Y-%m-%d")
    except Exception:
        end = f"{year}-{month:02d}-31"

    dates: List[str] = []
    async for session in get_session():
        stmt = (
            select(ExchangeTradingDate.trade_date)
            .where(ExchangeTradingDate.trade_date >= start, ExchangeTradingDate.trade_date <= end)
            .order_by(ExchangeTradingDate.trade_date)
        )
        result = await session.execute(stmt)
        rows = result.all()
        dates = [str(r[0]) for r in rows]
        break
    return ok(data={"dates": dates})


@router.get("/northbound-version", response_model=BaseResponse)
async def northbound_version():
    """返回北向资金同步代码版本，用于确认后端已加载新代码（重启后应为 stock_hsgt_hist_em_v1）"""
    from src.services.data_service.data_sync_service import NORTHBOUND_SYNC_VERSION
    return ok(data={"northbound_sync_version": NORTHBOUND_SYNC_VERSION})


@router.get("/tasks", response_model=BaseResponse)
async def list_tasks(page: int = 1, page_size: int = 15):
    """分页获取同步任务列表。page 从 1 开始；page_size 仅允许 15/25/50/100，默认 15。"""
    from src.services.data_service.sync_task_record_service import get_sync_tasks_paged
    allowed = (15, 25, 50, 100)
    if page_size not in allowed:
        page_size = 15 if page_size < 15 else 100
    page = max(1, page)
    data = await get_sync_tasks_paged(page=page, page_size=page_size)
    return ok(data=data)


@router.get("/tasks/{task_id}/logs", response_model=BaseResponse)
async def get_task_logs(task_id: int, page: int = 1, page_size: int = 100):
    """分页获取任务日志（摘要与关键行）"""
    from src.services.data_service.sync_task_record_service import get_task_logs as _get_task_logs
    data = await _get_task_logs(task_id, page=page, page_size=min(page_size, 500))
    return ok(data=data)


@router.post("/set-interval", response_model=BaseResponse)
async def set_interval(body: SetIntervalRequest, authorization: str = Header(None)):
    """保存用户自定义同步频率"""
    await _require_admin(authorization)

    from src.services.data_service.data_sync_service import CATEGORY_IDS, set_sync_intervals

    # 校验: 只允许已知分类ID
    invalid = [k for k in body.intervals if k not in CATEGORY_IDS]
    if invalid:
        raise HTTPException(400, f"未知分类: {invalid}, 可用: {CATEGORY_IDS}")

    # 校验: 值必须为正整数 (秒)
    cleaned: dict = {}
    for k, v in body.intervals.items():
        try:
            iv = int(v)
            if iv < 60:
                raise HTTPException(400, f"频率 {k}={iv}s 过低, 最小 60 秒")
            cleaned[k] = iv
        except (ValueError, TypeError):
            raise HTTPException(400, f"频率值无效: {k}={v}, 需要正整数(秒)")

    ok_flag = await set_sync_intervals(cleaned)
    if not ok_flag:
        raise HTTPException(500, "保存频率配置失败")
    out = {"saved": len(cleaned), "intervals": cleaned}
    if body.use_proxy is not None:
        from src.services.data_service.data_sync_service import set_sync_use_proxy
        use_proxy_cleaned = {k: bool(v) for k, v in body.use_proxy.items() if k in CATEGORY_IDS}
        if use_proxy_cleaned:
            await set_sync_use_proxy(use_proxy_cleaned)
        out["use_proxy_saved"] = len(use_proxy_cleaned)
    if body.schedule_enabled is not None:
        from src.services.data_service.data_sync_service import set_sync_schedule_enabled
        await set_sync_schedule_enabled(body.schedule_enabled)
        out["schedule_enabled"] = body.schedule_enabled
    if body.worker_concurrency is not None:
        if not (1 <= body.worker_concurrency <= 8):
            raise HTTPException(400, "worker_concurrency 须在 1~8 之间")
        from src.services.data_service.data_sync_service import set_sync_worker_concurrency
        ok_flag = await set_sync_worker_concurrency(body.worker_concurrency)
        if not ok_flag:
            raise HTTPException(500, "保存 Worker 并发数失败")
        out["worker_concurrency"] = body.worker_concurrency
    if body.sync_ak_config is not None:
        from src.services.data_service.akshare_call_service import set_sync_ak_config
        ok_flag = await set_sync_ak_config(
            retry_count=body.sync_ak_config.get("retry_count"),
            timeout_seconds=body.sync_ak_config.get("timeout_seconds"),
            replace_after_failures=body.sync_ak_config.get("replace_after_failures"),
        )
        if not ok_flag:
            raise HTTPException(500, "保存 AK 重试配置失败")
        out["sync_ak_config"] = body.sync_ak_config
    return ok(data=out)


@router.post("/run", response_model=BaseResponse)
async def run_sync(body: RunSyncRequest, authorization: str = Header(None)):
    """执行单分类同步（入队由 data_sync_worker 消费执行）"""
    user = await _require_admin(authorization)

    from src.services.data_service.data_sync_service import CATEGORY_IDS
    from src.services.data_service.sync_task_record_service import (
        _create_task,
        enqueue_sync_task,
        _update_task,
    )

    if body.category not in CATEGORY_IDS:
        raise HTTPException(400, f"未知分类: {body.category}, 可用: {CATEGORY_IDS}")

    task_id = await _create_task(body.category, body.sync_type)
    if not task_id:
        raise HTTPException(500, "创建任务失败")

    payload = {
        "task_id": task_id,
        "category": body.category,
        "sync_type": body.sync_type,
        "tenant_id": user.tenant_id,
        "symbols": body.symbols,
    }
    enq_ok = await enqueue_sync_task(payload)
    if not enq_ok:
        await _update_task(task_id, status="failed", error_detail="入队失败")
        raise HTTPException(500, "任务入队失败")
    try:
        from src.services.data_service.data_sync_service import _append_sync_global_log
        await _append_sync_global_log("INFO", f"已入队: {body.category} {body.sync_type}", task_id=task_id, category=body.category)
    except Exception:
        pass
    return ok(data={
        "message": f"数据同步已入队: {body.category} ({body.sync_type})",
        "task_id": task_id,
        "category": body.category,
        "sync_type": body.sync_type,
    })


@router.post("/run-all", response_model=BaseResponse)
async def run_all_sync(body: RunAllSyncRequest, authorization: str = Header(None)):
    """执行全量同步 (所有分类)，按依赖顺序逐条入队"""
    user = await _require_admin(authorization)

    from src.services.data_service.sync_task_record_service import (
        _create_task,
        enqueue_sync_task,
        _update_task,
    )

    # 按 run_sync_all 依赖顺序入队
    order = ["stock_list"]
    order += ["trade_calendar", "sector", "lhb", "northbound", "northbound_hold", "limit_updown", "margin", "block_trade", "dividend", "holder_count", "news"]
    order += ["kline", "financial", "capital_flow", "top_holder", "peer_comparison"]

    enqueued = 0
    for cat_id in order:
        task_id = await _create_task(cat_id, body.sync_type)
        if task_id and await enqueue_sync_task({
            "task_id": task_id,
            "category": cat_id,
            "sync_type": body.sync_type,
            "tenant_id": user.tenant_id,
            "symbols": None,
        }):
            enqueued += 1
            try:
                from src.services.data_service.data_sync_service import _append_sync_global_log
                await _append_sync_global_log("INFO", f"已入队: {cat_id} {body.sync_type}", task_id=task_id, category=cat_id)
            except Exception:
                pass
        elif task_id:
            await _update_task(task_id, status="failed", error_detail="入队失败")

    return ok(data={
        "message": f"全量数据同步已入队 ({body.sync_type})，共 {enqueued} 个分类",
        "sync_type": body.sync_type,
        "enqueued": enqueued,
    })


@router.post("/cancel/{task_id}", response_model=BaseResponse)
async def cancel_task(task_id: int, authorization: str = Header(None)):
    """取消单个同步任务（仅 running 可取消）"""
    await _require_admin(authorization)

    from src.services.data_service.sync_task_record_service import cancel_sync_task

    cancelled = await cancel_sync_task(task_id)
    if not cancelled:
        raise HTTPException(400, "任务不存在或已结束")
    try:
        from src.services.data_service.data_sync_service import _append_sync_global_log
        await _append_sync_global_log("INFO", f"任务 #{task_id} 已取消", task_id=task_id)
    except Exception:
        pass
    return ok(data={"cancelled": True, "task_id": task_id})


@router.post("/cancel-all", response_model=BaseResponse)
async def cancel_all_tasks(authorization: str = Header(None)):
    """取消所有运行中的同步任务"""
    await _require_admin(authorization)

    from src.services.data_service.sync_task_record_service import cancel_all_running_sync_tasks

    cancelled_count = await cancel_all_running_sync_tasks()
    return ok(data={"cancelled_count": cancelled_count})


@router.post("/kline-retry-failed", response_model=BaseResponse)
async def kline_retry_failed(body: KlineRetryFailedRequest, authorization: str = Header(None)):
    """仅对指定 K 线任务的失败代码重试同步，新建一条任务记录"""
    await _require_admin(authorization)

    from src.services.data_service.data_sync_service import kline_retry_failed as _kline_retry_failed

    result = await _kline_retry_failed(body.task_id)
    if not result.get("success"):
        raise HTTPException(400, result.get("error", "重试失败"))
    return ok(data={"task_id": result.get("task_id"), "message": result.get("message")})


@router.post("/financial-retry-failed", response_model=BaseResponse)
async def financial_retry_failed(body: FinancialRetryFailedRequest, authorization: str = Header(None)):
    """仅对指定财务指标任务的失败代码重试同步，新建一条任务记录"""
    await _require_admin(authorization)

    from src.services.data_service.data_sync_service import financial_retry_failed as _financial_retry_failed

    result = await _financial_retry_failed(body.task_id)
    if not result.get("success"):
        raise HTTPException(400, result.get("error", "重试失败"))
    return ok(data={"task_id": result.get("task_id"), "message": result.get("message")})


@router.post("/delete", response_model=BaseResponse)
async def delete_tasks(body: DeleteTasksRequest, authorization: str = Header(None)):
    """批量删除同步任务记录（仅非 running 可删除）"""
    await _require_admin(authorization)

    if not body.task_ids:
        raise HTTPException(400, "task_ids 不能为空")

    from src.services.data_service.sync_task_record_service import delete_sync_tasks

    deleted_count = await delete_sync_tasks(body.task_ids)
    return ok(data={"deleted_count": deleted_count})


# ---------------------------------------------------------------------------
# 代理池（配置中心·网络代理）
# ---------------------------------------------------------------------------

@router.get("/proxy-pool/config", response_model=BaseResponse)
async def get_proxy_pool_config_route(authorization: str = Header(None)):
    """获取代理池配置（url、enabled、concurrent、schedule_*）及只读的 test_domains。"""
    user = await _require_admin(authorization)
    from src.services.data_service.proxy_pool_service import get_proxy_pool_config, get_business_test_domains
    data = await get_proxy_pool_config(user.tenant_id)
    data["test_domains"] = get_business_test_domains()
    return ok(data=data)


@router.post("/proxy-pool/config", response_model=BaseResponse)
async def post_proxy_pool_config(body: ProxyPoolConfigBody, authorization: str = Header(None)):
    """保存代理池配置"""
    user = await _require_admin(authorization)
    from src.services.data_service.proxy_pool_service import set_proxy_pool_config
    data = await set_proxy_pool_config(user.tenant_id, body.model_dump(exclude_none=True))
    return ok(data=data)


@router.get("/proxy-pool/status", response_model=BaseResponse)
async def get_proxy_pool_status(
    verbose: Optional[str] = None, authorization: str = Header(None)
):
    """主数据来自 Redis 本地池；query verbose=1 或 verbose=true 时返回 log_entries。"""
    user = await _require_admin(authorization)
    from src.services.data_service.proxy_pool_service import (
        _fetch_external_reachable,
        fetch_status_from_redis,
        get_proxy_pool_config,
    )
    verbose_flag = verbose is not None and str(verbose).strip().lower() in ("1", "true")
    data = await fetch_status_from_redis(user.tenant_id, verbose=verbose_flag)
    cfg = await get_proxy_pool_config(user.tenant_id)
    base_url = (cfg.get("url") or "").strip()
    data["external_reachable"] = await _fetch_external_reachable(base_url)
    return ok(data=data)


@router.post("/proxy-pool/refresh", response_model=BaseResponse)
async def post_proxy_pool_refresh(
    body: Optional[ProxyPoolRefreshBody] = None, authorization: str = Header(None)
):
    """拉取外部候选 → 业务校验 → 入 Redis 本地池；body.verbose=true 时返回 log_entries。"""
    user = await _require_admin(authorization)
    from src.services.data_service.proxy_pool_service import run_refresh_pipeline
    verbose = body.verbose if body else False
    data = await run_refresh_pipeline(user.tenant_id, verbose=verbose)
    return ok(data=data)


@router.post("/proxy-pool/test-batch", response_model=BaseResponse)
async def post_proxy_pool_test_batch(
    body: Optional[ProxyPoolTestBatchBody] = None, authorization: str = Header(None)
):
    """对 Redis 本地池做业务目标有效性+延迟测试；sorted_by_latency=true 时按延迟升序取代理；verbose 时返回 log_entries。"""
    user = await _require_admin(authorization)
    from src.services.data_service.proxy_pool_service import run_test_batch
    limit = (body and body.limit is not None) and max(1, min(100, body.limit)) or 20
    verbose = body.verbose if body else False
    sorted_by_latency = body.sorted_by_latency if body else False
    out = await run_test_batch(user.tenant_id, limit=limit, verbose=verbose, sorted_by_latency=sorted_by_latency)
    if isinstance(out, dict) and "results" in out:
        return ok(data=out)
    return ok(data={"results": out})


@router.post("/proxy-pool/clear", response_model=BaseResponse)
async def post_proxy_pool_clear(authorization: str = Header(None)):
    """清空本地已校验代理池（Redis validated_proxy），便于清空后执行一键更新重新入池。"""
    user = await _require_admin(authorization)
    from src.services.data_service.proxy_pool_service import clear_proxy_pool
    data = await clear_proxy_pool(user.tenant_id)
    return ok(data=data)


@router.post("/proxy-pool/delete-invalid", response_model=BaseResponse)
async def post_proxy_pool_delete_invalid(body: ProxyPoolDeleteInvalidBody, authorization: str = Header(None)):
    """先从 Redis 移除无效代理，再可选调用外部 /delete/；body.verbose=true 时返回 log_entries。"""
    user = await _require_admin(authorization)
    if not body.proxies:
        return ok(data={"deleted": [], "failed": []})
    from src.services.data_service.proxy_pool_service import get_proxy_pool_config, delete_invalid_proxies
    cfg = await get_proxy_pool_config(user.tenant_id)
    base_url = (cfg.get("url") or "").strip() or None
    data = await delete_invalid_proxies(
        body.proxies, base_url=base_url, verbose=body.verbose or False, tenant_id=user.tenant_id
    )
    return ok(data=data)
