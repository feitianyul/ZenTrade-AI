import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_db
from src.models.alert import Alert
from src.schemas.alert import AlertCreate, AlertLevel, AlertStatus
from src.schemas.response import BaseResponse, ok
from src.services.alert_service import create_alert
from src.services.auth_service import verify_token
from src.services.ops_service import (
    get_system_status,
    get_time_sources,
    probe_all_components,
    get_metrics_summary,
)
from src.services.permission_service import has_permission
from src.services.market_warmup_service import (
    check_warmup_running,
    get_warmup_status,
    run_market_warmup,
    set_warmup_running,
)

router = APIRouter(tags=["Ops"])


class MarketWarmupRequest(BaseModel):
    """预热请求体：指定要预热的项"""
    items: List[str] = Field(default=["indices", "hot", "sectors", "ranking", "minute_top20"],
                            description="预热项: indices, hot, sectors, ranking, minute_top20")


async def _require_admin(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.replace("Bearer ", "")
    user = await verify_token(token)
    from src.services.permission_service import is_admin
    if not await is_admin(user.tenant_id, user.user_id):
        raise HTTPException(status_code=403, detail="admin access required")
    return user


@router.get("/ops/status", response_model=BaseResponse[Dict[str, Any]],
            summary="运维状态", description="系统资源、进程信息、DB连接池、Redis状态")
async def get_ops_status(
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    await _require_admin(authorization)
    status = await get_system_status()
    return ok(status)


@router.get("/ops/components", response_model=BaseResponse[List[Dict[str, Any]]],
            summary="组件健康探针", description="对所有10个基础设施/应用/监控组件执行真实健康检查")
async def get_ops_components(
    authorization: str | None = Header(default=None),
) -> BaseResponse[List[Dict[str, Any]]]:
    await _require_admin(authorization)
    components = await probe_all_components()
    return ok(components)


@router.get("/ops/metrics-summary", response_model=BaseResponse[Dict[str, Any]],
            summary="指标摘要", description="解析Prometheus指标返回前端可渲染的JSON摘要")
async def get_ops_metrics(
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    await _require_admin(authorization)
    summary = await get_metrics_summary()
    return ok(summary)


@router.get("/ops/time-sources", response_model=BaseResponse[Dict[str, Any]],
            summary="时间源", description="各组件当前时间(UTC)与一致性检测；不一致时写入告警")
async def get_ops_time_sources(
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_db),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_admin(authorization)
    result = await get_time_sources()
    if result.get("inconsistent") and result.get("details"):
        cutoff = datetime.utcnow() - timedelta(minutes=5)
        q = select(Alert).where(
            Alert.tenant_id == user.tenant_id,
            Alert.source == "time_sync",
            Alert.status == AlertStatus.ACTIVE,
            Alert.created_at >= cutoff,
        )
        r = await session.execute(q)
        if r.scalars().first() is None:
            msg = "; ".join(result["details"])
            await create_alert(
                session,
                user.tenant_id,
                AlertCreate(
                    title="组件时间不一致",
                    message=msg,
                    source="time_sync",
                    level=AlertLevel.WARNING,
                ),
            )
    return ok(result)


@router.post("/ops/market-warmup", response_model=BaseResponse[Dict[str, Any]],
             summary="行情预热", description="手动触发行情数据预热，异步执行，立即返回 accepted")
async def post_market_warmup(
    body: MarketWarmupRequest,
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_admin(authorization)
    if await check_warmup_running():
        raise HTTPException(status_code=409, detail="already_running")
    await set_warmup_running(True)

    async def _run_background() -> None:
        try:
            await run_market_warmup(body.items, tenant_id=user.tenant_id)
        except Exception as e:
            logger.exception("market warmup background task failed: %s", e)
            try:
                from src.services.cache_policy_service import set_cached
                import json
                err_entry = {"ts": datetime.utcnow().isoformat() + "Z", "level": "ERROR", "msg": f"预热异常: {e}"}
                await set_cached("market:warmup:log_entries", json.dumps([err_entry], ensure_ascii=False), ttl=7 * 24 * 3600)
                await set_cached("market:warmup:result", json.dumps({"error": str(e)}, ensure_ascii=False), ttl=7 * 24 * 3600)
            except Exception as inner:
                logger.warning("failed to write warmup error to Redis: %s", inner)
        finally:
            await set_warmup_running(False)

    asyncio.create_task(_run_background())
    return ok({"accepted": True})


@router.get("/ops/market-warmup/status", response_model=BaseResponse[Dict[str, Any]],
            summary="预热状态", description="获取上次预热时间、结果与日志")
async def get_market_warmup_status(
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    await _require_admin(authorization)
    result = await get_warmup_status()
    return ok(result)


@router.get("/ops/warmup-proxy-diagnostic", response_model=BaseResponse[Dict[str, Any]],
            summary="预热代理诊断", description="诊断 minute_top20 代理池可用数，无需执行预热")
async def get_warmup_proxy_diagnostic(
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    """返回当前租户下 eastmoney.com 代理池可用数及预热相关配置，便于排查代理使用情况。"""
    user = await _require_admin(authorization)
    domain = "eastmoney.com"
    out: Dict[str, Any] = {"tenant_id": user.tenant_id, "domain": domain}
    try:
        from src.services.config_center_service import get_config
        cfg_proxy = await get_config(user.tenant_id, "default", "market_warmup_use_proxy")
        use_proxy = str(cfg_proxy.get("value", "") or "").lower() in ("true", "1", "yes") if cfg_proxy else False
        out["market_warmup_use_proxy"] = use_proxy
        cfg_pct = await get_config(user.tenant_id, "default", "market_warmup_pool_pct")
        pool_pct = float(cfg_pct.get("value", 0.6) or 0.6) if cfg_pct else 0.6
        out["market_warmup_pool_pct"] = pool_pct
        cfg_per = await get_config(user.tenant_id, "default", "market_warmup_concurrent_per_proxy")
        per_proxy = int(float(cfg_per.get("value", 1) or 1)) if cfg_per else 1
        out["market_warmup_concurrent_per_proxy"] = per_proxy
        if use_proxy:
            from src.services.data_service.proxy_pool_service import (
                get_proxy_pool_available_count,
                get_proxies_top_pct,
            )
            k = await get_proxy_pool_available_count(user.tenant_id, domain=domain)
            out["available_count"] = k
            out["would_use_pool"] = k >= 2
            if k >= 2:
                active, reserve = await get_proxies_top_pct(user.tenant_id, pct=pool_pct, domain=domain)
                total_sem = len(active) * per_proxy
                out["active_proxies"] = len(active)
                out["reserve_proxies"] = len(reserve)
                out["total_sem"] = total_sem
                out["message"] = f"minute_top20 将使用代理池 N={len(active)}, per_proxy={per_proxy}, sem={total_sem}"
            else:
                out["message"] = f"代理池 {domain} 可用 {k}<2，将沿用单代理 (总并发=1)"
        else:
            out["message"] = "代理未启用，预热将直连拉取"
    except Exception as e:
        out["error"] = str(e)
    return ok(out)
