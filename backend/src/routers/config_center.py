"""T256 - 配置中心 API 与权限控制"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Query

from src.utils.log_entries import log_entry
from pydantic import BaseModel, Field

from src.schemas.response import BaseResponse, ok
from src.services.auth_service import verify_token
from src.services.config_center_service import delete_config, get_config, list_configs, set_config

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ConfigCenter"])


class ConfigSetRequest(BaseModel):
    namespace: str = Field(..., max_length=64)
    key: str = Field(..., max_length=128)
    value: str
    value_type: str = Field(default="string", pattern="^(string|int|float|json|bool)$")
    description: str = ""


async def _require_admin(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.replace("Bearer ", "")
    return await verify_token(token)


@router.post("/config-center", response_model=BaseResponse[Dict[str, Any]])
async def create_or_update_config(
    body: ConfigSetRequest,
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_admin(authorization)
    try:
        result = await set_config(
            user.tenant_id, body.namespace, body.key, body.value, body.value_type, body.description
        )
    except Exception as exc:
        logger.exception("set_config failed")
        raise HTTPException(status_code=500, detail=str(exc))
    return ok(result)


@router.get("/config-center/{namespace}/{key}", response_model=BaseResponse[Dict[str, Any]])
async def read_config(
    namespace: str,
    key: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_admin(authorization)
    result = await get_config(user.tenant_id, namespace, key)
    if not result:
        raise HTTPException(status_code=404, detail="config not found")
    return ok(result)


@router.get("/config-center", response_model=BaseResponse[List[Dict[str, Any]]])
async def list_all_configs(
    namespace: Optional[str] = None,
    limit: int = 100,
    authorization: str | None = Header(default=None),
) -> BaseResponse[List[Dict[str, Any]]]:
    user = await _require_admin(authorization)
    configs = await list_configs(user.tenant_id, namespace, limit)
    return ok(configs)


@router.delete("/config-center/{namespace}/{key}", response_model=BaseResponse[Dict[str, Any]])
async def remove_config(
    namespace: str,
    key: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_admin(authorization)
    result = await delete_config(user.tenant_id, namespace, key)
    return ok(result)


# ---------------------------------------------------------------------------
# 行情数据源分类探测
# ---------------------------------------------------------------------------

class TestMarketSourceRequest(BaseModel):
    source: str = Field(..., description="数据源标识: akshare")
    api_key: str = Field(default="", description="数据源 API Key（免费源可留空）")
    category: str = Field(default="all", description="测试分类: kline/quote/fundamental/technical/hot_rank/institutional/announcement 或 all")
    verbose: bool = Field(default=False, description="是否返回详细测试日志")


@router.post(
    "/config-center/test-market-source",
    summary="测试行情数据源",
    description="按分类探测行情数据源是否可用，返回每个分类的连通性、延迟和样本数据。"
               "verbose=true 时返回详细日志。",
)
async def test_market_source(
    body: TestMarketSourceRequest,
    authorization: str | None = Header(default=None),
):
    await _require_admin(authorization)

    from src.services.data_service.market_source_service import MARKET_SOURCES, probe_market_source

    if body.source not in MARKET_SOURCES:
        raise HTTPException(status_code=400, detail=f"不支持的数据源: {body.source}")

    results = await probe_market_source(body.source, body.api_key, body.category)

    # verbose: 为每个结果附加 log_entries 格式（与预热/代理日志一致）
    if body.verbose and isinstance(results, list):
        for r in results:
            if isinstance(r, dict):
                vlog = [
                    log_entry("INFO", f"数据源: {body.source} | 分类: {r.get('category', body.category)}"),
                    log_entry("INFO" if r.get("success") else "ERROR", f"结果: {'成功' if r.get('success') else '失败'}"),
                ]
                if r.get("latency_ms"):
                    vlog.append(log_entry("INFO", f"延迟: {r['latency_ms']}ms"))
                if r.get("error"):
                    vlog.append(log_entry("ERROR", f"错误: {r['error']}"))
                if r.get("sample_data"):
                    vlog.append(log_entry("INFO", "样本数据 (truncated)", data=str(r["sample_data"])[:500]))
                r["verbose_log"] = vlog
    elif body.verbose and isinstance(results, dict):
        results["verbose_log"] = [
            log_entry("INFO", f"数据源: {body.source} | 分类: {body.category}"),
            log_entry("INFO" if results.get("success") else "ERROR", f"结果: {'成功' if results.get('success') else '失败'}"),
        ]
        if results.get("error"):
            results["verbose_log"].append(log_entry("ERROR", f"错误: {results['error']}"))

    return ok(results)


# ---------------------------------------------------------------------------
# 交易网关连通性测试
# ---------------------------------------------------------------------------

class TestGatewayRequest(BaseModel):
    gateway_type: str = Field(..., description="网关类型: simulated/qmt/ctp/openctp")
    config: Dict[str, Any] = Field(default_factory=dict, description="网关配置参数")
    verbose: bool = Field(default=False, description="是否返回详细测试日志")


@router.post(
    "/config-center/test-gateway",
    summary="测试交易网关连通性",
    description="测试指定交易网关是否可连接。verbose=true 时返回详细日志。",
)
async def test_gateway(
    body: TestGatewayRequest,
    authorization: str | None = Header(default=None),
):
    import time

    await _require_admin(authorization)
    vlog: List[Dict] = []

    if body.verbose:
        # 脱敏: 隐藏密码字段
        safe_config = {
            k: ("***" if "password" in k.lower() else v)
            for k, v in body.config.items()
        }
        vlog.append(log_entry("INFO", f"网关类型: {body.gateway_type}"))
        vlog.append(log_entry("INFO", "配置参数", data=safe_config))

    t0 = time.time()
    try:
        if body.gateway_type == "simulated":
            latency = int((time.time() - t0) * 1000)
            if body.verbose:
                vlog.append(log_entry("INFO", "模拟网关: 无需外部连接，直接返回成功"))
            return ok({
                "success": True, "latency_ms": latency,
                "gateway_type": "simulated",
                "message": "模拟交易网关正常",
                **({"verbose_log": vlog} if body.verbose else {}),
            })

        elif body.gateway_type == "qmt":
            if body.verbose:
                vlog.append(log_entry("INFO", "正在连接 QMT 网关..."))
                vlog.append(log_entry("INFO", f"客户端路径: {body.config.get('client_path', '-')}"))
                vlog.append(log_entry("INFO", f"账号: {body.config.get('account', '-')}"))
            from src.services.trading_gateway.qmt_gateway import QmtGateway
            gw = QmtGateway(config=body.config)
            connected = await gw.test_connection()
            latency = int((time.time() - t0) * 1000)
            if body.verbose:
                vlog.append(log_entry("INFO" if connected else "ERROR", f"连接结果: {'成功' if connected else '失败'} | {latency}ms"))
            return ok({
                "success": connected, "latency_ms": latency,
                "gateway_type": "qmt",
                "message": "QMT 网关连接成功" if connected else "QMT 网关连接失败",
                **({"verbose_log": vlog} if body.verbose else {}),
            })

        elif body.gateway_type == "ctp":
            if body.verbose:
                vlog.append(log_entry("INFO", "正在连接 CTP 网关..."))
                vlog.append(log_entry("INFO", f"前置地址: {body.config.get('front_addr', '-')}"))
                vlog.append(log_entry("INFO", f"BrokerID: {body.config.get('broker_id', '-')}"))
            from src.services.trading_gateway.ctp_gateway import CtpGateway
            gw = CtpGateway(config=body.config)
            await gw.connect()
            connected = gw.connected
            await gw.close()
            latency = int((time.time() - t0) * 1000)
            if body.verbose:
                vlog.append(log_entry("INFO" if connected else "ERROR", f"连接结果: {'成功' if connected else '失败'} | {latency}ms"))
            return ok({
                "success": connected, "latency_ms": latency,
                "gateway_type": "ctp",
                "message": "CTP 网关连接成功" if connected else "CTP 网关连接失败",
                **({"verbose_log": vlog} if body.verbose else {}),
            })

        elif body.gateway_type == "openctp":
            if body.verbose:
                env = body.config.get("env", "sim")
                vlog.append(log_entry("INFO", f"正在连接 OpenCTP 网关 (环境: {env})..."))
                vlog.append(log_entry("INFO", f"交易前置: {body.config.get('front_td', '默认模拟')}"))
                vlog.append(log_entry("INFO", f"BrokerID: {body.config.get('broker_id', '9999')}"))
                vlog.append(log_entry("INFO", f"UserID: {body.config.get('user_id', '-')}"))
            from src.services.trading_gateway.openctp_gateway import OpenctpGateway
            gw = OpenctpGateway(config=body.config)
            result = await gw.test_connection()
            latency = result.get("latency_ms", int((time.time() - t0) * 1000))
            if body.verbose:
                vlog.append(log_entry("INFO" if result["success"] else "ERROR", f"连接结果: {'成功' if result['success'] else '失败'} | {latency}ms"))
                if result.get("trading_day"):
                    vlog.append(log_entry("INFO", f"交易日: {result['trading_day']}"))
                if not result["success"]:
                    vlog.append(log_entry("ERROR", f"错误信息: {result.get('message', '')}"))
            return ok({
                "success": result["success"], "latency_ms": latency,
                "gateway_type": "openctp",
                "trading_day": result.get("trading_day", ""),
                "message": result.get("message", ""),
                **({"verbose_log": vlog} if body.verbose else {}),
            })

        else:
            raise HTTPException(status_code=400, detail=f"不支持的网关类型: {body.gateway_type}")

    except HTTPException:
        raise
    except Exception as exc:
        latency = int((time.time() - t0) * 1000)
        logger.exception("test-gateway error")
        if body.verbose:
            vlog.append(log_entry("ERROR", f"异常: {str(exc)[:300]}"))
        return ok({
            "success": False, "latency_ms": latency,
            "gateway_type": body.gateway_type,
            "message": f"连接失败: {str(exc)[:300]}",
            **({"verbose_log": vlog} if body.verbose else {}),
        })
