"""策略部署服务：校验回测、网关配置，更新策略 status 与 run_env。"""

from typing import Any, Optional

from fastapi import HTTPException

from src.models.strategy import Strategy
from src.services.config_center_service import get_config
from src.services.strategy_service import get_strategy, update_strategy


def _is_sim_gateway(gw: dict) -> bool:
    """判定是否为模拟盘网关。"""
    if gw.get("type") == "simulated":
        return True
    env = (gw.get("config") or {}).get("env")
    if env is not None:
        return env == "sim"
    return False


def _is_live_gateway(gw: dict) -> bool:
    """判定是否为实盘网关。"""
    if gw.get("type") == "simulated":
        return False
    env = (gw.get("config") or {}).get("env")
    if env is not None:
        return env == "live"
    # 无 env 字段的类型（如 ctp/qmt）暂约定视为实盘
    return True


async def _get_gateways(tenant_id: str) -> list[dict]:
    """获取当前租户的网关列表。"""
    gw_cfg = await get_config(tenant_id, "default", "gateways")
    gw_list = gw_cfg.get("value") if gw_cfg else None
    if not isinstance(gw_list, list):
        return []
    return gw_list


async def get_deploy_eligibility(tenant_id: str) -> dict:
    """
    返回部署资格：sim 是否可部署、live 是否可部署、live 不可用时的说明。
    """
    gw_list = await _get_gateways(tenant_id)
    sim_ok = any(_is_sim_gateway(g) for g in gw_list)
    live_ok = False
    live_message = ""

    cfg = await get_config(tenant_id, "default", "feature_live_trading")
    if not cfg or cfg.get("value") is not True:
        live_message = "实盘交易未开放，请联系管理员"
    else:
        live_gateways = [g for g in gw_list if _is_live_gateway(g)]
        if not live_gateways:
            live_message = "请联系管理员，在管理端配置实盘网关"
        else:
            live_ok = True

    return {"sim": sim_ok, "live": live_ok, "live_message": live_message}


def _mask_account(gw: dict) -> str:
    """从网关 config 取 user_id 或 account，脱敏为 ***后三位。"""
    cfg = gw.get("config") or {}
    raw = cfg.get("user_id") or cfg.get("account") or ""
    if not raw or len(raw) <= 3:
        return "***"
    return "***" + str(raw)[-3:]


async def get_deploy_gateways(tenant_id: str, target: str) -> list[dict[str, Any]]:
    """
    获取可用于部署的网关列表（按 target 过滤），脱敏，不返回 password。
    返回项：index（在完整 gateways 列表中的索引）、label、type、account_display。
    """
    gw_list = await _get_gateways(tenant_id)
    if target == "live":
        cfg = await get_config(tenant_id, "default", "feature_live_trading")
        if not cfg or cfg.get("value") is not True:
            return []
        gateways = [g for g in gw_list if _is_live_gateway(g)]
    else:
        gateways = [g for g in gw_list if _is_sim_gateway(g)]

    result = []
    for i, gw in enumerate(gw_list):
        if gw not in gateways:
            continue
        result.append({
            "index": i,
            "label": gw.get("label") or gw.get("type") or str(i),
            "type": gw.get("type", ""),
            "account_display": _mask_account(gw),
        })
    return result


async def deploy_strategy(
    tenant_id: str,
    user_id: str,
    strategy_id: str,
    target: str,
    gateway_id: Optional[str] = None,
    gateway_account: Optional[str] = None,
    gateway_password: Optional[str] = None,
) -> Strategy:
    """
    部署策略到模拟盘或实盘。
    校验：策略存在、已回测、网关配置；更新 status=running、params_json.run_env。
    失败时抛出 HTTPException(403/400)。
    """
    strategy = await get_strategy(tenant_id, strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail={"code": "not_found", "message": "策略不存在"})

    # 1. 回测校验
    if not strategy.last_backtest_id and strategy.status not in ("backtested", "published", "running"):
        raise HTTPException(
            status_code=403,
            detail={"code": "backtest_required", "message": "请先完成回测验证"},
        )

    gw_list = await _get_gateways(tenant_id)

    # 2. target=sim：至少有一个模拟盘网关
    if target == "sim":
        sim_gateways = [g for g in gw_list if _is_sim_gateway(g)]
        if not sim_gateways:
            raise HTTPException(
                status_code=403,
                detail={"code": "gateway_required", "message": "请联系管理员，在管理端配置模拟盘网关"},
            )

    # 3. target=live：实盘开关 + 至少有一个实盘网关
    if target == "live":
        cfg = await get_config(tenant_id, "default", "feature_live_trading")
        if not cfg or cfg.get("value") is not True:
            raise HTTPException(
                status_code=403,
                detail={"code": "live_disabled", "message": "实盘交易未开放，请联系管理员"},
            )
        live_gateways = [g for g in gw_list if _is_live_gateway(g)]
        if not live_gateways:
            raise HTTPException(
                status_code=403,
                detail={"code": "gateway_required", "message": "请联系管理员，在管理端配置实盘网关"},
            )

    # 4. gateway_id 校验（可选）
    if gateway_id is not None and gateway_id != "":
        try:
            idx = int(gateway_id)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail={"code": "invalid_gateway", "message": "所选网关无效或不可用"},
            )
        if idx < 0 or idx >= len(gw_list):
            raise HTTPException(
                status_code=400,
                detail={"code": "invalid_gateway", "message": "所选网关无效或不可用"},
            )
        gw = gw_list[idx]
        if target == "sim" and not _is_sim_gateway(gw):
            raise HTTPException(
                status_code=400,
                detail={"code": "invalid_gateway", "message": "所选网关无效或不可用"},
            )
        if target == "live" and not _is_live_gateway(gw):
            raise HTTPException(
                status_code=400,
                detail={"code": "invalid_gateway", "message": "所选网关无效或不可用"},
            )

    # 5. 更新策略（写入 run_env、可选 deploy_gateway_index / deploy_gateway_account；密码不落库，仅当次连接用）
    params = dict(strategy.params_json or {})
    params["run_env"] = target
    if gateway_id is not None and gateway_id != "":
        try:
            params["deploy_gateway_index"] = int(gateway_id)
        except ValueError:
            pass
    if gateway_account:
        params["deploy_gateway_account"] = gateway_account
    # gateway_password 不写入 params，后续连接时由前端或当次请求提供

    updated = await update_strategy(
        tenant_id,
        strategy_id,
        logic_code=None,
        params_json=params,
        status="running",
    )
    if not updated:
        raise HTTPException(status_code=404, detail={"code": "not_found", "message": "策略不存在"})
    return updated
