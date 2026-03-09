import json
from datetime import date

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_db
from src.core.errors import ValidationError
from src.schemas.response import BaseResponse, ok
from src.schemas.strategy import (
    DeployOut,
    DeployRequest,
    FromAnalysisRequest,
    StrategyCreateRequest,
    StrategyList,
    StrategyOut,
    StrategyParseRequest,
    StrategyParseResult,
    StrategyUpdateRequest,
    StrategyVersionItem,
    StrategyVersionList,
    StrategyVersionSnapshotOut,
)
from src.schemas.user import UserOut
from src.services.ai_service import parse_strategy_prompt
from src.services.ai_usage_service import get_user_ai_limit_and_used, incr_ai_call
from src.services.audit_service import write_audit_log
from src.services.auth_service import get_user_from_token
from src.services.deploy_service import deploy_strategy
from src.services.strategy_service import (
    create_strategy,
    create_strategy_from_analysis,
    delete_strategy,
    get_strategy,
    list_strategies,
    update_strategy,
)
from src.services.strategy_version_service import (
    copy_version,
    delete_version,
    get_version,
    list_versions,
    list_versions_paginated,
    rollback_to_version,
    set_active_version,
)
from src.services.validation_service import validate_text_payload

router = APIRouter()


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.post("/ai/strategy/parse", response_model=BaseResponse[StrategyParseResult])
async def parse_strategy(
    payload: StrategyParseRequest,
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
) -> BaseResponse[StrategyParseResult]:
    user = await _require_user(authorization)
    errors = validate_text_payload(payload.prompt, "prompt")
    if errors:
        raise ValidationError("; ".join(errors)).as_http_exception()
    tenant_id = getattr(user, "tenant_id", "public")
    user_role = getattr(user, "role", "beginner")
    result = await parse_strategy_prompt(payload.prompt, db=db, tenant_id=tenant_id, user_role=user_role)
    return ok(result)


# ---- General AI Chat ----
from pydantic import BaseModel, Field
from src.services.ai_service import get_llm_router, get_agent_prompt

class AiChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    agent: str = "strategy_parse"  # strategy_parse | replay_analysis | data_validate | alert_reason
    history: list = []  # [{role, content}, ...]

class AiChatResponse(BaseModel):
    reply: str
    agent: str
    model: str = ""
    disclaimer: str = "【智能辅助提示】本内容仅为工具性分析，不构成任何投资建议，投资决策请谨慎。"

@router.post("/ai/chat", response_model=BaseResponse[AiChatResponse],
             summary="AI 助手通用对话", description="支持多 Agent 路由的通用 AI 对话接口")
async def ai_chat(
    payload: AiChatRequest,
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
):
    user = await _require_user(authorization)
    tenant_id = getattr(user, "tenant_id", "public")
    user_role = getattr(user, "role", "beginner")

    router_inst = await get_llm_router(db, tenant_id)
    if not router_inst:
        return ok(AiChatResponse(
            reply="AI 助手暂未启用，请联系管理员配置 LLM 模型。",
            agent=payload.agent,
        ))

    today = date.today().isoformat()
    ai_limit, ai_used = await get_user_ai_limit_and_used(db, user.user_id, tenant_id, today)
    if ai_used >= ai_limit:
        return ok(AiChatResponse(
            reply=f"今日 AI 调用次数已达上限（{ai_used}/{ai_limit}），请明日再试或联系管理员调整限额。",
            agent=payload.agent,
        ))

    system_prompt = await get_agent_prompt(db, tenant_id, payload.agent, user_role)

    # Build messages from history + current
    messages = []
    for h in (payload.history or [])[-10:]:  # last 10 turns
        messages.append({"role": h.get("role", "user"), "content": h.get("content", "")})
    messages.append({"role": "user", "content": payload.message})

    result = await router_inst.chat(messages=messages, system_prompt=system_prompt)

    if result.get("error"):
        return ok(AiChatResponse(
            reply=f"AI 请求失败：{result.get('message', '未知错误')}。请稍后重试。",
            agent=payload.agent,
        ))

    await incr_ai_call(user.user_id, today)

    return ok(AiChatResponse(
        reply=result.get("content", ""),
        agent=payload.agent,
        model=result.get("model", ""),
    ))


@router.get("/strategy", response_model=BaseResponse[StrategyList])
async def list_strategy(
    page: int = 1,
    limit: int = 20,
    authorization: str | None = Header(default=None),
) -> BaseResponse[StrategyList]:
    user = await _require_user(authorization)
    items, total = await list_strategies(user.tenant_id, page, limit, owner_id=user.user_id)
    return ok(
        StrategyList(
            items=[
                StrategyOut(
                    strategy_id=item.id,
                    tenant_id=item.tenant_id,
                    name=item.name,
                    status=item.status,
                    logic_desc=getattr(item, "logic_desc", None),
                    logic_code=item.logic_code,
                    params_json=item.params_json,
                    created_at=str(item.created_at) if item.created_at else None,
                    version=1,
                    last_backtest_id=getattr(item, "last_backtest_id", None),
                    last_backtest_grade=getattr(item, "last_backtest_grade", None),
                    last_backtest_metrics=getattr(item, "last_backtest_metrics", None),
                    source_report_id=getattr(item, "source_report_id", None),
                )
                for item in items
            ],
            page=page,
            limit=limit,
            total=total,
        )
    )


@router.post("/strategy/from-analysis", response_model=BaseResponse[dict])
async def from_analysis_endpoint(
    payload: FromAnalysisRequest,
    request: Request,
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """根据分析报告生成策略草稿并落库；同一 report_id 重复调用返回已存在的 strategy_id（幂等）。报告不存在或非本租户返回 404。"""
    user = await _require_user(authorization)
    strategy_id = await create_strategy_from_analysis(
        report_id=payload.report_id,
        tenant_id=user.tenant_id,
        owner_id=user.user_id,
    )
    if strategy_id is None:
        raise HTTPException(status_code=404, detail="report not found or access denied")
    ip_address = _get_client_ip(request)
    user_agent = (request.headers.get("user-agent") or "")[:256]
    await write_audit_log(
        tenant_id=user.tenant_id,
        actor_id=user.user_id,
        action="strategy_from_analysis",
        resource_type="strategy",
        resource_id=strategy_id,
        status="success",
        ip_address=ip_address,
        user_agent=user_agent,
        detail=json.dumps({"report_id": payload.report_id}, ensure_ascii=False),
    )
    return ok({"strategy_id": strategy_id})


@router.post("/strategy", response_model=BaseResponse[StrategyOut])
async def create_strategy_endpoint(
    payload: StrategyCreateRequest,
    authorization: str | None = Header(default=None),
) -> BaseResponse[StrategyOut]:
    user = await _require_user(authorization)
    errors = []
    errors.extend(validate_text_payload(payload.name, "name"))
    if payload.logic_code and payload.logic_code.strip():
        errors.extend(validate_text_payload(payload.logic_code, "logic_code"))
    if not (payload.logic_desc or "").strip() and not (payload.logic_code or "").strip():
        errors.append("logic_desc or logic_code is required")
    if errors:
        raise ValidationError("; ".join(errors)).as_http_exception()
    try:
        record = await create_strategy(
            user.tenant_id,
            user.user_id,
            payload.name,
            payload.logic_code,
            payload.params_json,
            logic_desc=payload.logic_desc,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return ok(
        StrategyOut(
            strategy_id=record.id,
            tenant_id=record.tenant_id,
            name=record.name,
            status=record.status,
            logic_desc=getattr(record, "logic_desc", None),
            logic_code=record.logic_code,
            params_json=record.params_json,
            created_at=str(record.created_at) if record.created_at else None,
            source_report_id=getattr(record, "source_report_id", None),
        )
    )


@router.get("/strategy/{strategy_id}", response_model=BaseResponse[StrategyOut],
            summary="获取单条策略", description="用于导出等场景拉取完整策略")
async def get_strategy_endpoint(
    strategy_id: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[StrategyOut]:
    user = await _require_user(authorization)
    record = await get_strategy(user.tenant_id, strategy_id)
    if not record or record.owner_id != user.user_id:
        raise HTTPException(status_code=404, detail="strategy not found")
    return ok(
        StrategyOut(
            strategy_id=record.id,
            tenant_id=record.tenant_id,
            name=record.name,
            status=record.status,
            logic_desc=getattr(record, "logic_desc", None),
            logic_code=record.logic_code,
            params_json=record.params_json,
            created_at=str(record.created_at) if record.created_at else None,
            version=1,
            last_backtest_id=getattr(record, "last_backtest_id", None),
            last_backtest_grade=getattr(record, "last_backtest_grade", None),
            last_backtest_metrics=getattr(record, "last_backtest_metrics", None),
            source_report_id=getattr(record, "source_report_id", None),
        )
    )


@router.delete("/strategy/{strategy_id}", summary="软删除策略")
async def delete_strategy_endpoint(
    strategy_id: str,
    authorization: str | None = Header(default=None),
):
    user = await _require_user(authorization)
    record = await get_strategy(user.tenant_id, strategy_id)
    if not record or record.owner_id != user.user_id:
        raise HTTPException(status_code=404, detail="strategy not found")
    ok_ = await delete_strategy(user.tenant_id, strategy_id)
    if not ok_:
        raise HTTPException(status_code=404, detail="strategy not found")
    return ok({"message": "ok"})


@router.put("/strategy/{strategy_id}", response_model=BaseResponse[StrategyOut],
            summary="更新策略", description="更新策略名称、逻辑代码、参数（含标的绑定）")
async def update_strategy_endpoint(
    strategy_id: str,
    payload: StrategyUpdateRequest,
    authorization: str | None = Header(default=None),
) -> BaseResponse[StrategyOut]:
    user = await _require_user(authorization)
    current = await get_strategy(user.tenant_id, strategy_id)
    if not current or current.owner_id != user.user_id:
        raise HTTPException(status_code=404, detail="strategy not found")
    errors = []
    if payload.name is not None:
        errors.extend(validate_text_payload(payload.name, "name"))
    if payload.logic_code is not None and payload.logic_code.strip():
        errors.extend(validate_text_payload(payload.logic_code, "logic_code"))
    next_logic_desc = payload.logic_desc if payload.logic_desc is not None else getattr(current, "logic_desc", None)
    next_logic_code = payload.logic_code if payload.logic_code is not None else current.logic_code
    if not (next_logic_desc or "").strip() and not (next_logic_code or "").strip():
        errors.append("logic_desc or logic_code is required")
    if errors:
        raise ValidationError("; ".join(errors)).as_http_exception()
    try:
        record = await update_strategy(
            user.tenant_id,
            strategy_id,
            name=payload.name,
            logic_code=payload.logic_code,
            logic_desc=payload.logic_desc,
            params_json=payload.params_json,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if not record:
        raise HTTPException(status_code=404, detail="strategy not found")
    return ok(
        StrategyOut(
            strategy_id=record.id,
            tenant_id=record.tenant_id,
            name=record.name,
            status=record.status,
            logic_desc=getattr(record, "logic_desc", None),
            logic_code=record.logic_code,
            params_json=record.params_json,
            created_at=str(record.created_at) if record.created_at else None,
            version=1,
            last_backtest_id=getattr(record, "last_backtest_id", None),
            last_backtest_grade=getattr(record, "last_backtest_grade", None),
            last_backtest_metrics=getattr(record, "last_backtest_metrics", None),
            source_report_id=getattr(record, "source_report_id", None),
        )
    )


def _format_version_created_at(dt) -> str:
    if not dt:
        return ""
    if hasattr(dt, "strftime"):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)[:19].replace("T", " ")


def _version_label(version_no: int) -> str:
    if version_no <= 0:
        return "V0"
    return f"V1.{version_no - 1}"


@router.get(
    "/strategy/{strategy_id}/versions",
    response_model=BaseResponse[StrategyVersionList],
    summary="列出策略版本（分页，支持状态筛选）",
)
async def list_strategy_versions_endpoint(
    strategy_id: str,
    page: int = 1,
    page_size: int = 10,
    status: str | None = None,
    sort_by: str = "created_at",
    order: str = "desc",
    authorization: str | None = Header(default=None),
) -> BaseResponse[StrategyVersionList]:
    user = await _require_user(authorization)
    record = await get_strategy(user.tenant_id, strategy_id)
    if not record or record.owner_id != user.user_id:
        raise HTTPException(status_code=404, detail="strategy not found")
    page = max(1, page)
    page_size = min(max(1, page_size), 50)
    status_filter = None if not status or status not in ("enabled", "disabled") else status
    if sort_by not in ("created_at", "version_no", "status"):
        sort_by = "created_at"
    if order not in ("asc", "desc"):
        order = "desc"
    items_with_active, total = await list_versions_paginated(
        user.tenant_id,
        strategy_id,
        page=page,
        page_size=page_size,
        status_filter=status_filter,
        sort_by=sort_by,
        order=order,
    )
    items = [
        StrategyVersionItem(
            version_no=v.version_no,
            version_label=_version_label(v.version_no),
            created_at=_format_version_created_at(getattr(v, "created_at", None)),
            created_by=getattr(v, "created_by", None),
            is_active=is_active,
        )
        for v, is_active in items_with_active
    ]
    return ok(StrategyVersionList(items=items, total=total, page=page, page_size=page_size))


@router.get(
    "/strategy/{strategy_id}/versions/{version_no}",
    response_model=BaseResponse[StrategyVersionSnapshotOut],
    summary="获取指定版本快照（用于编辑/复制并编辑）",
)
async def get_strategy_version_snapshot_endpoint(
    strategy_id: str,
    version_no: int,
    authorization: str | None = Header(default=None),
) -> BaseResponse[StrategyVersionSnapshotOut]:
    user = await _require_user(authorization)
    record = await get_strategy(user.tenant_id, strategy_id)
    if not record or record.owner_id != user.user_id:
        raise HTTPException(status_code=404, detail="strategy not found")
    version = await get_version(user.tenant_id, strategy_id, version_no)
    if not version or not getattr(version, "content_snapshot", None):
        raise HTTPException(status_code=404, detail="version not found")
    snapshot = version.content_snapshot or {}
    return ok(
        StrategyVersionSnapshotOut(
            logic_desc=snapshot.get("logic_desc"),
            logic_code=snapshot.get("logic_code"),
        )
    )


@router.post(
    "/strategy/{strategy_id}/versions/{version_no}/activate",
    response_model=BaseResponse[dict],
    summary="启用该版本（同策略仅一个版本可启用）",
)
async def activate_strategy_version_endpoint(
    strategy_id: str,
    version_no: int,
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict]:
    user = await _require_user(authorization)
    record = await get_strategy(user.tenant_id, strategy_id)
    if not record or record.owner_id != user.user_id:
        raise HTTPException(status_code=404, detail="strategy not found")
    version = await get_version(user.tenant_id, strategy_id, version_no)
    if not version:
        raise HTTPException(status_code=404, detail="version not found")
    ok_act = await set_active_version(user.tenant_id, strategy_id, version_no)
    if not ok_act:
        raise HTTPException(status_code=500, detail="activate failed")
    return ok({"status": "ok", "active_version_no": version_no})


@router.post(
    "/strategy/{strategy_id}/versions/{version_no}/deactivate",
    response_model=BaseResponse[dict],
    summary="禁用该版本",
)
async def deactivate_strategy_version_endpoint(
    strategy_id: str,
    version_no: int,
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict]:
    user = await _require_user(authorization)
    record = await get_strategy(user.tenant_id, strategy_id)
    if not record or record.owner_id != user.user_id:
        raise HTTPException(status_code=404, detail="strategy not found")
    ok_deact = await set_active_version(user.tenant_id, strategy_id, None)
    if not ok_deact:
        raise HTTPException(status_code=500, detail="deactivate failed")
    return ok({"status": "ok"})


@router.delete(
    "/strategy/{strategy_id}/versions/{version_no}",
    response_model=BaseResponse[dict],
    summary="删除指定版本",
)
async def delete_strategy_version_endpoint(
    strategy_id: str,
    version_no: int,
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict]:
    user = await _require_user(authorization)
    record = await get_strategy(user.tenant_id, strategy_id)
    if not record or record.owner_id != user.user_id:
        raise HTTPException(status_code=404, detail="strategy not found")
    ok_del = await delete_version(user.tenant_id, strategy_id, version_no)
    if not ok_del:
        raise HTTPException(status_code=404, detail="version not found or delete failed")
    return ok({"status": "ok"})


@router.post(
    "/strategy/{strategy_id}/versions/{version_no}/copy",
    response_model=BaseResponse[StrategyVersionItem],
    summary="复制该版本为新版本",
)
async def copy_strategy_version_endpoint(
    strategy_id: str,
    version_no: int,
    authorization: str | None = Header(default=None),
) -> BaseResponse[StrategyVersionItem]:
    user = await _require_user(authorization)
    record = await get_strategy(user.tenant_id, strategy_id)
    if not record or record.owner_id != user.user_id:
        raise HTTPException(status_code=404, detail="strategy not found")
    new_version = await copy_version(
        user.tenant_id, strategy_id, version_no, created_by=user.user_id or ""
    )
    if not new_version:
        raise HTTPException(status_code=404, detail="version not found or copy failed")
    return ok(
        StrategyVersionItem(
            version_no=new_version.version_no,
            version_label=_version_label(new_version.version_no),
            created_at=_format_version_created_at(getattr(new_version, "created_at", None)),
            created_by=getattr(new_version, "created_by", None),
            is_active=False,
        )
    )


@router.post(
    "/strategy/{strategy_id}/versions/{version_no}/restore",
    response_model=BaseResponse[StrategyOut],
    summary="恢复策略到指定版本",
)
async def restore_strategy_version_endpoint(
    strategy_id: str,
    version_no: int,
    authorization: str | None = Header(default=None),
) -> BaseResponse[StrategyOut]:
    user = await _require_user(authorization)
    record = await get_strategy(user.tenant_id, strategy_id)
    if not record or record.owner_id != user.user_id:
        raise HTTPException(status_code=404, detail="strategy not found")
    result = await rollback_to_version(user.tenant_id, strategy_id, version_no)
    if result.get("status") == "error":
        raise HTTPException(status_code=404, detail=result.get("message", "version not found"))
    record = await get_strategy(user.tenant_id, strategy_id)
    if not record:
        raise HTTPException(status_code=404, detail="strategy not found")
    return ok(
        StrategyOut(
            strategy_id=record.id,
            tenant_id=record.tenant_id,
            name=record.name,
            status=record.status,
            logic_desc=getattr(record, "logic_desc", None),
            logic_code=record.logic_code,
            params_json=record.params_json,
            created_at=str(record.created_at) if record.created_at else None,
            version=1,
            last_backtest_id=getattr(record, "last_backtest_id", None),
            last_backtest_grade=getattr(record, "last_backtest_grade", None),
            last_backtest_metrics=getattr(record, "last_backtest_metrics", None),
            source_report_id=getattr(record, "source_report_id", None),
        )
    )


def _get_client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for", "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else ""


@router.post(
    "/strategy/{strategy_id}/deploy",
    response_model=BaseResponse[DeployOut],
    summary="部署策略到模拟盘/实盘",
    description="将已回测策略部署到模拟盘(sim)或实盘(live)，需满足回测完成、网关配置等前置条件。",
)
async def deploy_strategy_endpoint(
    strategy_id: str,
    payload: DeployRequest,
    request: Request,
    authorization: str | None = Header(default=None),
) -> BaseResponse[DeployOut]:
    user = await _require_user(authorization)
    tenant_id = getattr(user, "tenant_id", "public")
    user_id = getattr(user, "user_id", "")
    ip_address = _get_client_ip(request)
    user_agent = request.headers.get("user-agent", "")

    try:
        record = await deploy_strategy(
            tenant_id=tenant_id,
            user_id=user_id,
            strategy_id=strategy_id,
            target=payload.target,
            gateway_id=payload.gateway_id,
            gateway_account=payload.gateway_account,
            gateway_password=payload.gateway_password,
        )
        detail = json.dumps(
            {"target": payload.target, "result": "success", "run_env": payload.target},
            ensure_ascii=False,
        )
        await write_audit_log(
            tenant_id=tenant_id,
            actor_id=user_id,
            action="strategy_deploy",
            resource_type="strategy",
            resource_id=strategy_id,
            status="success",
            ip_address=ip_address,
            user_agent=user_agent,
            detail=detail,
        )
        return ok(
            DeployOut(
                strategy_id=record.id,
                target=payload.target,
                status=record.status,
                run_env=payload.target,
            )
        )
    except HTTPException as e:
        detail = json.dumps(
            {
                "target": payload.target,
                "result": "fail",
                "reason": e.detail.get("message", str(e.detail)) if isinstance(e.detail, dict) else str(e.detail),
            },
            ensure_ascii=False,
        )
        await write_audit_log(
            tenant_id=tenant_id,
            actor_id=user_id,
            action="strategy_deploy",
            resource_type="strategy",
            resource_id=strategy_id,
            status="fail",
            ip_address=ip_address,
            user_agent=user_agent,
            detail=detail,
        )
        raise
