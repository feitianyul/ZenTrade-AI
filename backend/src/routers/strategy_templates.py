"""策略模板 API：列表/详情任意登录用户可读，增删改仅系统管理员"""
from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_db
from src.models.strategy_template import StrategyTemplate
from src.schemas.response import BaseResponse, ok
from src.schemas.strategy_template import (
    StrategyTemplateCreate,
    StrategyTemplateListResponse,
    StrategyTemplateOut,
    StrategyTemplateUpdate,
)
from src.schemas.user import UserOut
from src.services.auth_service import get_user_from_token
from src.services.permission_service import is_admin

router = APIRouter(prefix="/strategy-templates", tags=["策略模板"])
SYSTEM_TENANT_ID = "system"
# 兼容当前仅使用 public 租户：public 租户下的 admin 也视为可管理策略模板，无需改为 system 导致无法登录
PUBLIC_TENANT_ID = "public"


async def _is_template_admin(tenant_id: str, user_id: str) -> bool:
    """当前用户是否为「可管理策略模板」的管理员：system 或 public 租户下的 admin"""
    if tenant_id == SYSTEM_TENANT_ID and await is_admin(SYSTEM_TENANT_ID, user_id):
        return True
    if tenant_id == PUBLIC_TENANT_ID and await is_admin(PUBLIC_TENANT_ID, user_id):
        return True
    return False


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


async def _require_system_admin(authorization: str | None) -> UserOut:
    """系统租户或 public 租户下的 admin 可写模板（兼容全站 tenant_id=public）"""
    user = await _require_user(authorization)
    if not await _is_template_admin(user.tenant_id, user.user_id):
        raise HTTPException(status_code=403, detail="only admin can manage templates")
    return user


def _row_to_out(row: StrategyTemplate) -> StrategyTemplateOut:
    return StrategyTemplateOut(
        id=row.id,
        name=row.name,
        desc=row.desc or "",
        logic=row.logic or "",
        logic_code=row.logic_code,
        icon=row.icon or "fa-chart-line",
        tags=row.tags if row.tags is not None else [],
        intro=row.intro,
        pros=row.pros,
        cons=row.cons,
        tp=float(row.tp) if row.tp is not None else 10.0,
        sl=float(row.sl) if row.sl is not None else 8.0,
        sort_order=row.sort_order or 0,
    )


def _validate_template_content(logic: str | None, logic_code: str | None) -> None:
    if (logic or "").strip() or (logic_code or "").strip():
        return
    raise HTTPException(status_code=400, detail="template logic or logic_code is required")


@router.get("", response_model=BaseResponse[StrategyTemplateListResponse])
async def list_templates(
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
) -> BaseResponse[StrategyTemplateListResponse]:
    """列表：仅返回系统级模板，任意登录用户可读；can_manage 仅系统管理员为 true"""
    user = await _require_user(authorization)
    q = select(StrategyTemplate).where(
        StrategyTemplate.tenant_id == SYSTEM_TENANT_ID
    ).order_by(StrategyTemplate.sort_order.asc(), StrategyTemplate.created_at.asc())
    r = await db.execute(q)
    rows = r.scalars().all()
    can_manage = await _is_template_admin(user.tenant_id, user.user_id)
    payload = StrategyTemplateListResponse(
        data=[_row_to_out(x) for x in rows],
        can_manage=can_manage,
    )
    return ok(payload)


@router.get("/{template_id}", response_model=BaseResponse[StrategyTemplateOut])
async def get_template(
    template_id: str,
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
) -> BaseResponse[StrategyTemplateOut]:
    """详情：仅系统模板可读，任意登录用户"""
    await _require_user(authorization)
    q = select(StrategyTemplate).where(
        StrategyTemplate.id == template_id,
        StrategyTemplate.tenant_id == SYSTEM_TENANT_ID,
    )
    r = await db.execute(q)
    row = r.scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="template not found")
    return ok(_row_to_out(row))


@router.post("", response_model=BaseResponse[StrategyTemplateOut])
async def create_template(
    payload: StrategyTemplateCreate,
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
) -> BaseResponse[StrategyTemplateOut]:
    """新增：仅系统管理员"""
    user = await _require_system_admin(authorization)
    _validate_template_content(payload.logic, payload.logic_code)
    row = StrategyTemplate(
        tenant_id=SYSTEM_TENANT_ID,
        name=payload.name,
        desc=payload.desc,
        logic=payload.logic,
        logic_code=payload.logic_code,
        icon=payload.icon,
        tags=payload.tags,
        intro=payload.intro,
        pros=payload.pros,
        cons=payload.cons,
        tp=payload.tp,
        sl=payload.sl,
        sort_order=payload.sort_order,
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return ok(_row_to_out(row))


@router.put("/{template_id}", response_model=BaseResponse[StrategyTemplateOut])
async def update_template(
  template_id: str,
  payload: StrategyTemplateUpdate,
  authorization: str | None = Header(default=None),
  db: AsyncSession = Depends(get_db),
) -> BaseResponse[StrategyTemplateOut]:
    """更新：仅系统管理员，且仅可改系统模板"""
    await _require_system_admin(authorization)
    q = select(StrategyTemplate).where(
        StrategyTemplate.id == template_id,
        StrategyTemplate.tenant_id == SYSTEM_TENANT_ID,
    )
    r = await db.execute(q)
    row = r.scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="template not found")
    data = payload.model_dump(exclude_unset=True)
    _validate_template_content(data.get("logic", row.logic), data.get("logic_code", row.logic_code))
    for k, v in data.items():
        setattr(row, k, v)
    await db.commit()
    await db.refresh(row)
    return ok(_row_to_out(row))


@router.delete("/{template_id}", response_model=BaseResponse[dict])
async def delete_template(
  template_id: str,
  authorization: str | None = Header(default=None),
  db: AsyncSession = Depends(get_db),
) -> BaseResponse[dict]:
    """删除：仅系统管理员，且仅可删系统模板"""
    await _require_system_admin(authorization)
    q = select(StrategyTemplate).where(
        StrategyTemplate.id == template_id,
        StrategyTemplate.tenant_id == SYSTEM_TENANT_ID,
    )
    r = await db.execute(q)
    row = r.scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="template not found")
    await db.delete(row)
    await db.commit()
    return ok({"deleted": template_id})
