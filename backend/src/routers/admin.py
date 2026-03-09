from datetime import date

from fastapi import APIRouter, Body, Header, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select, update as sql_update

from src.core.crypto import decrypt_text, encrypt_text
from src.core.db import get_session
from src.services.ai_config_service import AIConfigService
from src.services.ai_usage_service import get_ai_calls_used_batch, resolve_ai_limit
from src.services.masking_service import mask_phone
from src.models.strategy import Strategy
from src.models.user import User
from src.models.user_role import UserRole
from src.models.role import Role
from src.schemas.response import BaseResponse, ok
from src.schemas.user import UserOut
from src.services.audit_service import write_audit_log
from src.services.auth_service import (
    get_user_from_token,
    register_user,
    update_password_by_user_id,
)
from src.services.permission_service import is_admin, set_user_role
from src.services.tenant_access_service import grant_tenant_access

router = APIRouter()

_DEFAULT_AI_LIMITS = {"beginner": 10, "advanced": 30, "expert": 100}


def _mask_phone_safe(encrypted_phone: str | None) -> str:
    """解密后脱敏；解密失败时返回 ***"""
    if not encrypted_phone:
        return ""
    try:
        plain = decrypt_text(encrypted_phone)
        return mask_phone(plain) if plain else "***"
    except Exception:
        return "***"


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.post("/admin/tenant/grant", response_model=BaseResponse[dict[str, str]])
async def grant_tenant(
    target_tenant_id: str,
    authorization: str | None = Header(default=None),
    x_real_ip: str | None = Header(default=None),
    user_agent: str | None = Header(default=None),
) -> BaseResponse[dict[str, str]]:
    user = await _require_user(authorization)
    if not await is_admin(user.tenant_id, user.user_id):
        raise HTTPException(status_code=403, detail="forbidden")
    await grant_tenant_access(user.tenant_id, target_tenant_id, user.user_id)
    
    await write_audit_log(
        tenant_id=user.tenant_id,
        actor_id=user.user_id,
        action="grant_tenant_access",
        resource_type="tenant",
        resource_id=target_tenant_id,
        status="success",
        ip_address=x_real_ip or "unknown",
        user_agent=user_agent or "unknown",
        detail=f"Granted access to tenant {target_tenant_id}",
    )
    
    return ok({"status": "ok"})


@router.get("/admin/users", response_model=BaseResponse[dict])
async def list_users(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """管理员查询用户列表（昵称、角色、状态、策略数等）"""
    user = await _require_user(authorization)
    if not await is_admin(user.tenant_id, user.user_id):
        raise HTTPException(status_code=403, detail="forbidden")

    async for session in get_session():
        # 总数
        count_q = select(func.count(User.id)).where(User.is_deleted.is_(False))
        total = (await session.execute(count_q)).scalar() or 0

        # 分页查询用户
        query = (
            select(User)
            .where(User.is_deleted.is_(False))
            .order_by(User.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        result = await session.execute(query)
        users = result.scalars().all()

        # 获取 AI 限额配置（管理员租户）
        ai_svc = AIConfigService(session)
        limits_cfg = await ai_svc.get_config(user.tenant_id, "ai_usage_limits")
        limits = (
            limits_cfg.value
            if limits_cfg and isinstance(limits_cfg.value, dict)
            else _DEFAULT_AI_LIMITS
        ).copy()
        for k in _DEFAULT_AI_LIMITS:
            limits.setdefault(k, _DEFAULT_AI_LIMITS[k])

        today = date.today().isoformat()
        user_ids = [str(u.id) for u in users]
        used_map = await get_ai_calls_used_batch(user_ids, today)

        user_list = []
        for u in users:
            # 查询策略数
            strat_count_q = (
                select(func.count(Strategy.id))
                .where(Strategy.owner_id == u.id)
                .where(Strategy.is_deleted.is_(False))
            )
            strat_count = (await session.execute(strat_count_q)).scalar() or 0

            # 查询角色
            role_q = (
                select(Role.name)
                .join(UserRole, Role.id == UserRole.role_id)
                .where(UserRole.user_id == u.id)
            )
            role_result = await session.execute(role_q)
            roles = [r[0] for r in role_result.all()]
            role = roles[0] if roles else "beginner"
            override = getattr(u, "ai_calls_limit_override", None)
            ai_limit = resolve_ai_limit(override, role, limits)
            ai_used = used_map.get(str(u.id), 0)

            user_list.append({
                "user_id": str(u.id),
                "phone": _mask_phone_safe(u.phone),
                "nickname": getattr(u, "nickname", None) or "",
                "level": u.level,
                "risk_level": u.risk_level,
                "roles": roles,
                "status": "banned" if getattr(u, "is_banned", False) else "active",
                "strategy_count": strat_count,
                "ai_calls_used": ai_used,
                "ai_calls_limit": ai_limit,
                "ai_calls_limit_override": getattr(u, "ai_calls_limit_override", None),
                "created_at": u.created_at.isoformat() if u.created_at else "",
            })

        return ok({"total": total, "items": user_list, "limit": limit, "offset": offset})
    return ok({"total": 0, "items": [], "limit": limit, "offset": offset})


class CreateUserBody(BaseModel):
    phone: str = Field(..., min_length=6, max_length=32, description="手机号")
    password: str = Field(..., min_length=6, max_length=64)
    nickname: str | None = Field(default=None, max_length=64)
    role: str = Field(default="beginner", description="beginner | advanced | expert | admin")


class UpdateUserBody(BaseModel):
    nickname: str | None = Field(default=None, max_length=64)
    phone: str | None = Field(default=None, min_length=6, max_length=32)
    email: str | None = Field(default=None, max_length=128)
    ai_calls_limit: int | None = Field(default=None, ge=-1, le=9999, description="-1=清除覆盖用角色默认，>=0=设置用户级限额")


class ResetPasswordBody(BaseModel):
    new_password: str = Field(..., min_length=6, max_length=64)


@router.post("/admin/users", response_model=BaseResponse[dict])
async def create_user(
    body: CreateUserBody = Body(...),
    authorization: str | None = Header(default=None),
    x_real_ip: str | None = Header(default=None),
    user_agent: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """管理员创建用户"""
    user = await _require_user(authorization)
    if not await is_admin(user.tenant_id, user.user_id):
        raise HTTPException(status_code=403, detail="forbidden")

    if body.role not in ("beginner", "advanced", "expert", "admin"):
        raise HTTPException(status_code=400, detail="invalid role")

    try:
        new_user = await register_user(
            phone=body.phone,
            password=body.password,
            tenant_id=user.tenant_id,
            nickname=body.nickname,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        await set_user_role(user.tenant_id, new_user.user_id, body.role)
    except ValueError:
        pass

    await write_audit_log(
        tenant_id=user.tenant_id,
        actor_id=user.user_id,
        action="create_user",
        resource_type="user",
        resource_id=new_user.user_id,
        status="success",
        ip_address=x_real_ip or "unknown",
        user_agent=user_agent or "unknown",
        detail=f"Created user {new_user.user_id}",
    )
    return ok({"user_id": new_user.user_id, "status": "ok"})


@router.put("/admin/users/{user_id}", response_model=BaseResponse[dict])
async def update_user(
    user_id: str = Path(..., description="目标用户 ID"),
    body: UpdateUserBody = Body(...),
    authorization: str | None = Header(default=None),
    x_real_ip: str | None = Header(default=None),
    user_agent: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """管理员修改用户（昵称、手机、邮箱）"""
    user = await _require_user(authorization)
    if not await is_admin(user.tenant_id, user.user_id):
        raise HTTPException(status_code=403, detail="forbidden")

    async for session in get_session():
        target = await session.execute(
            select(User).where(User.id == user_id).where(User.is_deleted.is_(False))
        )
        target_user = target.scalar_one_or_none()
        if not target_user:
            raise HTTPException(status_code=404, detail="user not found")

        values = {}
        if body.nickname is not None:
            values["nickname"] = body.nickname
        if body.phone is not None:
            values["phone"] = encrypt_text(body.phone)
        if body.email is not None:
            values["email"] = encrypt_text(body.email) if body.email else None
        if body.ai_calls_limit is not None:
            values["ai_calls_limit_override"] = None if body.ai_calls_limit == -1 else body.ai_calls_limit

        if values:
            await session.execute(
                sql_update(User).where(User.id == user_id).values(**values)
            )
            await session.commit()

        await write_audit_log(
            tenant_id=user.tenant_id,
            actor_id=user.user_id,
            action="update_user",
            resource_type="user",
            resource_id=user_id,
            status="success",
            ip_address=x_real_ip or "unknown",
            user_agent=user_agent or "unknown",
            detail=f"Updated user {user_id}",
        )
        return ok({"status": "ok"})
    raise HTTPException(status_code=500, detail="session unavailable")


@router.put("/admin/users/{user_id}/soft-delete", response_model=BaseResponse[dict])
async def soft_delete_user(
    user_id: str = Path(..., description="目标用户 ID"),
    authorization: str | None = Header(default=None),
    x_real_ip: str | None = Header(default=None),
    user_agent: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """管理员软删除用户"""
    user = await _require_user(authorization)
    if not await is_admin(user.tenant_id, user.user_id):
        raise HTTPException(status_code=403, detail="forbidden")

    async for session in get_session():
        result = await session.execute(
            sql_update(User)
            .where(User.id == user_id)
            .where(User.is_deleted.is_(False))
            .values(is_deleted=True)
        )
        await session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="user not found")

        await write_audit_log(
            tenant_id=user.tenant_id,
            actor_id=user.user_id,
            action="soft_delete_user",
            resource_type="user",
            resource_id=user_id,
            status="success",
            ip_address=x_real_ip or "unknown",
            user_agent=user_agent or "unknown",
            detail=f"Soft deleted user {user_id}",
        )
        return ok({"status": "ok"})
    raise HTTPException(status_code=500, detail="session unavailable")


@router.put("/admin/users/{user_id}/reset-password", response_model=BaseResponse[dict])
async def admin_reset_password(
    user_id: str = Path(..., description="目标用户 ID"),
    body: ResetPasswordBody = Body(...),
    authorization: str | None = Header(default=None),
    x_real_ip: str | None = Header(default=None),
    user_agent: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """管理员重置用户密码"""
    user = await _require_user(authorization)
    if not await is_admin(user.tenant_id, user.user_id):
        raise HTTPException(status_code=403, detail="forbidden")

    async for session in get_session():
        target = await session.execute(
            select(User).where(User.id == user_id).where(User.is_deleted.is_(False))
        )
        target_user = target.scalar_one_or_none()
        if not target_user:
            raise HTTPException(status_code=404, detail="user not found")

        ok_reset = await update_password_by_user_id(
            user_id, body.new_password, tenant_id=target_user.tenant_id
        )
        if not ok_reset:
            raise HTTPException(status_code=500, detail="reset password failed")

        await write_audit_log(
            tenant_id=user.tenant_id,
            actor_id=user.user_id,
            action="admin_reset_password",
            resource_type="user",
            resource_id=user_id,
            status="success",
            ip_address=x_real_ip or "unknown",
            user_agent=user_agent or "unknown",
            detail=f"Admin reset password for user {user_id}",
        )
        return ok({"status": "ok"})
    raise HTTPException(status_code=500, detail="session unavailable")


class SetRoleBody(BaseModel):
    role: str = Field(..., description="beginner | advanced | expert | admin")


@router.put("/admin/users/{user_id}/role", response_model=BaseResponse[dict])
async def set_user_role_endpoint(
    user_id: str = Path(..., description="目标用户 ID"),
    body: SetRoleBody = Body(...),
    authorization: str | None = Header(default=None),
    x_real_ip: str | None = Header(default=None),
    user_agent: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """管理员调整用户等级"""
    user = await _require_user(authorization)
    if not await is_admin(user.tenant_id, user.user_id):
        raise HTTPException(status_code=403, detail="forbidden")

    if body.role not in ("beginner", "advanced", "expert", "admin"):
        raise HTTPException(status_code=400, detail="invalid role")

    async for session in get_session():
        target = await session.execute(
            select(User).where(User.id == user_id).where(User.is_deleted.is_(False))
        )
        target_user = target.scalar_one_or_none()
        if not target_user:
            raise HTTPException(status_code=404, detail="user not found")

        try:
            await set_user_role(target_user.tenant_id, user_id, body.role)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        await write_audit_log(
            tenant_id=user.tenant_id,
            actor_id=user.user_id,
            action="set_user_role",
            resource_type="user",
            resource_id=user_id,
            status="success",
            ip_address=x_real_ip or "unknown",
            user_agent=user_agent or "unknown",
            detail=f"Set user {user_id} role to {body.role}",
        )
        return ok({"status": "ok"})
    raise HTTPException(status_code=500, detail="session unavailable")


@router.put("/admin/users/{user_id}/ban", response_model=BaseResponse[dict])
async def ban_user(
    user_id: str = Path(..., description="目标用户 ID"),
    authorization: str | None = Header(default=None),
    x_real_ip: str | None = Header(default=None),
    user_agent: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """管理员封禁用户"""
    user = await _require_user(authorization)
    if not await is_admin(user.tenant_id, user.user_id):
        raise HTTPException(status_code=403, detail="forbidden")

    async for session in get_session():
        result = await session.execute(
            sql_update(User)
            .where(User.id == user_id)
            .where(User.is_deleted.is_(False))
            .values(is_banned=True)
        )
        await session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="user not found")

        await write_audit_log(
            tenant_id=user.tenant_id,
            actor_id=user.user_id,
            action="ban_user",
            resource_type="user",
            resource_id=user_id,
            status="success",
            ip_address=x_real_ip or "unknown",
            user_agent=user_agent or "unknown",
            detail=f"Banned user {user_id}",
        )
        return ok({"status": "ok"})
    raise HTTPException(status_code=500, detail="session unavailable")


@router.put("/admin/users/{user_id}/unban", response_model=BaseResponse[dict])
async def unban_user(
    user_id: str = Path(..., description="目标用户 ID"),
    authorization: str | None = Header(default=None),
    x_real_ip: str | None = Header(default=None),
    user_agent: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """管理员解封用户"""
    user = await _require_user(authorization)
    if not await is_admin(user.tenant_id, user.user_id):
        raise HTTPException(status_code=403, detail="forbidden")

    async for session in get_session():
        result = await session.execute(
            sql_update(User)
            .where(User.id == user_id)
            .where(User.is_deleted.is_(False))
            .values(is_banned=False)
        )
        await session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="user not found")

        await write_audit_log(
            tenant_id=user.tenant_id,
            actor_id=user.user_id,
            action="unban_user",
            resource_type="user",
            resource_id=user_id,
            status="success",
            ip_address=x_real_ip or "unknown",
            user_agent=user_agent or "unknown",
            detail=f"Unbanned user {user_id}",
        )
        return ok({"status": "ok"})
    raise HTTPException(status_code=500, detail="session unavailable")
