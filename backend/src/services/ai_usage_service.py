"""AI 调用次数统计服务。使用 Redis 存储每日已用次数。"""

from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import select

from src.core.streams import get_redis_client

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

_KEY_PREFIX = "ai_calls"
_TTL_SECONDS = 86400 * 2  # 2 天
_DEFAULT_LIMITS = {"beginner": 10, "advanced": 30, "expert": 100}


def _key(user_id: str, d: str) -> str:
    return f"{_KEY_PREFIX}:{user_id}:{d}"


async def incr_ai_call(user_id: str, d: str) -> int:
    """对指定用户当日调用次数 +1，设置 TTL，返回自增后的值。"""
    try:
        client = await get_redis_client()
        k = _key(user_id, d)
        val = await client.incr(k)
        await client.expire(k, _TTL_SECONDS)
        return val
    except Exception:
        return 0


async def get_ai_calls_used(user_id: str, d: str) -> int:
    """获取指定用户指定日期的已用次数。"""
    try:
        client = await get_redis_client()
        k = _key(user_id, d)
        val = await client.get(k)
        return int(val) if val else 0
    except Exception:
        return 0


async def get_ai_calls_used_batch(user_ids: list[str], d: str) -> dict[str, int]:
    """批量查询各用户指定日期的已用次数。"""
    result: dict[str, int] = {}
    if not user_ids:
        return result
    try:
        client = await get_redis_client()
        keys = [_key(uid, d) for uid in user_ids]
        vals = await client.mget(keys)
        for uid, v in zip(user_ids, vals):
            result[uid] = int(v) if v else 0
    except Exception:
        for uid in user_ids:
            result[uid] = 0
    return result


def resolve_ai_limit(override: int | None, role: str, role_limits: dict) -> int:
    """用户有效限额：优先用户级覆盖，否则按角色。"""
    if override is not None and override >= 0:
        return override
    return role_limits.get(role, _DEFAULT_LIMITS.get(role, 10))


async def get_user_ai_limit_and_used(
    db: "AsyncSession",
    user_id: str,
    tenant_id: str,
    today: str,
) -> tuple[int, int]:
    """获取用户当日有效限额与已用次数。用于 ai_chat 调用前校验。"""
    from src.models.role import Role
    from src.models.user import User
    from src.models.user_role import UserRole
    from src.services.ai_config_service import AIConfigService

    # 查 User 的 override
    u = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    override = getattr(u, "ai_calls_limit_override", None) if u else None

    # 查角色
    role_q = (
        select(Role.name)
        .join(UserRole, Role.id == UserRole.role_id)
        .where(UserRole.user_id == user_id)
        .where(UserRole.tenant_id == tenant_id)
    )
    role_row = (await db.execute(role_q)).first()
    role = role_row[0] if role_row else "beginner"

    # 查 ai_usage_limits
    ai_svc = AIConfigService(db)
    cfg = await ai_svc.get_config(tenant_id, "ai_usage_limits")
    limits = cfg.value if cfg and isinstance(cfg.value, dict) else _DEFAULT_LIMITS
    for k in _DEFAULT_LIMITS:
        limits.setdefault(k, _DEFAULT_LIMITS[k])

    limit = resolve_ai_limit(override, role, limits)
    used = await get_ai_calls_used(user_id, today)
    return (limit, used)
