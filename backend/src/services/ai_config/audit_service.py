"""AI 配置审计与权限控制 — 写入/查询 audit_logs 表 + 查 roles 表验证权限"""

import json
import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select

from src.core.db import get_session
from src.core.time_util import utc_to_beijing_str
from src.models.audit_log import AuditLog
from src.models.role import Role
from src.models.user_role import UserRole

logger = logging.getLogger(__name__)


async def log_config_change(
    tenant_id: str,
    actor_id: str,
    config_type: str,
    action: str,
    old_value: Optional[dict] = None,
    new_value: Optional[dict] = None,
) -> dict[str, Any]:
    """写入 audit_logs 表记录配置变更审计"""
    try:
        async for session in get_session():
            detail = json.dumps(
                {"old_value": old_value, "new_value": new_value},
                ensure_ascii=False,
                default=str,
            )
            record = AuditLog(
                actor_id=actor_id,
                action=action,
                resource_type=config_type,
                resource_id=tenant_id,
                status="success",
                ip_address="",
                user_agent="",
                detail=detail,
            )
            session.add(record)
            await session.commit()
            await session.refresh(record)
            return {
                "id": str(record.id),
                "tenant_id": tenant_id,
                "actor_id": actor_id,
                "config_type": config_type,
                "action": action,
                "old_value": old_value,
                "new_value": new_value,
                "timestamp": utc_to_beijing_str(record.created_at) or utc_to_beijing_str(datetime.utcnow()),
            }
    except Exception as exc:
        logger.warning("log_config_change failed: %s", exc)
        return {
            "id": "error",
            "tenant_id": tenant_id,
            "actor_id": actor_id,
            "config_type": config_type,
            "action": action,
            "timestamp": utc_to_beijing_str(datetime.utcnow()),
        }


async def get_audit_trail(
    tenant_id: str,
    config_type: Optional[str] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """查询 audit_logs 表获取审计记录"""
    try:
        async for session in get_session():
            query = (
                select(AuditLog)
                .where(AuditLog.resource_id == tenant_id)
                .order_by(AuditLog.created_at.desc())
            )
            if config_type:
                query = query.where(AuditLog.resource_type == config_type)
            query = query.limit(limit)

            result = await session.execute(query)
            logs = result.scalars().all()

            return [
                {
                    "id": str(log.id),
                    "tenant_id": tenant_id,
                    "actor_id": log.actor_id,
                    "config_type": log.resource_type,
                    "action": log.action,
                    "detail": log.detail,
                    "timestamp": utc_to_beijing_str(log.created_at) or "",
                }
                for log in logs
            ]
    except Exception as exc:
        logger.warning("get_audit_trail failed: %s", exc)
        return []


async def check_config_permission(
    tenant_id: str, user_id: str, config_type: str, action: str
) -> dict[str, Any]:
    """查询 user_roles + roles 表验证用户是否有配置操作权限"""
    try:
        async for session in get_session():
            # 查用户关联的角色
            query = (
                select(Role)
                .join(UserRole, Role.id == UserRole.role_id)
                .where(UserRole.user_id == user_id)
            )
            result = await session.execute(query)
            roles = result.scalars().all()

            # 检查是否有 admin 角色或对应权限
            allowed = False
            for role in roles:
                if role.name in ("admin", "super_admin"):
                    allowed = True
                    break
                perms = role.permissions or {}
                # 检查 permissions JSON 是否包含该 config_type 操作权限
                if config_type in perms:
                    if action in perms[config_type] or "*" in perms[config_type]:
                        allowed = True
                        break
                # 通配符权限
                if "*" in perms:
                    allowed = True
                    break

            return {
                "allowed": allowed,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "config_type": config_type,
                "action": action,
            }
    except Exception as exc:
        logger.warning("check_config_permission failed: %s", exc)
        # 权限检查失败时默认拒绝
        return {
            "allowed": False,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "config_type": config_type,
            "action": action,
        }
