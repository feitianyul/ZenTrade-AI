from src.core.db import get_session
from src.models.audit_log import AuditLog


async def write_audit_log(
    tenant_id: str,
    actor_id: str,
    action: str,
    resource_type: str,
    resource_id: str,
    status: str,
    ip_address: str,
    user_agent: str,
    detail: str,
) -> None:
    async for session in get_session():
        log = AuditLog(
            tenant_id=tenant_id,
            actor_id=actor_id,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            status=status,
            ip_address=ip_address,
            user_agent=user_agent,
            detail=detail,
        )
        session.add(log)
        await session.commit()
        return
