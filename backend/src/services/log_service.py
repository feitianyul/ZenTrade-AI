from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.audit_log import AuditLog


async def get_audit_logs(
    session: AsyncSession,
    tenant_id: str,
    actor_id: Optional[str] = None,
    action: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> List[AuditLog]:
    query = select(AuditLog).where(AuditLog.tenant_id == tenant_id)
    if actor_id:
        query = query.where(AuditLog.actor_id == actor_id)
    if action:
        query = query.where(AuditLog.action == action)
    
    query = query.order_by(AuditLog.created_at.desc()).limit(limit).offset(offset)
    result = await session.execute(query)
    return result.scalars().all()
