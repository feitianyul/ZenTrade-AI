from typing import List, Optional

from fastapi import APIRouter, Depends, Header, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_db
from src.core.time_util import utc_to_beijing_str
from src.schemas.audit_log import AuditLogOut
from src.schemas.response import BaseResponse, ok
from src.services.auth_service import verify_token
from src.services.log_service import get_audit_logs

router = APIRouter(tags=["Logs"])

async def _require_user(authorization: str | None = Header(default=None)):
    if not authorization:
        raise ValueError("unauthorized")
    token = authorization.replace("Bearer ", "")
    return await verify_token(token)

@router.get("/logs/audit", response_model=BaseResponse[List[AuditLogOut]])
async def list_audit_logs(
    actor_id: Optional[str] = None,
    action: Optional[str] = None,
    limit: int = Query(50, le=100),
    offset: int = 0,
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_db),
) -> BaseResponse[List[AuditLogOut]]:
    user = await _require_user(authorization)
    logs = await get_audit_logs(session, user.tenant_id, actor_id, action, limit, offset)
    # 统一返回北京时间
    data = [
        AuditLogOut(
            id=str(l.id),
            tenant_id=getattr(l, "tenant_id", "") or "",
            actor_id=l.actor_id or "",
            action=l.action or "",
            resource_type=l.resource_type or "",
            resource_id=l.resource_id or "",
            status=l.status or "",
            ip_address=l.ip_address or "",
            user_agent=l.user_agent or "",
            detail=l.detail or "",
            created_at=utc_to_beijing_str(l.created_at) or "",
        )
        for l in logs
    ]
    return ok(data)
