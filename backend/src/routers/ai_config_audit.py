"""T229 - AI 配置审计路由"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Query

from src.schemas.response import BaseResponse, ok
from src.services.ai_config.audit_service import check_config_permission, get_audit_trail, log_config_change
from src.services.auth_service import verify_token

router = APIRouter(tags=["AIConfigAudit"])


async def _require_user(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.replace("Bearer ", "")
    return await verify_token(token)


@router.get("/ai-config/audit", response_model=BaseResponse[List[Dict[str, Any]]])
async def list_audit_trail(
    config_type: Optional[str] = None,
    limit: int = 50,
    authorization: str | None = Header(default=None),
) -> BaseResponse[List[Dict[str, Any]]]:
    user = await _require_user(authorization)
    records = await get_audit_trail(user.tenant_id, config_type, limit)
    return ok(records)


@router.get("/ai-config/permission-check", response_model=BaseResponse[Dict[str, Any]])
async def permission_check(
    config_type: str = Query(...),
    action: str = Query(default="read"),
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_user(authorization)
    result = await check_config_permission(
        user.tenant_id, user.user_id, config_type, action
    )
    return ok(result)
