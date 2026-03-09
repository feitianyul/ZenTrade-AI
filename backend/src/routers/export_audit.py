import base64
from typing import Any, Optional

from fastapi import APIRouter, Header, HTTPException, Query

from src.core.errors import ValidationError
from src.schemas.response import BaseResponse, ok
from src.schemas.user import UserOut
from src.services.auth_service import get_user_from_token
from src.services.export_audit_service import export_audit_logs

router = APIRouter()


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.get("/export/audit", response_model=BaseResponse[dict[str, Any]])
async def export_audit_endpoint(
    actor_id: Optional[str] = None,
    action: Optional[str] = None,
    limit: int = Query(1000, ge=1, le=5000),
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict[str, Any]]:
    user = await _require_user(authorization)
    try:
        result = await export_audit_logs(user.tenant_id, user.user_id, actor_id, action, limit)
    except ValidationError as exc:
        raise exc.as_http_exception() from exc
    payload = base64.b64encode(result["payload"]).decode("ascii")
    return ok(
        {
            "filename": result["filename"],
            "content_type": result["content_type"],
            "data_base64": payload,
        }
    )
