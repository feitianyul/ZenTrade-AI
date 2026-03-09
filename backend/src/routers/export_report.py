import base64
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query

from src.core.errors import NotFoundError, ValidationError
from src.schemas.response import BaseResponse, ok
from src.schemas.user import UserOut
from src.services.auth_service import get_user_from_token
from src.services.export_report_service import export_replay_report

router = APIRouter()


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.get("/export/report/{report_id}", response_model=BaseResponse[dict[str, Any]])
async def export_report_endpoint(
    report_id: str,
    export_type: str = Query("pdf", pattern="^(pdf|xlsx)$"),
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict[str, Any]]:
    user = await _require_user(authorization)
    try:
        result = await export_replay_report(user.tenant_id, report_id, export_type)
    except NotFoundError as exc:
        raise exc.as_http_exception() from exc
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
