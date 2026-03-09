from typing import Any

from fastapi import APIRouter, File, Header, HTTPException, UploadFile

from src.core.errors import ValidationError
from src.schemas.response import BaseResponse, ok
from src.schemas.user import UserOut
from src.services.auth_service import get_user_from_token
from src.services.import_strategy_service import import_strategies

router = APIRouter()


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.post("/import/strategy", response_model=BaseResponse[dict[str, Any]])
async def import_strategy_endpoint(
    file: UploadFile = File(...),
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict[str, Any]]:
    user = await _require_user(authorization)
    content = await file.read()
    try:
        result = await import_strategies(user.tenant_id, user.user_id, file.filename, content)
    except ValidationError as exc:
        raise exc.as_http_exception() from exc
    return ok(result)
