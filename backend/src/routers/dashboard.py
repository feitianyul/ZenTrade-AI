from fastapi import APIRouter, Header, HTTPException

from src.schemas.response import BaseResponse, ok
from src.services.auth_service import get_user_from_token

router = APIRouter()


@router.get("/dashboard/overview", response_model=BaseResponse[dict[str, str]])
async def overview(
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict[str, str]]:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return ok(
        {
            "user_id": user.user_id,
            "tenant_id": user.tenant_id,
            "risk_level": user.risk_level,
            "level": user.level,
        }
    )
