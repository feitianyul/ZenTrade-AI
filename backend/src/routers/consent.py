from typing import Any, Dict, List

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from src.schemas.response import BaseResponse, ok
from src.services.auth_service import verify_token
from src.services.consent_service import get_user_consents, grant_consent, revoke_consent

router = APIRouter(prefix="/consents", tags=["Consent"])


async def _require_user(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.replace("Bearer ", "")
    return await verify_token(token)


class GrantConsentRequest(BaseModel):
    scope: str
    consent_id: str


@router.get("/", response_model=BaseResponse[List[Dict[str, Any]]])
async def list_consents(authorization: str | None = Header(default=None)):
    user = await _require_user(authorization)
    result = await get_user_consents(user.user_id)
    # 将 ConsentRecord 对象转为 dict 以避免序列化问题
    return ok([c.model_dump() if hasattr(c, "model_dump") else c for c in result])


@router.post("/")
async def grant_user_consent(
    req: GrantConsentRequest,
    authorization: str | None = Header(default=None),
):
    user = await _require_user(authorization)
    result = await grant_consent(user.user_id, req.scope, req.consent_id)
    return ok(result.model_dump() if hasattr(result, "model_dump") else result)


@router.delete("/{consent_id}", response_model=BaseResponse[Dict[str, Any]])
async def revoke_user_consent(
    consent_id: str,
    authorization: str | None = Header(default=None),
):
    user = await _require_user(authorization)
    success = await revoke_consent(user.user_id, consent_id)
    if not success:
        raise HTTPException(status_code=404, detail="Consent not found")
    return ok({"status": "revoked"})
