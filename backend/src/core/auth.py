import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import jwt
from fastapi import Header, HTTPException
from src.schemas.user import UserOut

_JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
_JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
_JWT_EXP_HOURS = int(os.getenv("JWT_EXP_HOURS", "24"))
_DEFAULT_MFA_CODE = os.getenv("MFA_CODE", "000000")

def create_access_token(payload: Dict[str, Any], expires_hours: Optional[int] = None) -> str:
    exp_hours = expires_hours if expires_hours is not None else _JWT_EXP_HOURS
    token_payload = payload.copy()
    token_payload["exp"] = datetime.utcnow() + timedelta(hours=exp_hours)
    return jwt.encode(token_payload, _JWT_SECRET, algorithm=_JWT_ALGORITHM)

def decode_token(token: str) -> Dict[str, Any]:
    return jwt.decode(token, _JWT_SECRET, algorithms=[_JWT_ALGORITHM])

def verify_mfa_code(code: str, expected: Optional[str] = None) -> bool:
    return code == (expected or _DEFAULT_MFA_CODE)


async def get_current_user(
    authorization: str | None = Header(default=None),
) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    from src.services.auth_service import get_user_from_token

    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user
