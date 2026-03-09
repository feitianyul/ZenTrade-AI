import os

from fastapi import APIRouter, Header, HTTPException

from src.core.errors import ValidationError
from src.schemas.auth import ChangePasswordRequest, LoginRequest, RegisterRequest, TokenData
from src.schemas.response import BaseResponse, ok
from src.schemas.user import UserOut
from src.services.auth_service import (
    authenticate_user,
    change_password,
    create_token_for_user,
    get_user_from_token,
    register_user,
)
from src.services.validation_service import validate_login_payload

router = APIRouter()


def _allow_register() -> bool:
    """是否开放注册，由环境变量 ALLOW_REGISTER 控制（默认 true）。"""
    return os.getenv("ALLOW_REGISTER", "true").strip().lower() in ("1", "true", "yes")


@router.get("/auth/config", response_model=BaseResponse[dict])
async def auth_config() -> BaseResponse[dict]:
    """公开接口：前端用于判断是否展示注册入口。无需认证。"""
    return ok({"allow_register": _allow_register()})


@router.post("/auth/register", response_model=BaseResponse[UserOut])
async def register(
    payload: RegisterRequest,
    x_tenant_id: str | None = Header(default=None),
) -> BaseResponse[UserOut]:
    if not _allow_register():
        raise HTTPException(status_code=403, detail="注册已关闭")
    tenant_id = x_tenant_id or "public"
    errors = validate_login_payload(payload.phone, payload.password)
    if errors:
        raise ValidationError("; ".join(errors)).as_http_exception()
    try:
        user = await register_user(payload.phone, payload.password, tenant_id)
    except ValueError as exc:
        raise ValidationError(str(exc)).as_http_exception() from exc
    return ok(user)


@router.post("/auth/login", response_model=BaseResponse[TokenData])
async def login(
    payload: LoginRequest,
    x_tenant_id: str | None = Header(default=None),
) -> BaseResponse[TokenData]:
    tenant_id = x_tenant_id or "public"
    errors = validate_login_payload(payload.phone, payload.password)
    if errors:
        raise ValidationError("; ".join(errors)).as_http_exception()
    user = await authenticate_user(payload.phone, payload.password, tenant_id)
    if not user:
        raise ValidationError("invalid credentials").as_http_exception()
    token = await create_token_for_user(user)
    return ok(TokenData(token=token))


@router.put("/auth/change-password", response_model=BaseResponse[dict])
async def change_password_endpoint(
    payload: ChangePasswordRequest,
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """用户修改自己的密码"""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    errors = validate_login_payload("13800000000", payload.new_password)
    if errors:
        raise ValidationError("; ".join(errors)).as_http_exception()
    ok_changed = await change_password(
        user.user_id, user.tenant_id, payload.current_password, payload.new_password
    )
    if not ok_changed:
        raise ValidationError("current password incorrect").as_http_exception()
    return ok({"status": "ok"})


@router.get("/auth/profile", response_model=BaseResponse[UserOut])
async def profile(
    authorization: str | None = Header(default=None),
) -> BaseResponse[UserOut]:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return ok(user)
