import hashlib
import os
from typing import Optional

from sqlalchemy import delete, select

from src.core.auth import create_access_token, decode_token
from src.core.crypto import decrypt_text, encrypt_text
from src.core.db import get_session, with_tenant
from src.models.user import User
from src.schemas.user import UserOut

_SALT = os.getenv("AUTH_SALT", "dev-salt")
_EXCEPTION_PHONES = {
    item.strip()
    for item in os.getenv("AUTH_EXCEPTION_PHONES", "").split(",")
    if item.strip()
}
_ALLOW_MULTI_LOGIN = os.getenv("AUTH_ALLOW_MULTI_LOGIN", "true").lower() == "true"


def _hash_password(password: str) -> str:
    payload = f"{_SALT}:{password}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _encrypt_phone(phone: str) -> str:
    return encrypt_text(phone)


def _encrypt_password_hash(password_hash: str) -> str:
    return encrypt_text(password_hash)


def _decrypt_phone(encrypted_phone: str) -> str:
    return decrypt_text(encrypted_phone)


async def register_user(
    phone: str | None,
    password: str,
    tenant_id: str,
    email: str | None = None,
    nickname: str | None = None,
) -> UserOut:
    if not phone and not email:
        raise ValueError("phone or email required")
        
    encrypted_phone = _encrypt_phone(phone) if phone else None
    encrypted_email = encrypt_text(email) if email else None
    
    async for session in get_session():
        # Check phone if provided
        if encrypted_phone:
            query = with_tenant(select(User), User, tenant_id).where(User.phone == encrypted_phone)
            existing = await session.execute(query)
            if existing.scalar_one_or_none():
                raise ValueError("phone already registered")
                
        # Check email if provided
        if encrypted_email:
            query = with_tenant(select(User), User, tenant_id).where(User.email == encrypted_email)
            existing = await session.execute(query)
            if existing.scalar_one_or_none():
                raise ValueError("email already registered")

        record = User(
            tenant_id=tenant_id,
            phone=encrypted_phone or "",
            email=encrypted_email,
            nickname=nickname,
            password_hash=_encrypt_password_hash(_hash_password(password)),
            level="basic",
            risk_level="unassessed",
            is_deleted=False,
        )
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return _to_user_out(record)
    raise RuntimeError("session unavailable")


async def authenticate_user(
    phone: str | None, 
    password: str, 
    tenant_id: str,
    email: str | None = None
) -> Optional[UserOut]:
    encrypted_phone = _encrypt_phone(phone) if phone else None
    
    async for session in get_session():
        query = with_tenant(select(User), User, tenant_id)
        
        if encrypted_phone:
            query = query.where(User.phone == encrypted_phone)
        elif email: 
             query = query.where(User.email == encrypt_text(email))
        else:
            return None
            
        query = query.where(User.is_deleted.is_(False))
        
        result = await session.execute(query)
        record = result.scalar_one_or_none()
        if not record:
            return None
            
        if phone and phone in _EXCEPTION_PHONES:
            if getattr(record, "is_banned", False):
                return None
            return _to_user_out(record)

        expected_hash = _encrypt_password_hash(_hash_password(password))
        if record.password_hash != expected_hash:
            return None
        if getattr(record, "is_banned", False):
            return None
        return _to_user_out(record)
    return None


async def create_token_for_user(user: UserOut) -> str:
    return create_access_token({"user_id": user.user_id, "tenant_id": user.tenant_id})


def allow_multi_login() -> bool:
    return _ALLOW_MULTI_LOGIN


def is_exception_login(phone: str) -> bool:
    return phone in _EXCEPTION_PHONES


async def get_user_from_token(token: str) -> Optional[UserOut]:
    try:
        payload = decode_token(token)
    except Exception:
        return None
    user_id = payload.get("user_id")
    tenant_id = payload.get("tenant_id")
    if not user_id or not tenant_id:
        return None
    async for session in get_session():
        query = with_tenant(select(User), User, str(tenant_id)).where(User.id == str(user_id))
        result = await session.execute(query)
        record = result.scalar_one_or_none()
        if not record:
            return None
        return _to_user_out(record)
    return None


async def verify_token(token: str) -> UserOut:
    """验证 token 并返回用户信息（路由守卫使用）"""
    user = await get_user_from_token(token)
    if not user:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="invalid or expired token")
    return user


async def reset_auth_state() -> None:
    async for session in get_session():
        await session.execute(delete(User))
        await session.commit()
        return


async def update_password_by_phone(
    phone: str, new_password: str, tenant_id: str = "public"
) -> bool:
    """用当前 AUTH_SALT / SM4_KEY 重算并更新该手机号用户的密码。密钥或盐变更后可用此恢复登录。"""
    encrypted_phone = _encrypt_phone(phone) if phone else None
    if not encrypted_phone:
        return False
    async for session in get_session():
        query = (
            with_tenant(select(User), User, tenant_id)
            .where(User.phone == encrypted_phone)
            .where(User.is_deleted.is_(False))
        )
        result = await session.execute(query)
        record = result.scalar_one_or_none()
        if not record:
            return False
        record.password_hash = _encrypt_password_hash(_hash_password(new_password))
        await session.commit()
        return True
    return False


async def change_password(
    user_id: str,
    tenant_id: str,
    current_password: str,
    new_password: str,
) -> bool:
    """用户自己修改密码，需验证当前密码"""
    expected_hash = _encrypt_password_hash(_hash_password(current_password))
    async for session in get_session():
        query = (
            with_tenant(select(User), User, tenant_id)
            .where(User.id == user_id)
            .where(User.is_deleted.is_(False))
        )
        result = await session.execute(query)
        record = result.scalar_one_or_none()
        if not record or record.password_hash != expected_hash:
            return False
        record.password_hash = _encrypt_password_hash(_hash_password(new_password))
        await session.commit()
        return True
    return False


async def update_password_by_user_id(
    user_id: str,
    new_password: str,
    tenant_id: str = "public",
    phone: str | None = None,
) -> bool:
    """按 user_id 用当前密钥重算并更新密码；若传入 phone，同时用当前 SM4_KEY 更新该用户的 phone，以便登录能按手机号查到。"""
    async for session in get_session():
        query = with_tenant(select(User), User, tenant_id).where(User.id == user_id)
        result = await session.execute(query)
        record = result.scalar_one_or_none()
        if not record:
            return False
        record.password_hash = _encrypt_password_hash(_hash_password(new_password))
        if phone:
            record.phone = _encrypt_phone(phone)
        await session.commit()
        return True
    return False


def _to_user_out(record: User) -> UserOut:
    return UserOut(
        user_id=record.id,
        tenant_id=record.tenant_id,
        phone=_decrypt_phone(record.phone),
        level=record.level,
        risk_level=record.risk_level,
    )
