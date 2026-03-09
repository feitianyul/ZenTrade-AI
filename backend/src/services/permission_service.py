from typing import Iterable

from sqlalchemy import delete, select

from src.core.db import get_session, with_tenant
from src.models.role import Role
from src.models.user_role import UserRole


async def set_user_role(tenant_id: str, user_id: str, role_name: str) -> None:
    """设置用户角色（先删后插，替换该租户下该用户的全部角色）"""
    async for session in get_session():
        role_query = with_tenant(select(Role), Role, tenant_id).where(Role.name == role_name)
        role_result = await session.execute(role_query)
        role = role_result.scalar_one_or_none()
        if not role:
            raise ValueError(f"role not found: {role_name}")

        await session.execute(
            delete(UserRole).where(
                UserRole.user_id == user_id,
                UserRole.tenant_id == tenant_id,
            )
        )
        session.add(UserRole(tenant_id=tenant_id, user_id=user_id, role_id=role.id))
        await session.commit()
        return
    raise RuntimeError("session unavailable")


async def list_roles(tenant_id: str) -> Iterable[Role]:
    async for session in get_session():
        query = with_tenant(select(Role), Role, tenant_id)
        result = await session.execute(query)
        return result.scalars().all()
    return []


async def assign_role(tenant_id: str, user_id: str, role_id: str) -> None:
    async for session in get_session():
        record = UserRole(tenant_id=tenant_id, user_id=user_id, role_id=role_id)
        session.add(record)
        await session.commit()
        return


async def is_admin(tenant_id: str, user_id: str) -> bool:
    async for session in get_session():
        query = with_tenant(select(UserRole), UserRole, tenant_id).where(
            UserRole.user_id == user_id
        )
        result = await session.execute(query)
        user_roles = result.scalars().all()
        if not user_roles:
            return False
        role_ids = {role.role_id for role in user_roles}
        role_query = with_tenant(select(Role), Role, tenant_id).where(
            Role.id.in_(role_ids)
        )
        role_result = await session.execute(role_query)
        roles = role_result.scalars().all()
        return any(role.name == "admin" for role in roles)
    return False


async def has_permission(tenant_id: str, user_id: str, required_perm: str) -> bool:
    async for session in get_session():
        query = with_tenant(select(UserRole), UserRole, tenant_id).where(
            UserRole.user_id == user_id
        )
        result = await session.execute(query)
        user_roles = result.scalars().all()
        if not user_roles:
            return False
            
        role_ids = {role.role_id for role in user_roles}
        role_query = with_tenant(select(Role), Role, tenant_id).where(
            Role.id.in_(role_ids)
        )
        role_result = await session.execute(role_query)
        roles = role_result.scalars().all()
        
        # Admin has all permissions
        if any(role.name == "admin" for role in roles):
            return True
            
        # Check specific permissions
        for role in roles:
            perms = role.permissions or {}
            if perms.get(required_perm, False):
                return True
                
    return False
