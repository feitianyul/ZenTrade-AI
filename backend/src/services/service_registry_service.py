from typing import Any, Iterable, Optional

from sqlalchemy import select, update

from src.core.db import get_session, with_tenant
from src.models.service_registry import ServiceRegistry


async def register_service(
    tenant_id: str,
    service_name: str,
    endpoint: str,
    protocol: str,
    version: str = "v1",
    metadata: Optional[dict[str, Any]] = None,
) -> ServiceRegistry:
    async for session in get_session():
        record = ServiceRegistry(
            tenant_id=tenant_id,
            service_name=service_name,
            endpoint=endpoint,
            protocol=protocol,
            version=version,
            meta=metadata or {},
        )
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return record
    raise RuntimeError("session unavailable")

async def list_services(tenant_id: str) -> Iterable[ServiceRegistry]:
    async for session in get_session():
        query = with_tenant(select(ServiceRegistry), ServiceRegistry, tenant_id)
        result = await session.execute(query)
        return result.scalars().all()
    return []

async def update_health(
    tenant_id: str,
    service_id: str,
    health_status: str,
) -> None:
    async for session in get_session():
        stmt = update(ServiceRegistry).where(
            ServiceRegistry.id == service_id,
            ServiceRegistry.tenant_id == tenant_id,
        ).values(health_status=health_status)
        await session.execute(stmt)
        await session.commit()
        return
