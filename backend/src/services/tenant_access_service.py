from typing import Any


async def grant_tenant_access(
    tenant_id: str, target_tenant_id: str, actor_id: str
) -> dict[str, Any]:
    return {"tenant_id": tenant_id, "target_tenant_id": target_tenant_id, "actor_id": actor_id}
