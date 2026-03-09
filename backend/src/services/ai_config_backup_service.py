from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.audit_log import AuditLog
from src.services.ai_config_service import AIConfigService


class AIConfigBackupService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.config_service = AIConfigService(db)

    async def create_backup(self, tenant_id: str) -> dict:
        configs = await self.config_service.list_configs(tenant_id)
        backup_data = {
            "tenant_id": tenant_id,
            "timestamp": datetime.utcnow().isoformat(),
            "configs": [
                {
                    "key": c.key,
                    "value": c.value,
                    "version": c.version,
                    "description": c.description
                }
                for c in configs
            ]
        }
        # Log backup action
        audit = AuditLog(
            tenant_id=tenant_id,
            actor_id="system",
            action="ai_config_backup",
            resource_type="ai_config",
            resource_id="all",
            detail=f"config_count={len(configs)}",
            ip_address="",
            user_agent="",
        )
        self.db.add(audit)
        await self.db.commit()
        
        return backup_data

    async def restore_backup(self, tenant_id: str, backup_data: dict, operator_id: str):
        # Validate tenant
        if backup_data.get("tenant_id") != tenant_id:
            raise ValueError("Tenant mismatch in backup")
        
        configs = backup_data.get("configs", [])
        for item in configs:
            await self.config_service.set_config(
                tenant_id,
                item["key"],
                item["value"],
                f"Restored from backup {backup_data['timestamp']}"
            )
        
        audit = AuditLog(
            tenant_id=tenant_id,
            actor_id=operator_id,
            action="ai_config_restore",
            resource_type="ai_config",
            resource_id="all",
            detail=f"config_count={len(configs)}",
            ip_address="",
            user_agent="",
        )
        self.db.add(audit)
        await self.db.commit()
