from sqlalchemy.ext.asyncio import AsyncSession

from src.models.self_optimize_log import SelfOptimizeLog


class SelfOptimizeLogService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def log_event(
        self,
        tenant_id: str,
        trigger: str,
        action: str,
        details: dict,
        status: str = "success",
    ):
        log = SelfOptimizeLog(
            tenant_id=tenant_id,
            trigger_type=trigger,
            action_taken=action,
            details=details,
            status=status,
        )
        self.db.add(log)
        await self.db.commit()
