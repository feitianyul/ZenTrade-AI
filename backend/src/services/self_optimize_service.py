from sqlalchemy.ext.asyncio import AsyncSession

from src.services.ai_config_service import AIConfigService
from src.services.self_optimize_log_service import SelfOptimizeLogService


class SelfOptimizeService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.log_service = SelfOptimizeLogService(db)
        self.config_service = AIConfigService(db)

    async def check_and_trigger_optimization(self, tenant_id: str, metrics: dict):
        # Example rule: if negative feedback > 5 in last hour -> Adjust prompt temperature
        if metrics.get("negative_feedback_count", 0) > 5:
            await self._adjust_temperature(tenant_id, -0.1)

    async def _adjust_temperature(self, tenant_id: str, delta: float):
        # Get current config
        config = await self.config_service.get_config(tenant_id, "ai_generation_params")
        if config:
            params = config.value
            current_temp = params.get("temperature", 0.7)
            new_temp = max(0.1, min(1.0, current_temp + delta))
            
            params["temperature"] = new_temp
            await self.config_service.set_config(
                tenant_id, 
                "ai_generation_params", 
                params, 
                description=f"Self-optimize: adjusted temp by {delta}"
            )
            
            await self.log_service.log_event(
                tenant_id,
                "negative_feedback_threshold",
                "adjust_temperature",
                {"old": current_temp, "new": new_temp}
            )

    async def rollback_optimization(self, tenant_id: str, config_key: str):
        # Logic to revert to previous version would go here
        pass
