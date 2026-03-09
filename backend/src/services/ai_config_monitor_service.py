"""AI 配置漂移检测服务

TODO: 实现真实配置比对。
      当前仅检查 system_status 配置是否存在。
      完整实现需要:
        1. 存储各节点配置快照
        2. 定期对比节点间配置差异
        3. 发现漂移时创建告警
"""

from sqlalchemy.ext.asyncio import AsyncSession

from src.services.ai_config_service import AIConfigService
from src.services.alert_service import AlertService


class AIConfigMonitorService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.config_service = AIConfigService(db)
        self.alert_service = AlertService(db)

    async def check_sync_status(self, tenant_id: str):
        """检查配置同步状态。TODO: 实现多节点配置对比"""
        config = await self.config_service.get_config(tenant_id, "system_status")
        if not config:
            await self.alert_service.create_alert(
                tenant_id,
                "warning",
                "AI Config Missing",
                "system_status config is missing"
            )
