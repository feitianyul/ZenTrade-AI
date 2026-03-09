from typing import Any, Dict

from sqlalchemy.ext.asyncio import AsyncSession


class PricingPolicyService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_user_entitlements(
        self,
        tenant_id: str,
        user_id: int,
        user_level: str | None = None,
    ) -> Dict[str, Any]:
        """根据用户等级返回权益配额。

        等级映射:
        - basic (初级散户) → Basic 套餐
        - intermediate (进阶散户) → Pro 套餐
        - advanced (资深散户) → VIP 套餐
        """
        # 如果未传入 user_level，从数据库查询
        if not user_level:
            from sqlalchemy import select
            from src.models.user import User
            result = await self.db.execute(
                select(User.level).where(User.id == user_id)
            )
            row = result.scalar_one_or_none()
            user_level = row if row else "basic"

        # 用户等级 → 套餐映射
        level_to_tier = {
            "basic": "basic",
            "intermediate": "pro",
            "advanced": "vip",
        }
        tier = level_to_tier.get(user_level, "basic")

        policies = {
            "basic": {
                "tier": "Basic",
                "max_strategies": 5,
                "ai_calls_daily": 10,
                "real_time_data": False,
                "backtest_limit": "1y",
            },
            "pro": {
                "tier": "Pro",
                "max_strategies": 20,
                "ai_calls_daily": 100,
                "real_time_data": True,
                "backtest_limit": "5y",
            },
            "vip": {
                "tier": "VIP",
                "max_strategies": 100,
                "ai_calls_daily": 1000,
                "real_time_data": True,
                "backtest_limit": "unlimited",
            },
        }

        return policies.get(tier, policies["basic"])

    async def check_feature_access(
        self,
        tenant_id: str,
        user_id: int,
        feature_key: str,
    ) -> bool:
        entitlements = await self.get_user_entitlements(tenant_id, user_id)
        if feature_key == "real_time_data":
            return entitlements.get("real_time_data", False)
        # Add more checks as needed
        return True
