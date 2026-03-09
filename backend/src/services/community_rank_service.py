"""社区排行榜服务 — 查 strategies + users 表生成真实排行"""

import logging
from typing import Iterable

from sqlalchemy import func, select

from src.core.db import get_session, with_tenant
from src.models.strategy import Strategy
from src.models.user import User
from src.schemas.community import CommunityRankItem

logger = logging.getLogger(__name__)


async def build_rankings(tenant_id: str = "default", limit: int = 20) -> Iterable[CommunityRankItem]:
    """查询策略表，按回测收益率排序，关联用户表取信息。"""
    try:
        async for session in get_session():
            # 查询有回测成绩的策略，按 last_backtest_metrics 中收益排序
            query = (
                select(Strategy, User)
                .join(User, Strategy.owner_id == User.id, isouter=True)
                .where(Strategy.is_deleted.is_(False))
                .where(Strategy.last_backtest_metrics.isnot(None))
                .order_by(Strategy.last_backtest_grade.asc())  # A > B > C
                .limit(limit)
            )
            result = await session.execute(query)
            rows = result.all()

            if not rows:
                return []

            rankings = []
            for idx, (strategy, user) in enumerate(rows):
                metrics = strategy.last_backtest_metrics or {}
                annual_return = metrics.get("annual_return", 0)
                total_return = metrics.get("total_return", 0)
                score = round(total_return * 100, 1) if total_return else 0

                # 根据回测评级生成标签
                grade = strategy.last_backtest_grade or "C"
                label_map = {"A": "策略达人", "B": "稳健选手", "C": "潜力新秀"}
                label = label_map.get(grade, "参与者")

                user_id = user.id if user else strategy.owner_id
                rankings.append(CommunityRankItem(
                    user_id=str(user_id),
                    score=score,
                    label=label,
                ))
            return rankings
    except Exception as exc:
        logger.warning("build_rankings failed: %s", exc)
        return []
