"""Agent 根因分析与策略复用 — 查 agent_tasks 失败记录"""

import logging
from collections import Counter
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import func, select

from src.core.db import get_session
from src.models.agent_task import AgentTask
from src.models.strategy import Strategy

logger = logging.getLogger(__name__)


async def analyze_root_cause(
    tenant_id: str,
    failure_type: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """
    查询 agent_tasks 表中 status='failed' 的记录，
    统计失败模式进行根因分析。
    """
    try:
        async for session in get_session():
            # 查询近期失败的任务
            query = (
                select(AgentTask)
                .where(AgentTask.status == "failed")
                .order_by(AgentTask.completed_at.desc())
                .limit(100)
            )
            result = await session.execute(query)
            failed_tasks = result.scalars().all()

            if not failed_tasks:
                return {
                    "tenant_id": tenant_id,
                    "failure_type": failure_type,
                    "root_cause": "无失败记录，系统运行正常",
                    "confidence": 1.0,
                    "recommendations": [],
                    "analyzed_at": datetime.utcnow().isoformat(),
                }

            # 统计错误信息模式
            error_msgs = [t.error_msg or "unknown" for t in failed_tasks]
            patterns = Counter(error_msgs).most_common(3)

            # 如果 failure_type 匹配某些任务
            type_matched = [t for t in failed_tasks if t.task_type == failure_type]
            if type_matched:
                root_cause = f"最近 {len(type_matched)} 次 {failure_type} 任务失败，" \
                             f"最常见错误: {patterns[0][0] if patterns else '未知'}"
                confidence = min(0.6 + len(type_matched) * 0.02, 0.95)
            else:
                root_cause = f"未找到类型为 {failure_type} 的失败记录；" \
                             f"系统总失败 {len(failed_tasks)} 次，" \
                             f"主要错误: {patterns[0][0] if patterns else '未知'}"
                confidence = 0.5

            recommendations = [
                f"排查错误模式: {p[0]} (出现 {p[1]} 次)" for p in patterns[:3]
            ]
            recommendations.append("查看最近日志获取详细信息")

            return {
                "tenant_id": tenant_id,
                "failure_type": failure_type,
                "root_cause": root_cause,
                "confidence": round(confidence, 2),
                "recommendations": recommendations,
                "analyzed_at": datetime.utcnow().isoformat(),
            }
    except Exception as exc:
        logger.warning("analyze_root_cause failed: %s", exc)
        return {
            "tenant_id": tenant_id,
            "failure_type": failure_type,
            "root_cause": f"分析异常: {exc}",
            "confidence": 0.0,
            "recommendations": ["检查数据库连接", "联系管理员"],
            "analyzed_at": datetime.utcnow().isoformat(),
        }


async def suggest_strategy_reuse(
    tenant_id: str,
    current_strategy_params: dict[str, Any],
) -> list[dict[str, Any]]:
    """查 strategies 表按名称相似性推荐可复用策略"""
    try:
        async for session in get_session():
            query = (
                select(Strategy)
                .where(Strategy.is_deleted.is_(False))
                .where(Strategy.last_backtest_metrics.isnot(None))
                .limit(10)
            )
            result = await session.execute(query)
            strategies = result.scalars().all()

            if not strategies:
                return []

            return [
                {
                    "strategy_name": s.name,
                    "similarity": 0.5,  # TODO: 实现真实相似度计算
                    "source": "database",
                }
                for s in strategies
            ]
    except Exception as exc:
        logger.warning("suggest_strategy_reuse failed: %s", exc)
        return []


async def get_failure_history(
    tenant_id: str, limit: int = 20
) -> list[dict[str, Any]]:
    """查询 agent_tasks 表中失败历史"""
    try:
        async for session in get_session():
            query = (
                select(AgentTask)
                .where(AgentTask.status == "failed")
                .order_by(AgentTask.completed_at.desc())
                .limit(limit)
            )
            result = await session.execute(query)
            tasks = result.scalars().all()
            return [
                {
                    "task_id": str(t.id),
                    "task_type": t.task_type,
                    "error_msg": t.error_msg or "",
                    "failed_at": t.completed_at.isoformat() if t.completed_at else "",
                }
                for t in tasks
            ]
    except Exception as exc:
        logger.warning("get_failure_history failed: %s", exc)
        return []
