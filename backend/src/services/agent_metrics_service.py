"""Agent 运行态监控指标 — 查 agents + agent_tasks 表"""

import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from src.core.db import get_session, with_tenant
from src.models.agent import Agent
from src.models.agent_task import AgentTask

logger = logging.getLogger(__name__)


async def get_agent_metrics(tenant_id: str) -> dict[str, Any]:
    """查询 agents 表获取运行态指标"""
    try:
        async for session in get_session():
            # 查询所有 agent
            query = with_tenant(select(Agent), Agent, tenant_id)
            result = await session.execute(query)
            agents = result.scalars().all()

            agent_list = []
            for a in agents:
                # 统计该 agent 最近 1 小时的任务
                since = datetime.utcnow() - timedelta(hours=1)
                task_q = (
                    select(
                        func.count(AgentTask.id).label("total"),
                        func.count(
                            func.nullif(AgentTask.status, "failed")
                        ).label("non_failed"),
                    )
                    .where(AgentTask.agent_id == a.id)
                    .where(AgentTask.created_at >= since)
                )
                task_result = await session.execute(task_q)
                row = task_result.one_or_none()
                total = row.total if row else 0
                non_failed = row.non_failed if row else 0
                error_rate = round(1 - (non_failed / total), 4) if total > 0 else 0.0

                agent_list.append({
                    "agent_id": str(a.id),
                    "name": a.name,
                    "status": a.status,
                    "load_percent": round(a.load_factor * 100, 1),
                    "requests_per_min": total,
                    "error_rate": error_rate,
                })

            return {
                "tenant_id": tenant_id,
                "agents": agent_list,
                "timestamp": datetime.utcnow().isoformat(),
            }
    except Exception as exc:
        logger.warning("get_agent_metrics failed: %s", exc)
        return {"tenant_id": tenant_id, "agents": [], "timestamp": datetime.utcnow().isoformat()}


async def get_agent_health_summary(tenant_id: str = "default") -> dict[str, Any]:
    """Agent 健康汇总 — 查 agents 表统计状态"""
    try:
        async for session in get_session():
            query = select(Agent)
            if tenant_id != "default":
                query = with_tenant(query, Agent, tenant_id)
            result = await session.execute(query)
            agents = result.scalars().all()

            total = len(agents)
            healthy = sum(1 for a in agents if a.status in ("idle", "busy", "running"))
            down = sum(1 for a in agents if a.status == "offline")
            degraded = total - healthy - down

            return {
                "total_agents": total,
                "healthy": healthy,
                "degraded": degraded,
                "down": down,
            }
    except Exception as exc:
        logger.warning("get_agent_health_summary failed: %s", exc)
        return {"total_agents": 0, "healthy": 0, "degraded": 0, "down": 0}
