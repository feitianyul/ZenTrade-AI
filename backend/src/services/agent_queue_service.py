"""Agent 负载与队列服务 — 查 agents + agent_tasks 表"""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import func, select

from src.core.db import get_session, with_tenant
from src.models.agent import Agent
from src.models.agent_task import AgentTask

logger = logging.getLogger(__name__)


async def get_queue_status(tenant_id: str) -> dict[str, Any]:
    """查询 agent_tasks 表统计队列状态"""
    try:
        async for session in get_session():
            # 按 task_type 分组统计各状态数
            query = (
                select(
                    AgentTask.task_type,
                    AgentTask.status,
                    func.count(AgentTask.id).label("cnt"),
                )
                .group_by(AgentTask.task_type, AgentTask.status)
            )
            result = await session.execute(query)
            rows = result.all()

            # 汇总成 { task_type: { pending: N, running: N, completed: N } }
            queue_map: dict[str, dict[str, int]] = {}
            for row in rows:
                tt = row.task_type or "unknown"
                if tt not in queue_map:
                    queue_map[tt] = {"pending": 0, "running": 0, "completed": 0, "failed": 0}
                st = row.status or "pending"
                queue_map[tt][st] = queue_map[tt].get(st, 0) + row.cnt

            queues = [
                {
                    "name": name,
                    "pending": vals.get("pending", 0),
                    "processing": vals.get("running", 0),
                    "completed": vals.get("completed", 0),
                    "failed": vals.get("failed", 0),
                }
                for name, vals in queue_map.items()
            ]
            return {
                "tenant_id": tenant_id,
                "queues": queues,
                "timestamp": datetime.utcnow().isoformat(),
            }
    except Exception as exc:
        logger.warning("get_queue_status failed: %s", exc)
        return {"tenant_id": tenant_id, "queues": [], "timestamp": datetime.utcnow().isoformat()}


async def get_load_distribution(tenant_id: str = "default") -> dict[str, Any]:
    """查询 agents 表获取负载分布"""
    try:
        async for session in get_session():
            query = select(Agent)
            if tenant_id != "default":
                query = with_tenant(query, Agent, tenant_id)
            result = await session.execute(query)
            agents = result.scalars().all()
            return {
                "agents": [
                    {
                        "agent_id": str(a.id),
                        "name": a.name,
                        "load_percent": round(a.load_factor * 100, 1),
                    }
                    for a in agents
                ]
            }
    except Exception as exc:
        logger.warning("get_load_distribution failed: %s", exc)
        return {"agents": []}
