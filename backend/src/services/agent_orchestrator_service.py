from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.agent import Agent
from src.models.agent_task import AgentTask


class AgentOrchestratorService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def register_agent(self, tenant_id: str, agent_data: dict) -> Agent:
        agent = Agent(tenant_id=tenant_id, **agent_data)
        self.db.add(agent)
        await self.db.commit()
        await self.db.refresh(agent)
        return agent

    async def submit_task(self, tenant_id: str, task_data: dict) -> AgentTask:
        task = AgentTask(tenant_id=tenant_id, **task_data)
        self.db.add(task)
        await self.db.commit()
        await self.db.refresh(task)
        
        # Try to dispatch immediately (simplification)
        await self.dispatch_tasks(tenant_id)
        return task

    async def dispatch_tasks(self, tenant_id: str):
        # Find pending tasks
        stmt = select(AgentTask).where(
            AgentTask.tenant_id == tenant_id,
            AgentTask.status == "pending"
        ).order_by(AgentTask.priority.desc(), AgentTask.created_at)
        result = await self.db.execute(stmt)
        tasks = result.scalars().all()

        for task in tasks:
            # Find suitable agent
            agent = await self._find_best_agent(tenant_id, task.task_type)
            if agent:
                await self._assign_task(task, agent)

    async def _find_best_agent(self, tenant_id: str, role_needed: str) -> Optional[Agent]:
        # Simple logic: find idle agent with matching role (mapped from task_type)
        # Assuming task_type maps to role for now, or use capabilities
        stmt = select(Agent).where(
            Agent.tenant_id == tenant_id,
            Agent.status == "idle"
            # In real scenario, check capabilities or role
        ).order_by(Agent.load_factor)
        result = await self.db.execute(stmt)
        return result.scalars().first()

    async def _assign_task(self, task: AgentTask, agent: Agent):
        task.agent_id = agent.id
        task.status = "running"
        task.started_at = datetime.utcnow()
        
        agent.status = "busy"
        agent.load_factor += 1.0 # Simple increment
        
        await self.db.commit()
        # Trigger actual execution logic (async background task) here

    async def complete_task(self, task_id: str, result: dict):
        stmt = select(AgentTask).where(AgentTask.id == task_id)
        res = await self.db.execute(stmt)
        task = res.scalars().first()
        if task:
            task.status = "completed"
            task.result = result
            task.completed_at = datetime.utcnow()
            
            # Free up agent
            if task.agent_id:
                agent_stmt = select(Agent).where(Agent.id == task.agent_id)
                agent_res = await self.db.execute(agent_stmt)
                agent = agent_res.scalars().first()
                if agent:
                    agent.status = "idle"
                    agent.load_factor = max(0, agent.load_factor - 1.0)
            
            await self.db.commit()
