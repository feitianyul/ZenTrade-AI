from typing import Any, Dict

from pydantic import BaseModel


class AgentCreate(BaseModel):
    name: str
    role: str
    capabilities: Dict[str, Any] = {}
    config: Dict[str, Any] = {}

class AgentTaskCreate(BaseModel):
    task_type: str
    payload: Dict[str, Any]
    priority: int = 0
