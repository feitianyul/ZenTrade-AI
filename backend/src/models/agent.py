from sqlalchemy import JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class Agent(BaseModel):
    __tablename__ = "agents"

    name: Mapped[str] = mapped_column(String(100))
    role: Mapped[str] = mapped_column(String(50))  # e.g., 'analyst', 'trader', 'reviewer'
    capabilities: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(20), default="idle")  # idle, busy, offline
    load_factor: Mapped[float] = mapped_column(default=0.0)
    config: Mapped[dict] = mapped_column(
        JSON,
        default=dict,
    )  # Specific configuration for this agent
