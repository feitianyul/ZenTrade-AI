"""策略模板表：供模板库与创建策略向导使用，管理员可增删改。"""
from typing import Any, Optional

from sqlalchemy import Float, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class StrategyTemplate(BaseModel):
    __tablename__ = "strategy_templates"

    name: Mapped[str] = mapped_column(String(128))
    desc: Mapped[str] = mapped_column(String(512), default="")
    logic: Mapped[str] = mapped_column(Text, default="")
    logic_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)
    icon: Mapped[str] = mapped_column(String(64), default="fa-chart-line")
    tags: Mapped[list[str]] = mapped_column(JSON, default=list)
    intro: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)
    pros: Mapped[Optional[list[str]]] = mapped_column(JSON, nullable=True, default=None)
    cons: Mapped[Optional[list[str]]] = mapped_column(JSON, nullable=True, default=None)
    tp: Mapped[float] = mapped_column(Float, default=10.0)
    sl: Mapped[float] = mapped_column(Float, default=8.0)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
