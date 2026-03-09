"""T223 - 知识库模型"""

from sqlalchemy import Boolean, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class KnowledgeBase(BaseModel):
    __tablename__ = "knowledge_bases"

    name: Mapped[str] = mapped_column(String(128), index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    category: Mapped[str] = mapped_column(String(64), default="general")
    entry_count: Mapped[int] = mapped_column(Integer, default=0)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class KnowledgeEntry(BaseModel):
    __tablename__ = "knowledge_entries"

    kb_id: Mapped[str] = mapped_column(String(36), index=True)
    title: Mapped[str] = mapped_column(String(256))
    content: Mapped[str] = mapped_column(Text)
    embedding_id: Mapped[str] = mapped_column(String(128), default="")
    source: Mapped[str] = mapped_column(String(256), default="")
    tags_json: Mapped[dict] = mapped_column(JSON, default=dict)
