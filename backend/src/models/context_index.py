from sqlalchemy import JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class ContextIndex(BaseModel):
    __tablename__ = "context_indices"

    session_id: Mapped[str] = mapped_column(String(64), index=True)
    context_type: Mapped[str] = mapped_column(String(50)) # e.g., 'conversation', 'market_analysis'
    embedding_id: Mapped[str] = mapped_column(String(64), nullable=True) # ID in vector store
    summary: Mapped[str] = mapped_column(String(500), nullable=True)
    meta_data: Mapped[dict] = mapped_column(JSON, default=dict)
