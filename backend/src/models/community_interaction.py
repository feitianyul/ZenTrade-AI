from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class CommunityInteraction(BaseModel):
    __tablename__ = "community_interactions"

    post_id: Mapped[str] = mapped_column(String(36), index=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    interaction_type: Mapped[str] = mapped_column(String(32))
    content: Mapped[str] = mapped_column(Text, default="")
