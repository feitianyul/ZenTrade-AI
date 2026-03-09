from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class CommunityRelation(BaseModel):
    __tablename__ = "community_relations"

    user_id: Mapped[str] = mapped_column(String(64), index=True)
    target_user_id: Mapped[str] = mapped_column(String(64), index=True)
    relation_type: Mapped[str] = mapped_column(String(32), default="follow")
