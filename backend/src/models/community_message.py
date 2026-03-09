from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class CommunityMessage(BaseModel):
    __tablename__ = "community_messages"

    sender_id: Mapped[str] = mapped_column(String(64), index=True)
    receiver_id: Mapped[str] = mapped_column(String(64), index=True)
    content: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), default="unread")
