from sqlalchemy import JSON, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class CommunityPost(BaseModel):
    __tablename__ = "community_posts"

    author_id: Mapped[str] = mapped_column(String(64), index=True)
    title: Mapped[str] = mapped_column(String(128))
    content: Mapped[str] = mapped_column(Text)
    tags: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)
    post_type: Mapped[str] = mapped_column(String(32), default="discussion")
    like_count: Mapped[int] = mapped_column(Integer, default=0)
    comment_count: Mapped[int] = mapped_column(Integer, default=0)
