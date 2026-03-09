from typing import Any, Optional

from pydantic import BaseModel, Field


class CommunityPostCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=128)
    content: str = Field(..., min_length=1)
    tags: Optional[dict[str, str]] = None
    post_type: str = Field(default="discussion", max_length=32)


class CommunityPostOut(BaseModel):
    post_id: str
    tenant_id: str
    author_id: str
    title: str
    content: str
    tags: dict[str, Any]
    post_type: str
    like_count: int
    comment_count: int


class CommunityInteractionCreate(BaseModel):
    post_id: str
    interaction_type: str
    content: Optional[str] = ""


class CommunityInteractionOut(BaseModel):
    interaction_id: str
    post_id: str
    user_id: str
    interaction_type: str
    content: str


class CommunityMessageCreate(BaseModel):
    receiver_id: str
    content: str = Field(..., min_length=1, max_length=1000)


class CommunityMessageOut(BaseModel):
    message_id: str
    sender_id: str
    receiver_id: str
    content: str
    status: str


class CommunityRankItem(BaseModel):
    user_id: str
    score: int
    label: str
