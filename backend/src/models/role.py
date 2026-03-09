from sqlalchemy import JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class Role(BaseModel):
    __tablename__ = "roles"

    name: Mapped[str] = mapped_column(String(64), unique=True)
    description: Mapped[str] = mapped_column(Text, default="")
    permissions: Mapped[dict] = mapped_column(JSON, default={})
