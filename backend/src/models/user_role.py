from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class UserRole(BaseModel):
    __tablename__ = "user_roles"

    user_id: Mapped[str] = mapped_column(String(64), index=True)
    role_id: Mapped[str] = mapped_column(String(36), index=True)
