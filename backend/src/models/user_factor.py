from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class UserFactor(BaseModel):
    __tablename__ = "user_factors"

    name: Mapped[str] = mapped_column(String(100))
    code: Mapped[str] = mapped_column(String(5000)) # Python code
    description: Mapped[str] = mapped_column(String(500), nullable=True)
    is_public: Mapped[bool] = mapped_column(default=False)
