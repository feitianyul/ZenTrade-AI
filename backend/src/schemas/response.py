from datetime import datetime
from typing import Generic, Optional, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")

class BaseResponse(BaseModel, Generic[T]):
    code: int = 0
    message: str = "ok"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    data: Optional[T] = None
    data_updated_at: Optional[str] = Field(default=None, description="数据最后更新时间 ISO8601，供前端判断是否落后 30 分钟触发预热")

def ok(data: Optional[T] = None, data_updated_at: Optional[str] = None) -> BaseResponse[T]:
    return BaseResponse(code=0, message="ok", data=data, data_updated_at=data_updated_at)

def fail(code: int, message: str) -> BaseResponse[None]:
    return BaseResponse(code=code, message=message, data=None)
