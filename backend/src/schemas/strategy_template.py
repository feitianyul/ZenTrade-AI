"""策略模板 API 的请求/响应模型"""
from typing import Optional

from pydantic import BaseModel, Field


class StrategyTemplateOut(BaseModel):
    id: str
    name: str
    desc: str = ""
    logic: str = ""
    logic_code: Optional[str] = None
    icon: str = "fa-chart-line"
    tags: list[str] = Field(default_factory=list)
    intro: Optional[str] = None
    pros: Optional[list[str]] = None
    cons: Optional[list[str]] = None
    tp: float = 10.0
    sl: float = 8.0
    sort_order: int = 0

    class Config:
        from_attributes = True


class StrategyTemplateCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    desc: str = Field(default="", max_length=512)
    logic: str = Field(default="")
    logic_code: Optional[str] = None
    icon: str = Field(default="fa-chart-line", max_length=64)
    tags: list[str] = Field(default_factory=list)
    intro: Optional[str] = None
    pros: Optional[list[str]] = None
    cons: Optional[list[str]] = None
    tp: float = Field(default=10.0, ge=0, le=100)
    sl: float = Field(default=8.0, ge=0, le=50)
    sort_order: int = Field(default=0, ge=0)


class StrategyTemplateListResponse(BaseModel):
    """列表响应：data + can_manage（是否允许系统管理员增删改）"""
    data: list[StrategyTemplateOut] = Field(default_factory=list)
    can_manage: bool = False


class StrategyTemplateUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=128)
    desc: Optional[str] = Field(None, max_length=512)
    logic: Optional[str] = None
    logic_code: Optional[str] = None
    icon: Optional[str] = Field(None, max_length=64)
    tags: Optional[list[str]] = None
    intro: Optional[str] = None
    pros: Optional[list[str]] = None
    cons: Optional[list[str]] = None
    tp: Optional[float] = Field(None, ge=0, le=100)
    sl: Optional[float] = Field(None, ge=0, le=50)
    sort_order: Optional[int] = Field(None, ge=0)
