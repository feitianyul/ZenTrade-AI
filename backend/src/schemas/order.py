"""T199 - 订单 Schema 含环境字段"""

from typing import Optional

from pydantic import BaseModel, Field


class OrderCreate(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=32)
    direction: str = Field(..., pattern="^(BUY|SELL)$")
    price: float = Field(..., gt=0)
    volume: int = Field(..., gt=0)
    env: str = Field(default="sim", pattern="^(sim|real)$")
    strategy_id: Optional[str] = None


class OrderOut(BaseModel):
    order_id: str
    user_id: str
    env: str
    symbol: str
    direction: str
    price: float
    volume: int
    status: str
    gateway_ref: str
    is_sim: bool = False


class SimOrderTag(BaseModel):
    """模拟订单标注信息"""
    order_id: str
    env: str
    is_sim: bool
    sim_label: str = ""

    @staticmethod
    def from_order(order_id: str, env: str) -> "SimOrderTag":
        is_sim = env == "sim"
        label = "[模拟]" if is_sim else "[实盘]"
        return SimOrderTag(
            order_id=order_id, env=env, is_sim=is_sim, sim_label=label
        )
