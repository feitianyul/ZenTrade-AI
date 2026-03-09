"""CTP 期货交易网关

TODO: 接入 CTP/OpenCTP 真实 API。
      当前为框架占位，所有操作返回模拟结果。
      真实接入步骤:
        1. 安装 vnpy_ctp 或 openctp-ctp 包
        2. 配置 broker_id, front_addr, user_id, password
        3. 替换 connect/send_order/query_positions 中的 Mock 逻辑
"""

from typing import Any, Dict, Iterable

from src.services.trading_gateway.base import TradingGateway


class CtpGateway(TradingGateway):
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("ctp", config)

    def load_plugin(self, plugin_path: str):
        # TODO: 实现真实的插件动态加载
        pass

    async def connect(self) -> None:
        self.connected = True
        for p in self.plugins:
            p.on_connect()

    async def close(self) -> None:
        self.connected = False

    async def send_order(self, order: dict[str, Any]) -> str:
        for p in self.plugins:
            p.on_order(order)
        return "ctp_" + str(order.get("symbol", "order"))

    async def cancel_order(self, order_id: str) -> None:
        return None

    async def query_positions(self) -> Iterable[dict[str, Any]]:
        return []

    async def query_account(self) -> dict[str, Any]:
        return {"balance": 0}

    async def subscribe(self, symbols: list[str]) -> None:
        return None
