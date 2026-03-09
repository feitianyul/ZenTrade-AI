from typing import Any, Dict, Iterable

from src.services.trading_gateway.base import TradingGateway


class IbGateway(TradingGateway):
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("ib", config)

    async def connect(self) -> None:
        self.connected = True
        for p in self.plugins:
            p.on_connect()

    async def close(self) -> None:
        self.connected = False

    async def send_order(self, order: dict[str, Any]) -> str:
        for p in self.plugins:
            p.on_order(order)
        return "ib_" + str(order.get("symbol", "order"))

    async def cancel_order(self, order_id: str) -> None:
        return None

    async def query_positions(self) -> Iterable[dict[str, Any]]:
        return []

    async def query_account(self) -> dict[str, Any]:
        return {"balance": 0}

    async def subscribe(self, symbols: list[str]) -> None:
        return None
