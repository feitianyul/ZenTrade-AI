import uuid

from src.services.trading_gateway.base import TradingGateway


class VnpyGateway(TradingGateway):
    def __init__(self) -> None:
        super().__init__("vnpy")

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def send_order(self, order: dict[str, object]) -> str:
        if not self.connected:
            await self.connect()
        return f"VN{uuid.uuid4().hex[:12].upper()}"

    async def cancel_order(self, order_id: str) -> None:
        return None

    async def query_positions(self) -> list[dict[str, object]]:
        return []

    async def query_account(self) -> dict[str, object]:
        return {"balance": 0.0}

    async def subscribe(self, symbols: list[str]) -> None:
        return None


_GATEWAY = VnpyGateway()


def get_vnpy_gateway() -> VnpyGateway:
    return _GATEWAY
