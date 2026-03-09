from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Optional


class GatewayPlugin:
    def on_init(self, gateway: 'TradingGateway'): pass
    def on_connect(self): pass
    def on_order(self, order: dict): pass
    def on_tick(self, tick: dict): pass

class TradingGateway(ABC):
    def __init__(self, gateway_name: str, config: Optional[dict[str, Any]] = None):
        self.gateway_name = gateway_name
        self.config = config or {}
        self.connected = False
        self.plugins: List[GatewayPlugin] = []

    def register_plugin(self, plugin: GatewayPlugin):
        plugin.on_init(self)
        self.plugins.append(plugin)

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def send_order(self, order: dict[str, Any]) -> str:
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def query_positions(self) -> Iterable[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def query_account(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def subscribe(self, symbols: list[str]) -> None:
        raise NotImplementedError
