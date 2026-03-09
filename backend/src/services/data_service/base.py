from abc import ABC, abstractmethod
from typing import Any, Iterable


class MarketDataService(ABC):
    @abstractmethod
    async def fetch_kline(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str = "1d",
    ) -> Iterable[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def fetch_quote(self, symbol: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def fetch_fundamental(self, symbol: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def health(self) -> bool:
        raise NotImplementedError
