from abc import ABC, abstractmethod
from typing import Any, Iterable


class LlmService(ABC):
    @abstractmethod
    async def chat(self, messages: Iterable[dict[str, Any]]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def embed(self, inputs: Iterable[str]) -> list[list[float]]:
        raise NotImplementedError

    @abstractmethod
    async def health(self) -> bool:
        raise NotImplementedError
