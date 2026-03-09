"""T215 - 云端存储适配基类"""

from abc import ABC, abstractmethod
from typing import Any, Optional


class CloudStorageBackend(ABC):
    """云端存储后端抽象基类"""

    @abstractmethod
    async def upload(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        ...

    @abstractmethod
    async def download(self, key: str) -> Optional[bytes]:
        ...

    @abstractmethod
    async def delete(self, key: str) -> bool:
        ...

    @abstractmethod
    async def list_objects(
        self, prefix: str = "", limit: int = 100
    ) -> list[dict[str, Any]]:
        ...

    @abstractmethod
    async def exists(self, key: str) -> bool:
        ...

    @abstractmethod
    async def get_metadata(self, key: str) -> Optional[dict[str, Any]]:
        ...
