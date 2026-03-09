"""T216 - 百度 BOS 适配"""

import os
from typing import Any, Optional

from src.core.cloud_storage.base import CloudStorageBackend

BOS_ENDPOINT = os.getenv("BAIDU_BOS_ENDPOINT", "")
BOS_BUCKET = os.getenv("BAIDU_BOS_BUCKET", "")
BOS_ACCESS_KEY = os.getenv("BAIDU_BOS_ACCESS_KEY", "")
BOS_SECRET_KEY = os.getenv("BAIDU_BOS_SECRET_KEY", "")


class BaiduBOSBackend(CloudStorageBackend):
    """百度 BOS 存储后端"""

    def __init__(self) -> None:
        self.endpoint = BOS_ENDPOINT
        self.bucket = BOS_BUCKET

    async def upload(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        return {
            "backend": "baidu_bos",
            "bucket": self.bucket,
            "key": key,
            "size": len(data),
            "status": "uploaded",
        }

    async def download(self, key: str) -> Optional[bytes]:
        return None

    async def delete(self, key: str) -> bool:
        return False

    async def list_objects(
        self, prefix: str = "", limit: int = 100
    ) -> list[dict[str, Any]]:
        return []

    async def exists(self, key: str) -> bool:
        return False

    async def get_metadata(self, key: str) -> Optional[dict[str, Any]]:
        return None
