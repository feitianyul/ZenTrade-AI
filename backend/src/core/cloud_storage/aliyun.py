"""T216 - 阿里云 OSS 适配"""

import os
from typing import Any, Optional

from src.core.cloud_storage.base import CloudStorageBackend

OSS_ENDPOINT = os.getenv("ALIYUN_OSS_ENDPOINT", "")
OSS_BUCKET = os.getenv("ALIYUN_OSS_BUCKET", "")
OSS_ACCESS_KEY = os.getenv("ALIYUN_OSS_ACCESS_KEY", "")
OSS_SECRET_KEY = os.getenv("ALIYUN_OSS_SECRET_KEY", "")


class AliyunOSSBackend(CloudStorageBackend):
    """阿里云 OSS 存储后端"""

    def __init__(self) -> None:
        self.endpoint = OSS_ENDPOINT
        self.bucket = OSS_BUCKET

    async def upload(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        # 占位 - 生产中使用 oss2 SDK
        return {
            "backend": "aliyun_oss",
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
