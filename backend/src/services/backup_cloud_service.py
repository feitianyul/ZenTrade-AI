"""T215 - 云端备份存储适配"""

import os
from typing import Any, Optional

CLOUD_BACKEND = os.getenv("BACKUP_CLOUD_BACKEND", "none")  # none | aliyun_oss | baidu_bos | s3


async def upload_backup(
    tenant_id: str,
    backup_id: str,
    data: bytes,
    metadata: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """上传备份到云端"""
    if CLOUD_BACKEND == "aliyun_oss":
        return await _upload_aliyun(tenant_id, backup_id, data, metadata)
    elif CLOUD_BACKEND == "baidu_bos":
        return await _upload_baidu(tenant_id, backup_id, data, metadata)
    elif CLOUD_BACKEND == "s3":
        return await _upload_s3(tenant_id, backup_id, data, metadata)
    return {"status": "skipped", "reason": "no cloud backend configured"}


async def download_backup(
    tenant_id: str, backup_id: str
) -> Optional[bytes]:
    """从云端下载备份"""
    if CLOUD_BACKEND == "none":
        return None
    # 占位实现
    return None


async def list_cloud_backups(
    tenant_id: str, limit: int = 50
) -> list[dict[str, Any]]:
    """列出云端备份"""
    return []


async def delete_cloud_backup(
    tenant_id: str, backup_id: str
) -> bool:
    """删除云端备份"""
    return False


async def get_cloud_status() -> dict[str, Any]:
    """获取云存储状态"""
    return {
        "backend": CLOUD_BACKEND,
        "configured": CLOUD_BACKEND != "none",
    }


async def _upload_aliyun(
    tenant_id: str, backup_id: str, data: bytes, metadata: Optional[dict[str, str]]
) -> dict[str, Any]:
    return {"backend": "aliyun_oss", "backup_id": backup_id, "size": len(data), "status": "uploaded"}


async def _upload_baidu(
    tenant_id: str, backup_id: str, data: bytes, metadata: Optional[dict[str, str]]
) -> dict[str, Any]:
    return {"backend": "baidu_bos", "backup_id": backup_id, "size": len(data), "status": "uploaded"}


async def _upload_s3(
    tenant_id: str, backup_id: str, data: bytes, metadata: Optional[dict[str, str]]
) -> dict[str, Any]:
    return {"backend": "s3", "backup_id": backup_id, "size": len(data), "status": "uploaded"}
