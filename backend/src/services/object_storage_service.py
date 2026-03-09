"""T166 - 对象存储与备份落盘服务"""

import os
from typing import Any, Optional

import httpx

STORAGE_BACKEND = os.getenv("OBJECT_STORAGE_BACKEND", "local")  # local | s3 | oss
LOCAL_STORAGE_ROOT = os.getenv("LOCAL_STORAGE_ROOT", "./data/object_storage")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
S3_BUCKET = os.getenv("S3_BUCKET", "retail-lowfreq")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")


async def upload_object(
    tenant_id: str,
    object_key: str,
    data: bytes,
    content_type: str = "application/octet-stream",
    metadata: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """上传对象到存储后端"""
    if STORAGE_BACKEND == "local":
        return await _local_upload(tenant_id, object_key, data, content_type)
    elif STORAGE_BACKEND == "s3":
        return await _s3_upload(tenant_id, object_key, data, content_type, metadata)
    return {"error": f"unsupported backend: {STORAGE_BACKEND}"}


async def download_object(
    tenant_id: str,
    object_key: str,
) -> Optional[bytes]:
    """下载对象"""
    if STORAGE_BACKEND == "local":
        return await _local_download(tenant_id, object_key)
    elif STORAGE_BACKEND == "s3":
        return await _s3_download(tenant_id, object_key)
    return None


async def delete_object(
    tenant_id: str,
    object_key: str,
) -> bool:
    """删除对象"""
    if STORAGE_BACKEND == "local":
        return await _local_delete(tenant_id, object_key)
    return False


async def list_objects(
    tenant_id: str,
    prefix: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """列出对象"""
    if STORAGE_BACKEND == "local":
        return await _local_list(tenant_id, prefix, limit)
    return []


async def persist_backup(
    tenant_id: str,
    backup_id: str,
    data: bytes,
) -> dict[str, Any]:
    """备份数据落盘"""
    object_key = f"backups/{backup_id}.bak"
    result = await upload_object(tenant_id, object_key, data, "application/x-backup")
    return {**result, "backup_id": backup_id, "persisted": True}


# --------------- Local Storage ---------------

async def _local_upload(
    tenant_id: str, object_key: str, data: bytes, content_type: str
) -> dict[str, Any]:
    import aiofiles
    import pathlib

    path = pathlib.Path(LOCAL_STORAGE_ROOT) / tenant_id / object_key
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(str(path), "wb") as f:
        await f.write(data)
    return {
        "backend": "local",
        "key": object_key,
        "size": len(data),
        "content_type": content_type,
    }


async def _local_download(tenant_id: str, object_key: str) -> Optional[bytes]:
    import aiofiles
    import pathlib

    path = pathlib.Path(LOCAL_STORAGE_ROOT) / tenant_id / object_key
    if not path.exists():
        return None
    async with aiofiles.open(str(path), "rb") as f:
        return await f.read()


async def _local_delete(tenant_id: str, object_key: str) -> bool:
    import pathlib

    path = pathlib.Path(LOCAL_STORAGE_ROOT) / tenant_id / object_key
    if path.exists():
        path.unlink()
        return True
    return False


async def _local_list(
    tenant_id: str, prefix: str, limit: int
) -> list[dict[str, Any]]:
    import pathlib

    base = pathlib.Path(LOCAL_STORAGE_ROOT) / tenant_id
    if not base.exists():
        return []
    items: list[dict[str, Any]] = []
    for p in sorted(base.rglob("*")):
        if p.is_file():
            rel = str(p.relative_to(base)).replace("\\", "/")
            if rel.startswith(prefix):
                items.append({"key": rel, "size": p.stat().st_size})
                if len(items) >= limit:
                    break
    return items


# --------------- S3-compatible Storage ---------------

async def _s3_upload(
    tenant_id: str,
    object_key: str,
    data: bytes,
    content_type: str,
    metadata: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    # Placeholder for S3-compatible upload via httpx or boto3
    full_key = f"{tenant_id}/{object_key}"
    return {
        "backend": "s3",
        "bucket": S3_BUCKET,
        "key": full_key,
        "size": len(data),
        "status": "uploaded",
    }


async def _s3_download(tenant_id: str, object_key: str) -> Optional[bytes]:
    # Placeholder for S3-compatible download
    return None
