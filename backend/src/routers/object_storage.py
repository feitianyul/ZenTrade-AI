"""T166 - 对象存储路由"""

from typing import Any, Dict, List

from fastapi import APIRouter, Header, HTTPException, UploadFile, File, Query

from src.schemas.response import BaseResponse, ok
from src.services.auth_service import verify_token
from src.services.object_storage_service import (
    delete_object,
    download_object,
    list_objects,
    persist_backup,
    upload_object,
)

router = APIRouter(tags=["ObjectStorage"])


async def _require_user(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.replace("Bearer ", "")
    return await verify_token(token)


@router.post("/object-storage/upload", response_model=BaseResponse[Dict[str, Any]])
async def upload(
    file: UploadFile = File(...),
    object_key: str = Query(...),
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_user(authorization)
    data = await file.read()
    result = await upload_object(
        user.tenant_id, object_key, data, file.content_type or "application/octet-stream"
    )
    return ok(result)


@router.get("/object-storage/list", response_model=BaseResponse[List[Dict[str, Any]]])
async def list_stored_objects(
    prefix: str = "",
    limit: int = 100,
    authorization: str | None = Header(default=None),
) -> BaseResponse[List[Dict[str, Any]]]:
    user = await _require_user(authorization)
    items = await list_objects(user.tenant_id, prefix, limit)
    return ok(items)


@router.delete("/object-storage/{object_key:path}", response_model=BaseResponse[Dict[str, Any]])
async def remove_object(
    object_key: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_user(authorization)
    deleted = await delete_object(user.tenant_id, object_key)
    return ok({"key": object_key, "deleted": deleted})


@router.post("/object-storage/backup-persist", response_model=BaseResponse[Dict[str, Any]])
async def backup_persist(
    backup_id: str = Query(...),
    file: UploadFile = File(...),
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_user(authorization)
    data = await file.read()
    result = await persist_backup(user.tenant_id, backup_id, data)
    return ok(result)
