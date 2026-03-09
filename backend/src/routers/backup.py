import os
from typing import List

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_db
from src.models.backup import Backup, BackupStatus
from src.schemas.backup import BackupCreate, BackupListData, BackupOut, BackupPolicyOut, BackupPolicyPut, BackupRestoreBody
from src.schemas.response import BaseResponse, ok
from src.services.auth_service import verify_token
from src.services.backup_policy_service import get_backup_policy_api, set_backup_policy_api
from src.services.backup_service import (
    check_backup_readiness,
    check_binlog_for_incremental,
    create_backup_task,
    delete_backup_by_id,
    get_backups,
    restore_backup,
)

router = APIRouter(tags=["Backup"])


async def _require_admin(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.replace("Bearer ", "")
    try:
        user = await verify_token(token)
        return user
    except HTTPException:
        raise


@router.post("/backups", response_model=BaseResponse[BackupOut])
async def create_backup(
    backup_in: BackupCreate,
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_db),
) -> BaseResponse[BackupOut]:
    user = await _require_admin(authorization)
    backup = await create_backup_task(
        session,
        user.tenant_id,
        backup_in.name,
        backup_in.type,
        content=backup_in.content,
        destination=backup_in.destination,
    )
    return ok(backup)


@router.get("/backups", response_model=BaseResponse[BackupListData])
async def list_backups(
    page: int = 1,
    page_size: int = 15,
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_db),
) -> BaseResponse[BackupListData]:
    user = await _require_admin(authorization)
    if page < 1:
        page = 1
    if page_size < 0:
        page_size = 15
    skip = (page - 1) * page_size
    items, total = await get_backups(session, user.tenant_id, skip=skip, limit=page_size)
    return ok(BackupListData(items=items, total=total, page=page, page_size=page_size))


@router.get("/backups/{backup_id}/download")
async def download_backup(
    backup_id: str,
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_db),
):
    """下载备份文件（仅 success 且本地路径）。"""
    user = await _require_admin(authorization)
    backup = await session.get(Backup, backup_id)
    if not backup or backup.tenant_id != user.tenant_id:
        raise HTTPException(status_code=404, detail="备份不存在或无权访问")
    if backup.status != BackupStatus.SUCCESS:
        raise HTTPException(status_code=400, detail="仅成功状态的备份可下载")
    if not backup.location:
        raise HTTPException(status_code=404, detail="备份文件位置不存在")
    if not os.path.isfile(backup.location):
        raise HTTPException(status_code=404, detail="备份文件不存在")
    is_zip = backup.location.endswith(".zip")
    filename = f"backup_{backup_id}.zip" if is_zip else f"backup_{backup_id}.json"
    media_type = "application/zip" if is_zip else "application/octet-stream"
    return FileResponse(
        backup.location,
        media_type=media_type,
        filename=filename,
    )


@router.delete("/backups/{backup_id}")
async def delete_backup(
    backup_id: str,
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_db),
) -> BaseResponse[dict]:
    """删除单条备份记录及本地文件。"""
    user = await _require_admin(authorization)
    deleted = await delete_backup_by_id(session, user.tenant_id, backup_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="备份不存在或无权删除")
    return ok({"deleted": backup_id})


@router.post("/backups/{backup_id}/restore", response_model=BaseResponse[dict])
async def restore_from_backup(
    backup_id: str,
    body: BackupRestoreBody | None = None,
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_db),
) -> BaseResponse[dict]:
    user = await _require_admin(authorization)
    restore_config = body.restore_config if body else None
    try:
        result = await restore_backup(
            session, user.tenant_id, backup_id, restore_config=restore_config
        )
        return ok({"status": "restore_completed", **result})
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))


@router.get("/backup-policy", response_model=BaseResponse[BackupPolicyOut])
async def get_backup_policy(
    authorization: str | None = Header(default=None),
) -> BaseResponse[BackupPolicyOut]:
    """获取备份策略：schedule_cron、retention_days、enabled（规格 5.1）"""
    user = await _require_admin(authorization)
    policy = await get_backup_policy_api(user.tenant_id)
    return ok(policy)


@router.get("/backup-policy/binlog-status", response_model=BaseResponse[dict])
async def get_backup_policy_binlog_status(
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """检测当前 MySQL 是否开启 binlog，用于判断是否具备增量备份条件。"""
    await _require_admin(authorization)
    result = await check_binlog_for_incremental()
    return ok(result)


@router.get("/backup-policy/backup-readiness", response_model=BaseResponse[dict])
async def get_backup_readiness(
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict]:
    """检测各组件备份条件：MySQL Binlog、Redis（含配置扫描与 docker 路径）、Qdrant、ClickHouse。"""
    await _require_admin(authorization)
    result = await check_backup_readiness()
    return ok(result)


@router.put("/backup-policy", response_model=BaseResponse[BackupPolicyOut])
async def put_backup_policy(
    body: BackupPolicyPut,
    authorization: str | None = Header(default=None),
) -> BaseResponse[BackupPolicyOut]:
    """保存备份策略（规格 5.1）。定时任务与超期清理按此策略执行。"""
    user = await _require_admin(authorization)
    policy = await set_backup_policy_api(
        user.tenant_id,
        schedule_cron=body.schedule_cron,
        full_interval_days=body.full_interval_days,
        schedule_time=body.schedule_time,
        incremental_enabled=body.incremental_enabled,
        retention_days=body.retention_days,
        enabled=body.enabled,
    )
    return ok(policy)
