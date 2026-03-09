import json

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.auth import get_current_user
from src.core.db import get_db
from src.services.ai_config_backup_service import AIConfigBackupService

router = APIRouter(prefix="/ai-config/backup", tags=["AI Config Backup"])

@router.get("/download")
async def download_backup(
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    service = AIConfigBackupService(db)
    backup_data = await service.create_backup(current_user.tenant_id)
    return backup_data

@router.post("/restore")
async def restore_backup(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    try:
        content = await file.read()
        backup_data = json.loads(content)
        service = AIConfigBackupService(db)
        await service.restore_backup(current_user.tenant_id, backup_data, current_user.user_id)
        return {"message": "Backup restored successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
