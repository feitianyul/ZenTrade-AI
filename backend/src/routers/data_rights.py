from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.auth import get_current_user
from src.core.db import get_db
from src.schemas.masking import DataRightRequest
from src.schemas.user import User, UserOut
from src.services.data_rights_service import process_rights_request

router = APIRouter(prefix="/data-rights", tags=["Data Rights"])

@router.post("/request")
async def submit_rights_request(
    request: DataRightRequest,
    current_user: UserOut = Depends(get_current_user),
    session: AsyncSession = Depends(get_db)
):
    result = await process_rights_request(session, current_user.user_id, request)
    return result
