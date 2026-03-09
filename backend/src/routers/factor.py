from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_db
from src.schemas.response import BaseResponse as ResponseBase
from src.services.factor_service import FactorService

router = APIRouter(prefix="/factors", tags=["Panda Factors"])

@router.get("/public", response_model=ResponseBase)
async def list_public_factors(
    category: str = None,
    db: AsyncSession = Depends(get_db)
):
    service = FactorService(db)
    factors = await service.get_public_factors(category)
    return ResponseBase(data=factors)
