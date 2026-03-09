from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.auth import get_current_user
from src.core.db import get_db
from src.schemas.response import BaseResponse, ok
from src.services.user_factor_service import UserFactorService

router = APIRouter(prefix="/user-factors", tags=["User Factors"])


class FactorCreate(BaseModel):
    name: str
    code: str
    description: str = None


@router.post("/")
async def create_user_factor(
    factor: FactorCreate,
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # 自定义因子创建仅资深散户可用
    if current_user.level != "advanced":
        raise HTTPException(
            status_code=403,
            detail="自定义因子创建需资深散户等级",
        )
    service = UserFactorService(db)
    result = await service.create_factor(
        current_user.tenant_id,
        factor.name,
        factor.code,
        factor.description,
    )
    return ok(result)


@router.get("/")
async def list_my_factors(
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    service = UserFactorService(db)
    factors = await service.get_my_factors(current_user.tenant_id)
    return ok(factors)
