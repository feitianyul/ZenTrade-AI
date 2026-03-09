from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.auth import get_current_user
from src.core.db import get_db
from src.schemas.response import BaseResponse as ResponseBase
from src.services.pricing_policy_service import PricingPolicyService

router = APIRouter(prefix="/pricing", tags=["Pricing & Entitlements"])

@router.get("/entitlements", response_model=ResponseBase)
async def get_my_entitlements(
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    service = PricingPolicyService(db)
    entitlements = await service.get_user_entitlements(
        current_user.tenant_id,
        current_user.id,
        user_level=current_user.level,
    )
    return ResponseBase(data=entitlements)

@router.get("/plans", response_model=ResponseBase)
async def get_pricing_plans():
    """Public endpoint to list available plans"""
    plans = [
        {
            "name": "Basic",
            "price": "Free",
            "features": [
                "5 Strategies",
                "10 AI Calls/Day",
                "Delayed Data",
            ],
        },
        {
            "name": "Pro",
            "price": "¥99/mo",
            "features": [
                "20 Strategies",
                "100 AI Calls/Day",
                "Real-time Data",
                "Priority Support",
            ],
        },
        {
            "name": "VIP",
            "price": "¥299/mo",
            "features": [
                "Unlimited Strategies",
                "1000 AI Calls/Day",
                "Real-time Data",
                "1-on-1 Advisor",
            ],
        }
    ]
    return ResponseBase(data=plans)
