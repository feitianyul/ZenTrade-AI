import os
from typing import Any, Dict

from fastapi import APIRouter

from src.schemas.response import BaseResponse, ok
from src.services.ops_service import get_health_with_components

router = APIRouter()


@router.get("/system/health", response_model=BaseResponse[Dict[str, Any]],
            summary="系统健康检查",
            description="含组件快速探活结果 + 整体评级（healthy/degraded/unhealthy）")
async def health() -> BaseResponse[Dict[str, Any]]:
    result = await get_health_with_components()
    return ok(result)
