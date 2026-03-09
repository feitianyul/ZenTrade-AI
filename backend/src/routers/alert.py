from typing import List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_db
from src.schemas.alert import AlertCreate, AlertOut, AlertStatus
from src.schemas.response import BaseResponse, ok
from src.services.alert_service import create_alert, get_alerts, resolve_alert
from src.services.auth_service import verify_token

router = APIRouter(tags=["Alert"])


async def _require_user(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.replace("Bearer ", "")
    return await verify_token(token)


@router.post("/alerts", response_model=BaseResponse[AlertOut])
async def create_new_alert(
    alert_in: AlertCreate,
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_db),
) -> BaseResponse[AlertOut]:
    user = await _require_user(authorization)
    alert = await create_alert(session, user.tenant_id, alert_in)
    return ok(alert)


@router.get("/alerts", response_model=BaseResponse[List[AlertOut]])
async def list_alerts(
    status: Optional[AlertStatus] = None,
    limit: int = 50,
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_db),
) -> BaseResponse[List[AlertOut]]:
    user = await _require_user(authorization)
    alerts = await get_alerts(session, user.tenant_id, status, limit)
    return ok(alerts)


@router.put("/alerts/{alert_id}/resolve", response_model=BaseResponse[AlertOut])
async def resolve_existing_alert(
    alert_id: str,
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_db),
) -> BaseResponse[AlertOut]:
    user = await _require_user(authorization)
    alert = await resolve_alert(session, user.tenant_id, alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return ok(alert)
