from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.alert import Alert
from src.schemas.alert import AlertCreate, AlertStatus


def _alert_to_dict(alert: Alert) -> Dict[str, Any]:
    return {
        "id": alert.id,
        "tenant_id": alert.tenant_id,
        "title": alert.title,
        "message": alert.message,
        "level": alert.level,
        "status": alert.status,
        "source": alert.source,
        "metadata": alert.metadata_info,
        "created_at": alert.created_at,
        "updated_at": alert.updated_at,
        "resolved_at": alert.resolved_at,
    }


async def create_alert(session: AsyncSession, tenant_id: str, alert_in: AlertCreate) -> Dict[str, Any]:
    alert = Alert(
        tenant_id=tenant_id,
        title=alert_in.title,
        message=alert_in.message,
        level=alert_in.level,
        source=alert_in.source,
        metadata_info=alert_in.metadata,
        status=AlertStatus.ACTIVE,
    )
    session.add(alert)
    await session.commit()
    await session.refresh(alert)
    return _alert_to_dict(alert)


async def get_alerts(
    session: AsyncSession,
    tenant_id: str,
    status: Optional[AlertStatus] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    query = select(Alert).where(Alert.tenant_id == tenant_id)
    if status:
        # 前端传 pending 表示“未处理”，与 active 等价
        filter_status = AlertStatus.ACTIVE if status == AlertStatus.PENDING else status
        query = query.where(Alert.status == filter_status)
    query = query.order_by(Alert.created_at.desc()).limit(limit)
    result = await session.execute(query)
    return [_alert_to_dict(a) for a in result.scalars().all()]


async def resolve_alert(session: AsyncSession, tenant_id: str, alert_id: str) -> Optional[Dict[str, Any]]:
    query = select(Alert).where(Alert.tenant_id == tenant_id, Alert.id == alert_id)
    result = await session.execute(query)
    alert = result.scalar_one_or_none()
    if alert:
        alert.status = AlertStatus.RESOLVED
        alert.resolved_at = datetime.utcnow()
        await session.commit()
        await session.refresh(alert)
        return _alert_to_dict(alert)
    return None
