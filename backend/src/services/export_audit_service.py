from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import Optional

from src.core.db import get_session
from src.core.errors import ValidationError
from src.core.file_limits import ensure_export_limits
from src.core.time_util import now_beijing, utc_to_beijing_str
from src.services.audit_service import write_audit_log
from src.services.log_service import get_audit_logs


async def export_audit_logs(
    tenant_id: str,
    user_id: str,
    actor_id: Optional[str] = None,
    action: Optional[str] = None,
    limit: int = 1000,
) -> dict[str, object]:
    if limit <= 0 or limit > 5000:
        raise ValidationError("invalid export limit")
    async for session in get_session():
        logs = await get_audit_logs(session, tenant_id, actor_id, action, limit, 0)
        break

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "id",
            "actor_id",
            "action",
            "resource_type",
            "resource_id",
            "status",
            "ip_address",
            "user_agent",
            "detail",
            "created_at",
        ]
    )
    for log in logs:
        writer.writerow(
            [
                log.id,
                log.actor_id,
                log.action,
                log.resource_type,
                log.resource_id,
                log.status,
                log.ip_address,
                log.user_agent,
                log.detail,
                utc_to_beijing_str(log.created_at) or "",
            ]
        )
    payload = output.getvalue().encode("utf-8")
    ensure_export_limits(len(payload))
    timestamp = now_beijing().strftime("%Y%m%d%H%M%S")
    filename = f"audit_logs_{timestamp}.csv"

    await write_audit_log(
        tenant_id=tenant_id,
        actor_id=user_id,
        action="export_audit_logs",
        resource_type="audit_log",
        resource_id="batch",
        status="success",
        ip_address="unknown",
        user_agent="unknown",
        detail=json.dumps({"exported_count": len(logs)}, ensure_ascii=False),
    )

    return {
        "filename": filename,
        "content_type": "text/csv",
        "payload": payload,
    }
