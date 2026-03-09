from __future__ import annotations

import json
from typing import Any

from src.core.db import get_session
from src.core.errors import ValidationError
from src.core.file_limits import ensure_import_limits
from src.services.ai_config_service import AIConfigService
from src.services.audit_service import write_audit_log


def _parse_knowledge_items(content: bytes) -> list[dict[str, Any]]:
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValidationError("invalid json") from exc
    if isinstance(payload, dict) and "items" in payload:
        items = payload.get("items")
    else:
        items = payload
    if not isinstance(items, list):
        raise ValidationError("knowledge payload must be a list")
    normalized = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("title") or "").strip()
        content_text = str(item.get("content") or "").strip()
        weight = item.get("weight", 1.0)
        if not name or not content_text:
            continue
        normalized.append({"name": name, "content": content_text, "weight": weight})
    return normalized


async def import_knowledge(
    tenant_id: str,
    user_id: str,
    filename: str,
    content: bytes,
) -> dict[str, Any]:
    ensure_import_limits(filename, len(content), {".json"})
    items = _parse_knowledge_items(content)
    if not items:
        raise ValidationError("no valid knowledge items")

    async for session in get_session():
        service = AIConfigService(session)
        config = await service.set_config(
            tenant_id,
            "knowledge_import",
            {"items": items, "source": filename},
            description=f"import {filename}",
        )
        break

    await write_audit_log(
        tenant_id=tenant_id,
        actor_id=user_id,
        action="import_knowledge",
        resource_type="ai_config",
        resource_id="knowledge_import",
        status="success",
        ip_address="unknown",
        user_agent="unknown",
        detail=json.dumps({"imported_count": len(items)}, ensure_ascii=False),
    )

    return {
        "config_key": config.key,
        "version": config.version,
        "imported_count": len(items),
    }
