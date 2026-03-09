from typing import Any


async def export_payload(export_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "export_type": export_type,
        "status": "ready",
        "payload": payload,
    }
