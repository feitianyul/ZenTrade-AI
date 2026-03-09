from typing import Any, Dict

from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.masking import DataRightRequest


async def export_user_data(session: AsyncSession, user_id: int) -> Dict[str, Any]:
    # In a real implementation, this would query multiple tables
    # For now, return a structure
    return {
        "user_id": user_id,
        "profile": {"name": "Masked", "email": "m***@example.com"},
        "trades": [],
        "strategies": [],
        "exported_at": "2024-01-01T00:00:00Z",
    }

async def delete_user_account(
    session: AsyncSession,
    user_id: int,
    reason: str,
) -> bool:
    # Soft delete logic would go here
    # e.g., update users set is_deleted=1 where id=user_id
    # For prototype, we just return True
    return True

async def process_rights_request(
    session: AsyncSession,
    user_id: int,
    request: DataRightRequest,
) -> Dict[str, Any]:
    if request.request_type == "export":
        data = await export_user_data(session, user_id)
        return {"status": "completed", "data": data}
    elif request.request_type == "delete":
        success = await delete_user_account(
            session,
            user_id,
            request.reason or "user_request",
        )
        return {"status": "completed" if success else "failed"}
    return {"status": "invalid_type"}
