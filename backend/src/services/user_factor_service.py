from typing import Any, Dict, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.user_factor import UserFactor


def _to_dict(f: UserFactor) -> Dict[str, Any]:
    return {
        "id": f.id,
        "tenant_id": f.tenant_id,
        "name": f.name,
        "code": f.code,
        "description": f.description,
        "created_at": f.created_at.isoformat() if f.created_at else None,
    }


class UserFactorService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_factor(self, tenant_id: str, name: str, code: str, desc: str) -> Dict[str, Any]:
        factor = UserFactor(
            tenant_id=tenant_id,
            name=name,
            code=code,
            description=desc,
        )
        self.db.add(factor)
        await self.db.commit()
        await self.db.refresh(factor)
        return _to_dict(factor)

    async def get_my_factors(self, tenant_id: str) -> List[Dict[str, Any]]:
        stmt = select(UserFactor).where(UserFactor.tenant_id == tenant_id)
        result = await self.db.execute(stmt)
        return [_to_dict(f) for f in result.scalars().all()]
