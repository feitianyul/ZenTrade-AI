from sqlalchemy import JSON, Integer, String, select
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db import get_session, with_tenant
from src.models.base import BaseModel


class GrowthProfile(BaseModel):
    __tablename__ = "growth_profiles"
    
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    points: Mapped[int] = mapped_column(Integer, default=0)
    level: Mapped[int] = mapped_column(Integer, default=1)
    achievements: Mapped[dict] = mapped_column(JSON, default={})

async def get_growth_profile(tenant_id: str, user_id: str) -> dict:
    async for session in get_session():
        query = with_tenant(select(GrowthProfile), GrowthProfile, tenant_id).where(
            GrowthProfile.user_id == user_id
        )
        result = await session.execute(query)
        profile = result.scalar_one_or_none()
        
        if not profile:
            # Create default profile
            profile = GrowthProfile(
                tenant_id=tenant_id,
                user_id=user_id,
                points=0,
                level=1,
                achievements={}
            )
            session.add(profile)
            await session.commit()
            await session.refresh(profile)
            
        return {
            "points": profile.points,
            "level": profile.level,
            "achievements": profile.achievements
        }
    return {"points": 0, "level": 1, "achievements": {}}

async def add_growth_points(tenant_id: str, user_id: str, points: int, reason: str) -> dict:
    async for session in get_session():
        query = with_tenant(select(GrowthProfile), GrowthProfile, tenant_id).where(
            GrowthProfile.user_id == user_id
        )
        result = await session.execute(query)
        profile = result.scalar_one_or_none()
        
        if not profile:
            profile = GrowthProfile(
                tenant_id=tenant_id,
                user_id=user_id,
                points=0,
                level=1,
                achievements={}
            )
            session.add(profile)
        
        profile.points += points
        # Simple level up logic: every 100 points = 1 level
        profile.level = 1 + (profile.points // 100)
        
        await session.commit()
        await session.refresh(profile)
        return {
            "points": profile.points,
            "level": profile.level,
            "achievements": profile.achievements
        }
    return {}
