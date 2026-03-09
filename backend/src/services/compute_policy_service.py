from datetime import datetime, time

from sqlalchemy.ext.asyncio import AsyncSession


class ComputePolicyService:
    def __init__(self, db: AsyncSession):
        self.db = db

    def is_off_peak_hours(self) -> bool:
        """Check if current time is off-peak (e.g., night time or weekends)"""
        now = datetime.now()
        # Weekend
        if now.weekday() >= 5: 
            return True
        # Night time (e.g., 18:00 - 08:00)
        current_time = now.time()
        if current_time >= time(18, 0) or current_time < time(8, 0):
            return True
        return False

    async def can_run_heavy_task(self, tenant_id: str, user_tier: str) -> bool:
        """Determine if a heavy compute task (like deep backtest) can run now."""
        if user_tier in ["vip"]:
            return True # VIPs can run anytime
            
        if user_tier == "pro":
            # Pros can run anytime but maybe lower priority (handled by queue)
            return True
            
        # Basic users restricted to off-peak for heavy tasks
        return self.is_off_peak_hours()
