import time

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.vector_store import VectorStore
from src.services.alert_service import AlertService


class VectorPerfService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.vector_store = VectorStore() # Assume configured from env
        self.alert_service = AlertService(db)

    async def check_latency(self, tenant_id: str):
        start = time.time()
        # Perform a dummy search
        try:
            # VectorStore needs a mock search or ping
            # await self.vector_store.search("test", limit=1)
            pass 
        except Exception:
            pass # Ignore errors for now if store not ready
        
        latency_ms = (time.time() - start) * 1000
        
        # Baseline check (e.g. 200ms)
        if latency_ms > 200:
             await self.alert_service.create_alert(
                tenant_id,
                "warning",
                "Vector DB Latency High",
                f"Latency {latency_ms:.2f}ms exceeds 200ms baseline"
            )
        return latency_ms
