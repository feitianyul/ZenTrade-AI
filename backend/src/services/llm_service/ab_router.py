from typing import Any, Dict

from src.services.llm_service.runtime_manager import manager


async def route_request(user_id: str, prompt: str) -> Dict[str, Any]:
    # Simple consistent hashing or user-based routing for A/B testing
    # For now, just use weighted random from manager
    runtime = manager.select_runtime_weighted()
    if not runtime:
        return {"error": "No runtime available"}
        
    return {
        "model_id": runtime.model_id,
        "endpoint": runtime.endpoint,
        "ab_group": "A" if runtime.model_id == "panda-v1" else "B"
    }
