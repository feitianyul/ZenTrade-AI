from typing import Any


async def start_strategy(payload: dict[str, Any]) -> dict[str, Any]:
    return {"runtime_id": "rt_" + str(payload.get("strategy_id", "default")), "status": "running"}


async def stop_strategy(runtime_id: str) -> dict[str, Any]:
    return {"runtime_id": runtime_id, "status": "stopped"}
