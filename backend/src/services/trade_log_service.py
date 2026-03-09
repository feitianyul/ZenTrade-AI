from typing import Any


async def write_trade_log(payload: dict[str, Any]) -> dict[str, Any]:
    return {"log_id": "log_" + str(payload.get("order_id", "default")), "payload": payload}
