from typing import Any


async def detect_anomaly(quote: dict[str, Any]) -> bool:
    change = float(quote.get("change", 0))
    return abs(change) > 5.0
