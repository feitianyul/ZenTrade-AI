from typing import Any


def map_fields(source: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"source": source, **payload}
