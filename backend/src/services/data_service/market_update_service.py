from typing import Iterable


async def update_market(symbols: Iterable[str]) -> int:
    return len(list(symbols))
