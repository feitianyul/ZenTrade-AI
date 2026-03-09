import asyncio
from typing import Any, Dict, List

import httpx

PROBE_TARGETS = [
    {"name": "market_data_source", "url": "https://api.example.com/health", "timeout": 5},
    {"name": "trading_gateway", "url": "http://localhost:8000/system/health", "timeout": 2},
]

async def probe_target(target: Dict[str, Any]) -> Dict[str, Any]:
    url = target["url"]
    timeout = target.get("timeout", 5)
    name = target["name"]
    
    start_time = asyncio.get_event_loop().time()
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=timeout)
            duration = asyncio.get_event_loop().time() - start_time
            return {
                "name": name,
                "status": "up" if resp.status_code == 200 else "down",
                "latency_ms": int(duration * 1000),
                "code": resp.status_code
            }
    except Exception as e:
        return {
            "name": name,
            "status": "down",
            "error": str(e),
            "latency_ms": -1
        }

async def run_probes() -> List[Dict[str, Any]]:
    tasks = [probe_target(t) for t in PROBE_TARGETS]
    return await asyncio.gather(*tasks)
