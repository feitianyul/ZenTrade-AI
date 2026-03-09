import asyncio
from datetime import datetime

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/ws/push")
async def push_channel(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        await websocket.send_json(
            {"type": "connected", "timestamp": datetime.utcnow().isoformat()}
        )
        for index in range(3):
            await websocket.send_json(
                {
                    "type": "order_status",
                    "payload": {"status": "submitted", "sequence": index + 1},
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )
            await asyncio.sleep(0.2)
        await websocket.send_json(
            {"type": "position_update", "payload": {"count": 1}}
        )
        await websocket.send_json(
            {
                "type": "market_update",
                "payload": {"symbol": "000001.SZ", "price": 10.2},
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
        await websocket.send_json(
            {
                "type": "trade_update",
                "payload": {"order_id": "demo", "status": "filled"},
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
    except WebSocketDisconnect:
        return
