from typing import AsyncGenerator

from fastapi.responses import StreamingResponse


class SSEBridge:
    @staticmethod
    def format_sse(data: str, event: str = None) -> str:
        msg = f"data: {data}\n\n"
        if event:
            msg = f"event: {event}\n{msg}"
        return msg

    @staticmethod
    async def stream_generator(generator: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
        async for chunk in generator:
            yield SSEBridge.format_sse(chunk)
            
    @staticmethod
    def create_response(generator: AsyncGenerator[str, None]) -> StreamingResponse:
        return StreamingResponse(
            SSEBridge.stream_generator(generator),
            media_type="text/event-stream"
        )
