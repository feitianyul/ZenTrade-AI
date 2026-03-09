import json
import os
from typing import AsyncGenerator

import httpx


class PandaLLMClient:
    def __init__(
        self,
        api_key: str = None,
        base_url: str = "https://api.panda.ai/v1",
    ):
        self.api_key = api_key or os.getenv("PANDA_API_KEY")
        self.base_url = base_url

    async def chat_completion_stream(
        self,
        messages: list,
        model: str = "panda-7b",
    ) -> AsyncGenerator[str, None]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": model,
            "messages": messages,
            "stream": True
        }
        
        async with httpx.AsyncClient() as client:
            # TODO: 接入 Panda 生态后配置 PANDA_API_KEY 环境变量
            if not self.api_key:
                yield "[错误] Panda API Key 未配置。请在管理后台配置中心设置 PANDA_API_KEY。\n"
                return

            try:
                async with client.stream(
                    "POST",
                    f"{self.base_url}/chat/completions",
                    json=payload,
                    headers=headers,
                ) as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if line.startswith("data: "):
                            data_str = line[6:]
                            if data_str == "[DONE]":
                                break
                            try:
                                data = json.loads(data_str)
                                content = data["choices"][0]["delta"].get("content", "")
                                if content:
                                    yield content
                            except json.JSONDecodeError:
                                pass
            except Exception as e:
                yield f"Error calling Panda LLM: {str(e)}"
