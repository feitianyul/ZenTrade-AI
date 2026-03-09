"""LLM Router: Multi-key routing with failover, round-robin, weighted strategies."""

import logging
import random
import time
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


class LLMRouter:
    """Routes LLM requests across multiple API keys with configurable strategy."""

    def __init__(self, keys_config: Dict[str, Any], llm_params: Optional[Dict] = None):
        self.keys = keys_config.get("keys", [])
        self.strategy = keys_config.get("strategy", "primary_backup")
        self.default_model = keys_config.get("default_model", "gpt-4o-mini")
        self.params = llm_params or {}
        self._rr_index = 0  # round-robin counter

    def _active_keys(self) -> List[Dict]:
        return [k for k in self.keys if k.get("enabled", True) is not False]

    def _select_key(self, model: str = None) -> Optional[Dict]:
        active = self._active_keys()
        if not active:
            return None

        if self.strategy == "by_model" and model:
            for k in active:
                if k.get("model") == model:
                    return k

        if self.strategy == "round_robin":
            k = active[self._rr_index % len(active)]
            self._rr_index += 1
            return k

        if self.strategy == "weighted":
            total = sum(k.get("weight", 100) for k in active)
            r = random.uniform(0, total)
            cum = 0
            for k in active:
                cum += k.get("weight", 100)
                if r <= cum:
                    return k
            return active[-1]

        # primary_backup (default): try primary first
        primaries = [k for k in active if k.get("role") != "backup"]
        if primaries:
            return primaries[0]
        return active[0]

    def _get_failover_keys(self, failed_key: Dict) -> List[Dict]:
        active = self._active_keys()
        return [k for k in active if k is not failed_key]

    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: str = None,
        system_prompt: str = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Send a chat completion request with automatic failover."""
        use_model = model or self.default_model
        key = self._select_key(use_model)
        if not key:
            return {"error": "no_key", "message": "未配置可用的 API Key"}

        # Build messages with system prompt
        full_messages = []
        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})
        full_messages.extend(messages)

        # Merge params
        temperature = kwargs.get("temperature", self.params.get("temperature", 0.7))
        max_tokens = kwargs.get("max_tokens", self.params.get("max_tokens", 2048))
        top_p = kwargs.get("top_p", self.params.get("top_p", 0.9))
        timeout = self.params.get("request_timeout_sec", 60)

        # Try primary key, then failover
        tried_keys = []
        current_key = key
        while current_key:
            tried_keys.append(current_key)
            result = await self._call_key(
                current_key, full_messages, use_model, temperature, max_tokens, top_p, timeout
            )
            if result.get("error") is None:
                return result
            # Failover
            remaining = [k for k in self._active_keys() if k not in tried_keys]
            current_key = remaining[0] if remaining else None
            if current_key:
                logger.warning("LLM failover from %s to %s", tried_keys[-1].get("label"), current_key.get("label"))

        last_err = result.get("message", "未知错误") if result else "未知错误"
        return {"error": "all_failed", "message": f"所有 API Key 均请求失败 (最后错误: {str(last_err)[:150]})", "tried": len(tried_keys)}

    async def _call_key(
        self, key: Dict, messages: List, model: str,
        temperature: float, max_tokens: int, top_p: float, timeout: int,
    ) -> Dict[str, Any]:
        endpoint = key.get("endpoint", "").rstrip("/")
        api_key = key.get("api_key", "")
        use_model = key.get("model") or model

        url = f"{endpoint}/chat/completions"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        body = {
            "model": use_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "top_p": top_p,
        }

        t0 = time.time()
        try:
            async with httpx.AsyncClient(timeout=float(timeout)) as client:
                resp = await client.post(url, json=body, headers=headers)
            latency_ms = int((time.time() - t0) * 1000)

            if resp.status_code == 200:
                data = resp.json()
                content = ""
                choices = data.get("choices", [])
                if choices:
                    content = choices[0].get("message", {}).get("content", "")
                usage = data.get("usage", {})
                return {
                    "content": content,
                    "model": data.get("model", use_model),
                    "usage": usage,
                    "latency_ms": latency_ms,
                    "key_label": key.get("label", ""),
                }
            else:
                logger.warning("LLM call failed: %s %s", resp.status_code, resp.text[:200])
                return {"error": f"HTTP {resp.status_code}", "message": resp.text[:300]}
        except Exception as exc:
            logger.exception("LLM call exception for key %s", key.get("label"))
            return {"error": "exception", "message": str(exc)[:300]}
