"""T228 - 推理参数配置与同步监控"""

from typing import Any, Optional

# 默认推理参数
DEFAULT_LLM_PARAMS = {
    "temperature": 0.7,
    "max_tokens": 2048,
    "top_p": 0.9,
    "frequency_penalty": 0.0,
    "presence_penalty": 0.0,
    "stop": None,
}

_tenant_params: dict[str, dict[str, Any]] = {}


async def get_params(tenant_id: str) -> dict[str, Any]:
    """获取推理参数"""
    return _tenant_params.get(tenant_id, DEFAULT_LLM_PARAMS.copy())


async def update_params(
    tenant_id: str, params: dict[str, Any]
) -> dict[str, Any]:
    """更新推理参数"""
    current = _tenant_params.get(tenant_id, DEFAULT_LLM_PARAMS.copy())
    for key in DEFAULT_LLM_PARAMS:
        if key in params:
            current[key] = params[key]
    _tenant_params[tenant_id] = current
    return current


async def reset_params(tenant_id: str) -> dict[str, Any]:
    """重置为默认参数"""
    _tenant_params[tenant_id] = DEFAULT_LLM_PARAMS.copy()
    return _tenant_params[tenant_id]


async def validate_params(params: dict[str, Any]) -> dict[str, Any]:
    """校验推理参数"""
    errors = []
    if "temperature" in params:
        t = params["temperature"]
        if not (0 <= t <= 2):
            errors.append("temperature 应在 0-2 之间")
    if "max_tokens" in params:
        if params["max_tokens"] < 1 or params["max_tokens"] > 32768:
            errors.append("max_tokens 应在 1-32768 之间")
    if "top_p" in params:
        if not (0 <= params["top_p"] <= 1):
            errors.append("top_p 应在 0-1 之间")
    return {"valid": len(errors) == 0, "errors": errors}
