from typing import Any, Dict, List, Optional
import time
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.auth import get_current_user
from src.core.db import get_db
from src.services.ai_config_service import AIConfigService
from src.schemas.response import BaseResponse, ok

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ai-config", tags=["AI Config"])

class ConfigSetRequest(BaseModel):
    key: str
    value: Dict[str, Any]
    description: str = None

class ConfigResponse(BaseModel):
    key: str
    value: Dict[str, Any]
    version: int
    description: str = None

async def _require_ai_admin(current_user=Depends(get_current_user)):
    from src.services.permission_service import is_admin
    if not await is_admin(current_user.tenant_id, current_user.user_id):
        raise HTTPException(status_code=403, detail="admin access required for AI config")
    return current_user

@router.get("/", response_model=List[ConfigResponse])
async def list_configs(
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """列出当前租户的 AI 配置（只读，无需管理员权限）"""
    service = AIConfigService(db)
    configs = await service.list_configs(current_user.tenant_id)
    return [
        ConfigResponse(
            key=c.key,
            value=c.value,
            version=c.version,
            description=c.description,
        )
        for c in configs
    ]

@router.get("/{key}", response_model=ConfigResponse)
async def get_config(
    key: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    service = AIConfigService(db)
    config = await service.get_config(current_user.tenant_id, key)
    if not config:
        raise HTTPException(status_code=404, detail="Config not found")
    return ConfigResponse(
        key=config.key,
        value=config.value,
        version=config.version,
        description=config.description,
    )

@router.post("/", response_model=ConfigResponse)
async def set_config(
    req: ConfigSetRequest,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    # Only admin or specific role? For now, any auth user
    service = AIConfigService(db)
    config = await service.set_config(
        current_user.tenant_id,
        req.key,
        req.value,
        req.description,
    )
    return ConfigResponse(
        key=config.key,
        value=config.value,
        version=config.version,
        description=config.description,
    )


@router.delete("/{key}")
async def delete_config(
    key: str,
    current_user: dict = Depends(_require_ai_admin),
    db: AsyncSession = Depends(get_db),
):
    service = AIConfigService(db)
    config = await service.get_config(current_user.tenant_id, key)
    if not config:
        raise HTTPException(status_code=404, detail="Config not found")
    await service.delete_config(current_user.tenant_id, key)
    return ok({"status": "deleted", "key": key})


class FetchModelsRequest(BaseModel):
    provider: str = "custom"
    endpoint: str
    api_key: str


# --- 供应商分类 ---
# A类: OpenAI 兼容 GET {endpoint}/models，Bearer token
_OPENAI_COMPAT_PROVIDERS = {
    "openai", "deepseek", "moonshot", "siliconflow",
    "together", "groq", "openrouter", "custom",
}
# B类: Azure OpenAI，api-key Header + api-version 参数
_AZURE_PROVIDERS = {"azure"}
# C类: 不支持远程 /models，前端使用本地静态列表
_FALLBACK_PROVIDERS = {"qwen", "bailian", "wenxin", "doubao"}
# D类: 先尝试远程获取，失败则由前端 fallback 到静态列表
_TRY_THEN_FALLBACK = {"zhipu"}


@router.post("/fetch-models",
             summary="获取供应商可用模型列表",
             description="根据供应商类型代理请求 /models 端点，返回可用模型列表。"
                         "A类(OpenAI兼容)直接请求；B类(Azure)使用 api-key Header；"
                         "C类(通义千问/文心/豆包)返回 source=static 由前端使用预置列表。")
async def fetch_models(
    req: FetchModelsRequest,
    current_user: dict = Depends(get_current_user),
):
    import httpx

    if not req.endpoint or not req.api_key:
        raise HTTPException(status_code=400, detail="endpoint 和 api_key 不能为空")

    provider = req.provider.lower().strip()

    # C类: 不支持远程获取，直接返回 static
    if provider in _FALLBACK_PROVIDERS:
        return ok({"source": "static", "models": []})

    url = f"{req.endpoint.rstrip('/')}/models"
    headers: Dict[str, str] = {}
    params: Dict[str, str] = {}

    # B类: Azure OpenAI
    if provider in _AZURE_PROVIDERS:
        headers["api-key"] = req.api_key
        params["api-version"] = "2024-10-21"
    else:
        # A类 + D类: Bearer token
        headers["Authorization"] = f"Bearer {req.api_key}"

    # 硅基流动: 支持类型过滤
    if provider == "siliconflow":
        params["type"] = "text"
        params["sub_type"] = "chat"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers=headers, params=params)

        if resp.status_code != 200:
            # D类: 尝试失败则 fallback
            if provider in _TRY_THEN_FALLBACK:
                logger.info("fetch-models try-then-fallback for %s: HTTP %s", provider, resp.status_code)
                return ok({"source": "static", "models": []})
            detail = resp.text[:300]
            logger.warning("fetch-models failed: %s %s", resp.status_code, detail)
            raise HTTPException(
                status_code=502,
                detail=f"供应商返回 HTTP {resp.status_code}: {detail}",
            )

        data = resp.json().get("data", [])
        models = [
            {"id": m["id"], "owned_by": m.get("owned_by", "")}
            for m in data
            if isinstance(m, dict) and "id" in m
        ]
        return ok({"source": "remote", "models": models})

    except httpx.RequestError as exc:
        # D类: 网络失败也 fallback
        if provider in _TRY_THEN_FALLBACK:
            logger.info("fetch-models try-then-fallback network error for %s: %s", provider, exc)
            return ok({"source": "static", "models": []})
        logger.exception("fetch-models request error")
        raise HTTPException(status_code=502, detail=f"请求失败: {str(exc)[:200]}")


class TestConnectionRequest(BaseModel):
    key_index: int = 0  # which key in llm_keys.keys[] to test
    verbose: bool = False  # 是否返回详细日志

class VerboseLogEntry(BaseModel):
    msg: str
    data: Optional[Any] = None

class TestConnectionResponse(BaseModel):
    success: bool
    latency_ms: int = 0
    model: str = ""
    error: str = ""
    verbose_log: Optional[List[VerboseLogEntry]] = None
    tenant_id: Optional[str] = None  # 失败时便于核对当前租户是否与预期一致


@router.post("/test-connection", response_model=BaseResponse[TestConnectionResponse],
             summary="测试 LLM 连通性",
             description="支持多 Key：传入 key_index 指定测试哪个 Key，"
                         "也兼容旧的单 Key 模式（读取 llm_api_key）。"
                         "verbose=true 时返回详细的请求/响应日志。")
async def test_connection(
    req: TestConnectionRequest = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    import httpx

    service = AIConfigService(db)
    tid = getattr(current_user, "tenant_id", None) or (current_user.get("tenant_id") if isinstance(current_user, dict) else None) or ""
    raw_key_index = req.key_index if req is not None else 0
    key_index = int(raw_key_index) if raw_key_index is not None else 0
    verbose = bool(req.verbose if req else False)
    vlog: List[Dict] = []

    # Try multi-key config first
    keys_cfg = await service.get_config(tid, "llm_keys")
    api_key = endpoint = model = ""

    if keys_cfg and keys_cfg.value.get("keys"):
        key_list = keys_cfg.value["keys"]
        if 0 <= key_index < len(key_list):
            k = key_list[key_index]
            api_key = k.get("api_key", "")
            endpoint = k.get("endpoint", "")
            model = k.get("model", "") or keys_cfg.value.get("default_model", "")
            logger.info(
                "test_connection: tenant_id=%s key_index=%s endpoint=%s model=%s keys_count=%s",
                tid, key_index, (endpoint or "")[:60], model, len(key_list),
            )
        else:
            logger.warning("test_connection: tenant_id=%s key_index=%s out of range (keys_count=%s)", tid, key_index, len(key_list))
    
    # Fallback to legacy single-key config
    if not api_key:
        api_key_cfg = await service.get_config(tid, "llm_api_key")
        endpoint_cfg = await service.get_config(tid, "llm_endpoint")
        model_cfg = await service.get_config(tid, "default_model")
        api_key = (api_key_cfg.value if api_key_cfg else {}).get("v", "")
        endpoint = (endpoint_cfg.value if endpoint_cfg else {}).get("v", "")
        model = (model_cfg.value if model_cfg else {}).get("v", "")

    if not api_key or not endpoint:
        return ok(TestConnectionResponse(
            success=False,
            error="未配置 API Key 或端点地址，请先保存模型配置",
            tenant_id=tid or None,
        ))

    url = endpoint.rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    body = {"model": model or "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 5}

    if verbose:
        masked_key = api_key[:4] + "..." + api_key[-4:] if len(api_key) >= 8 else "***"
        vlog.append({"msg": f"Request URL: POST {url}"})
        vlog.append({"msg": f"Authorization: Bearer {masked_key}"})
        vlog.append({"msg": f"Request Body", "data": body})

    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=body, headers=headers)
        latency = int((time.time() - t0) * 1000)

        if verbose:
            vlog.append({"msg": f"Response Status: {resp.status_code}"})
            vlog.append({"msg": f"Response Body (truncated)", "data": resp.text[:500]})

        if resp.status_code == 200:
            data = resp.json()
            return ok(TestConnectionResponse(
                success=True, latency_ms=latency, model=data.get("model", model),
                verbose_log=vlog if verbose else None,
            ))
        else:
            detail = resp.text[:500] if resp.text else "(empty body)"
            logger.warning(
                "LLM test-connection failed: %s tenant_id=%s key_index=%s body=%s",
                resp.status_code, tid, key_index, detail,
            )
            # 失败时始终带上 LLM 响应摘要，便于排查 400 等原因
            if not verbose:
                vlog.append({"msg": f"Response Status: {resp.status_code}"})
                vlog.append({"msg": "Response Body (LLM 返回)", "data": resp.text[:500] if resp.text else "(empty)"})
            hint = ""
            if resp.status_code == 400 and "openrouter" in (endpoint or "").lower():
                hint = " OpenRouter 400 常见原因：模型 ID 错误或已变更，请核对模型接入页的「模型」字段与 OpenRouter 文档一致。"
            return ok(TestConnectionResponse(
                success=False, latency_ms=latency, error=f"HTTP {resp.status_code}: {detail}{hint}",
                verbose_log=vlog,
                tenant_id=tid or None,
            ))
    except Exception as exc:
        latency = int((time.time() - t0) * 1000)
        logger.exception("LLM test-connection exception")
        if verbose:
            vlog.append({"msg": f"Exception: {str(exc)[:300]}"})
        return ok(TestConnectionResponse(
            success=False, latency_ms=latency, error=str(exc)[:300],
            verbose_log=vlog if verbose else None,
            tenant_id=tid or None,
        ))
