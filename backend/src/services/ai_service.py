"""AI Service: Strategy parsing via real LLM with fallback to local mock."""

import hashlib
import json
import logging
import re
from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.strategy import LogicBlock, StrategyParseResult
from src.services.ai_config_service import AIConfigService
from src.services.llm_service.llm_router import LLMRouter

logger = logging.getLogger(__name__)

STRATEGY_SYSTEM_PROMPT = """你是一个专业的量化策略分析师。用户会用自然语言描述交易想法，你需要将其解析为结构化的策略配置。

请严格按以下JSON格式输出（不要输出其他内容）：
{
  "name": "策略名称",
  "logic_code": "Python策略代码",
  "params_json": {"source": "ai", "indicators": ["MA", "RSI"]},
  "logic_blocks": [
    {"id": "1", "type": "input", "label": "数据输入", "details": "日K线收盘价"},
    {"id": "2", "type": "condition", "label": "买入条件", "details": "MA5上穿MA20"},
    {"id": "3", "type": "action", "label": "执行动作", "details": "买入100%仓位"}
  ],
  "risk_hint": "风险提示内容"
}

注意：你的分析仅为工具性辅助，不构成任何投资建议。"""


async def get_llm_router(db: AsyncSession, tenant_id: str) -> Optional[LLMRouter]:
    """Build LLMRouter from saved configs."""
    service = AIConfigService(db)
    
    # Try multi-key config
    keys_cfg = await service.get_config(tenant_id, "llm_keys")
    params_cfg = await service.get_config(tenant_id, "llm_params")
    llm_params = params_cfg.value if params_cfg else {}

    if keys_cfg and keys_cfg.value.get("keys"):
        return LLMRouter(keys_cfg.value, llm_params)

    # Fallback: legacy single-key config
    api_key_cfg = await service.get_config(tenant_id, "llm_api_key")
    endpoint_cfg = await service.get_config(tenant_id, "llm_endpoint")
    model_cfg = await service.get_config(tenant_id, "default_model")

    api_key = (api_key_cfg.value if api_key_cfg else {}).get("v", "")
    endpoint = (endpoint_cfg.value if endpoint_cfg else {}).get("v", "")
    model = (model_cfg.value if model_cfg else {}).get("v", "")

    if not api_key or not endpoint:
        return None

    legacy_config = {
        "keys": [{"label": "默认", "provider": "legacy", "api_key": api_key,
                   "endpoint": endpoint, "model": model, "role": "primary", "enabled": True}],
        "strategy": "primary_backup",
        "default_model": model or "gpt-4o-mini",
    }
    return LLMRouter(legacy_config, llm_params)


async def get_agent_prompt(db: AsyncSession, tenant_id: str, agent: str, role: str = "beginner") -> str:
    """Get custom system prompt for an agent, falling back to default."""
    service = AIConfigService(db)
    key = f"prompt_{agent}_{role}"
    cfg = await service.get_config(tenant_id, key)
    if cfg and cfg.value.get("system_prompt"):
        return cfg.value["system_prompt"]
    return STRATEGY_SYSTEM_PROMPT


async def parse_strategy_prompt(
    prompt: str,
    db: AsyncSession = None,
    tenant_id: str = "public",
    user_role: str = "beginner",
) -> StrategyParseResult:
    """Parse natural language strategy via LLM, with local fallback."""

    # Try real LLM if db is available
    if db:
        router = await get_llm_router(db, tenant_id)
        if router:
            system_prompt = await get_agent_prompt(db, tenant_id, "strategy_parse", user_role)
            result = await router.chat(
                messages=[{"role": "user", "content": prompt}],
                system_prompt=system_prompt,
            )
            if result.get("error") is None:
                parsed = _parse_llm_response(result.get("content", ""), prompt)
                if parsed:
                    logger.info("Strategy parsed via LLM (model=%s, latency=%dms)",
                                result.get("model"), result.get("latency_ms", 0))
                    return parsed
                else:
                    logger.warning("LLM returned non-parseable response, falling back to mock")
            else:
                logger.warning("LLM request failed: %s, falling back to mock", result.get("message"))

    # Fallback: local mock
    return _mock_parse(prompt)


def _parse_llm_response(content: str, original_prompt: str) -> Optional[StrategyParseResult]:
    """Try to parse LLM JSON response into StrategyParseResult."""
    try:
        # Extract JSON from markdown code blocks if present
        json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', content, re.DOTALL)
        if json_match:
            content = json_match.group(1)
        
        # Try direct JSON parse
        data = json.loads(content.strip())
        
        logic_blocks = []
        for b in data.get("logic_blocks", []):
            logic_blocks.append(LogicBlock(
                id=str(b.get("id", "")),
                type=b.get("type", "condition"),
                label=b.get("label", ""),
                details=b.get("details"),
            ))
        
        if not logic_blocks:
            logic_blocks = [LogicBlock(id="1", type="condition", label="AI 解析结果", details=content[:200])]

        return StrategyParseResult(
            name=data.get("name", f"AI策略-{original_prompt[:10]}"),
            logic_code=data.get("logic_code", "# AI generated\npass"),
            params_json=data.get("params_json", {"source": "ai"}),
            logic_blocks=logic_blocks,
            risk_hint=data.get("risk_hint", "【智能辅助提示】本内容仅为工具性分析，不构成任何投资建议，投资决策请谨慎。"),
        )
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        logger.warning("Failed to parse LLM JSON: %s", e)
        # Try to extract useful content even if not valid JSON
        if content and len(content) > 20:
            return StrategyParseResult(
                name=f"AI策略-{original_prompt[:10]}",
                logic_code=f"# AI 分析结果\n# {content[:500]}",
                params_json={"source": "ai", "raw_response": True},
                logic_blocks=[LogicBlock(id="1", type="condition", label="AI 分析", details=content[:300])],
                risk_hint="【智能辅助提示】本内容仅为工具性分析，不构成任何投资建议。",
            )
        return None


def _mock_parse(prompt: str) -> StrategyParseResult:
    """Local mock fallback when LLM is unavailable.
    
    TODO: 配置 LLM API Key 后走真实解析。此 Mock 仅在无可用 LLM 时触发，
    返回模板策略并提示用户配置 LLM。
    """
    digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:8]
    name = f"策略-{digest}"
    logic_code = (
        "class GeneratedStrategy:\n"
        "    def init(self):\n"
        "        self.ma5 = self.I(SMA, self.data.Close, 5)\n"
        "        self.ma20 = self.I(SMA, self.data.Close, 20)\n"
        "        self.rsi = self.I(RSI, self.data.Close, 14)\n"
        "\n"
        "    def next(self):\n"
        "        if self.ma5 > self.ma20 and self.rsi < 70:\n"
        "            if not self.position:\n"
        "                self.buy()\n"
        "        elif self.ma5 < self.ma20:\n"
        "            if self.position:\n"
        "                self.sell()\n"
    )
    logic_blocks = [
        LogicBlock(id="1", type="input", label="Market Data", details="Daily Close Price"),
        LogicBlock(id="2", type="condition", label="Moving Average Crossover", details="MA5 > MA20"),
        LogicBlock(id="3", type="condition", label="RSI Filter", details="RSI < 70"),
        LogicBlock(id="4", type="action", label="Buy Signal", details="Position: 100%"),
    ]
    return StrategyParseResult(
        name=name,
        logic_code=logic_code,
        params_json={"source": "mock", "prompt_hash": digest},
        logic_blocks=logic_blocks,
        risk_hint="⚠️ 当前使用本地 Mock（AI 未配置），请管理员配置 LLM 后重试。",
    )
