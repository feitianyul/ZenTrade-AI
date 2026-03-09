"""AI 个股分析：数据聚合 + 四维 Subagent + 主控 Orchestrator。"""
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.ai_analysis import (
    AiAnalysisResult,
    DimensionOut,
    MainOut,
    SuggestOut,
    ZoneOut,
)
from src.services.audit_service import write_audit_log
from src.services.ai_config_service import AIConfigService
from src.services.data_service.market_read_service import (
    get_capital_flow_data,
    get_irm_qa_by_symbol,
    get_peer_comparison_data,
)
from src.services.data_service.market_source_service import (
    fetch_fundamental,
    fetch_kline,
    fetch_news,
)

logger = logging.getLogger(__name__)

# 兜底结果：LLM 未配置或调用失败时返回
def _fallback_result(symbol: str, reason: str = "分析暂时不可用") -> AiAnalysisResult:
    return AiAnalysisResult(
        dimensions=[
            DimensionOut(name="技术面", direction="中性", strength=50, summary="数据或模型不可用"),
            DimensionOut(name="资金面", direction="中性", strength=50, summary="数据或模型不可用"),
            DimensionOut(name="基本面", direction="中性", strength=50, summary="数据或模型不可用"),
            DimensionOut(name="舆情", direction="中性", strength=50, summary="数据或模型不可用"),
        ],
        main=MainOut(
            direction="中性",
            score=50,
            confidence=0,
            summary=reason,
        ),
        zones=[
            ZoneOut(id="aggressive", label="激进区", desc="高风险高收益", action="暂不建议"),
            ZoneOut(id="balanced", label="平衡区", desc="中等风险收益", action="观望"),
            ZoneOut(id="stable", label="稳定区", desc="低波动稳健", action="观望"),
        ],
        suggest=SuggestOut(buy=False, text=reason),
    )


async def _get_llm_config(db: AsyncSession, tenant_id: str) -> Tuple[str, str, str]:
    """获取当前租户 LLM 配置：api_key, endpoint, model。与 ai_config test_connection 一致。"""
    service = AIConfigService(db)
    api_key = endpoint = model = ""

    keys_cfg = await service.get_config(tenant_id, "llm_keys")
    if keys_cfg and keys_cfg.value.get("keys"):
        key_list = keys_cfg.value["keys"]
        if key_list:
            k = key_list[0]
            api_key = k.get("api_key", "")
            endpoint = k.get("endpoint", "")
            model = k.get("model", "") or keys_cfg.value.get("default_model", "")

    if not api_key:
        api_key_cfg = await service.get_config(tenant_id, "llm_api_key")
        endpoint_cfg = await service.get_config(tenant_id, "llm_endpoint")
        model_cfg = await service.get_config(tenant_id, "default_model")
        api_key = (api_key_cfg.value if api_key_cfg else {}).get("v", "")
        endpoint = (endpoint_cfg.value if endpoint_cfg else {}).get("v", "")
        model = (model_cfg.value if model_cfg else {}).get("v", "")

    return api_key, endpoint, model


def _build_fundamental_context(fundamental_raw: Dict[str, Any]) -> str:
    """构造基本面上下文：F10 items + 十大股东/分红配股/股东户数，总长不超过约 4000 字符。"""
    fund_items = fundamental_raw.get("items") or []
    items_part = json.dumps(
        [{"item": x.get("item"), "value": x.get("value")} for x in fund_items[:25]],
        ensure_ascii=False,
    )[:2500]
    if not items_part.strip():
        items_part = "[]"

    # 扩展：十大股东（按 report_date 去重取最近 2 期，每期最多 10 条）
    top_holders = fundamental_raw.get("top_holders") or []
    holders_by_date: List[Tuple[str, List[Dict]]] = []
    for h in top_holders:
        rd = str(h.get("report_date") or "")
        if not holders_by_date or holders_by_date[-1][0] != rd:
            if len(holders_by_date) >= 2:
                break
            holders_by_date.append((rd, []))
        if len(holders_by_date[-1][1]) < 10:
            holders_by_date[-1][1].append(h)
        if len(holders_by_date) >= 2 and len(holders_by_date[-1][1]) >= 10:
            break
    lines_holders = ["十大股东："]
    if not holders_by_date:
        lines_holders.append("暂无")
    else:
        for rd, rows in holders_by_date:
            lines_holders.append(f"报告期 {rd}")
            for r in rows[:10]:
                name = r.get("holder_name") or ""
                ratio = r.get("hold_ratio")
                chg = r.get("change_type") or r.get("change_count")
                lines_holders.append(f"  {name} 持股比例 {ratio} 变动 {chg}")
    holders_str = "\n".join(lines_holders)

    # 分红配股：最近 5 条
    dividends = (fundamental_raw.get("dividends") or [])[:5]
    lines_div = ["分红配股："]
    if not dividends:
        lines_div.append("暂无")
    else:
        for d in dividends:
            rd = d.get("report_date")
            ex = d.get("ex_date")
            bonus = d.get("bonus_ratio")
            conv = d.get("convert_ratio")
            lines_div.append(f"  报告期 {rd} 除权日 {ex} 送转 {bonus} 派息比例 {conv}")
    div_str = "\n".join(lines_div)

    # 股东户数：最近 5 条
    holder_count = (fundamental_raw.get("holder_count") or [])[:5]
    lines_cnt = ["股东户数："]
    if not holder_count:
        lines_cnt.append("暂无")
    else:
        for c in holder_count:
            ed = c.get("end_date")
            cnt = c.get("holder_count")
            chg = c.get("holder_count_change")
            avg = c.get("avg_hold_amount")
            lines_cnt.append(f"  期末 {ed} 户数 {cnt} 变动 {chg} 户均持股 {avg}")
    cnt_str = "\n".join(lines_cnt)

    extended = f"\n{holders_str}\n{div_str}\n{cnt_str}"
    if len(extended) > 1500:
        extended = extended[:1500] + "..."
    out = items_part + extended
    return out[:4000]


def _format_peer_comparison_for_fundamental(peer_raw: Dict[str, Any]) -> str:
    """将同行比较接口返回格式化为供基本面子 Agent 使用的短文本，总长不超过约 600 字符。"""
    if not peer_raw or not isinstance(peer_raw, dict):
        return ""
    sub_types = peer_raw.get("sub_types")
    if not sub_types or not isinstance(sub_types, list):
        return ""
    name_map = {"growth": "成长性", "valuation": "估值", "dupont": "杜邦", "scale": "规模"}
    lines = ["行业内比较（同行比较，成长性/估值/杜邦/规模）："]
    for st in sub_types[:4]:
        if not isinstance(st, dict):
            continue
        sub_type = st.get("sub_type") or ""
        data = st.get("data")
        if not isinstance(data, list):
            continue
        label = name_map.get(sub_type, sub_type)
        head = data[:3]
        snippet = json.dumps(head, ensure_ascii=False)[:150]
        if len(snippet) >= 147:
            snippet = snippet[:147] + "..."
        lines.append(f"{label}：{snippet}")
    if len(lines) <= 1:
        return ""
    out = "\n".join(lines)
    return out[:600]


def _build_news_context(news_list: List[Dict[str, Any]]) -> str:
    """构造舆情资讯上下文：优先使用 content 作为摘要，总长不超过 2000 字符。"""
    return json.dumps(
        [
            {
                "title": n.get("title"),
                "summary": (n.get("content") or n.get("summary") or "")[:80],
            }
            for n in (news_list or [])[:15]
        ],
        ensure_ascii=False,
    )[:2000]


def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    """从 LLM 回复中提取 JSON（允许被 markdown 代码块包裹）。"""
    if not text or not text.strip():
        return None
    text = text.strip()
    # 尝试去掉 ```json ... ``` 包裹
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
    if m:
        text = m.group(1).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # 尝试找 { ... } 块
    start = text.find("{")
    if start != -1:
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start : i + 1])
                    except json.JSONDecodeError:
                        break
    return None


async def _call_llm(
    endpoint: str,
    api_key: str,
    model: str,
    system_prompt: str,
    user_message: str,
    timeout: float = 60.0,
) -> Optional[str]:
    """非流式调用 chat/completions，返回 content。"""
    url = endpoint.rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    body = {
        "model": model or "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": 1024,
        "stream": False,
    }
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, json=body, headers=headers)
        if resp.status_code != 200:
            logger.warning("LLM request failed: status=%s body=%s", resp.status_code, resp.text[:300])
            return None
        data = resp.json()
        choices = data.get("choices") or []
        if not choices:
            return None
        return (choices[0].get("message") or {}).get("content") or ""
    except Exception as e:
        logger.exception("LLM call exception: %s", e)
        return None


# ---------- 四维 Subagent 系统提示 ----------
TECH_SYSTEM = """你是一个 A 股个股技术面分析子 Agent。仅根据给定的 K 线和指标数据，输出技术面结论。
输出仅包含一个 JSON 对象，不要其他解释。格式：{"direction": "偏多|偏空|中性", "strength": 0-100, "summary": "一段话结论，含均线/形态/量能等要点"}
不要编造数据；若数据不足则 summary 标明「数据不足」并给 direction 为「中性」、strength 为 50。"""

CAPITAL_SYSTEM = """你是一个 A 股个股资金面分析子 Agent。仅根据给定的资金流向数据，输出资金面结论。
输出仅包含一个 JSON 对象，不要其他解释。格式：{"direction": "偏多|偏空|中性", "strength": 0-100, "summary": "一段话结论，含主力/大单/持续性等要点"}
不要编造数据；若数据不足则 summary 标明并给 direction 为「中性」、strength 为 50。"""

FUNDAMENTAL_SYSTEM = """你是一个 A 股个股基本面分析子 Agent。仅根据给定的 F10 与财务数据（可能包含：F10 概览与财务指标、十大股东、分红配股、股东户数），输出基本面结论。
输入中可能包含「行业内比较（同行比较）」的简要数据（成长性/估值/杜邦/规模），可结合这些相对同行的信息在 summary 中简要提及相对估值、相对盈利或相对成长等；若未提供同行比较则忽略。
输入可能包含：估值/盈利/成长/风险相关的财务指标；十大股东持股集中度与变动；分红配股派息与送转；股东户数变化与户均持股。summary 可结合股东结构、分红、户数等简要提及。
输出仅包含一个 JSON 对象，不要其他解释。格式：{"direction": "偏多|偏空|中性", "strength": 0-100, "summary": "一段话结论，含估值/盈利/成长/风险等要点"}
不要编造数据；若数据不足则 summary 标明并给 direction 为「中性」、strength 为 50。"""

SENTIMENT_SYSTEM = """你是一个 A 股个股舆情分析子 Agent。仅根据给定的资讯/公告摘要与互动易问答摘要（若有），输出舆情结论。
输入可能包含：近期资讯/公告摘要；互动易问答摘要（深市标的）。仅根据给定内容输出结论，不要编造。
输出仅包含一个 JSON 对象，不要其他解释。格式：{"direction": "偏多|偏空|中性", "strength": 0-100, "summary": "一段话结论，含利好/利空/热点等要点"}
不要编造数据；若数据不足则 summary 标明并给 direction 为「中性」、strength 为 50。"""

ORCHESTRATOR_SYSTEM = """你是 A 股个股多维度分析的主控 Agent。根据技术面、资金面、基本面、舆情四个子结论，以及用户补充说明（若有），综合成主结论与建议。
用户补充说明若含「仅观察不买入」「仓位上限」等，须在主结论与建议中体现。
输出仅包含一个 JSON 对象，不要其他解释。结构必须为：
{
  "dimensions": [{"name":"技术面","direction":"","strength":0,"summary":""}, {"name":"资金面",...}, {"name":"基本面",...}, {"name":"舆情",...}],
  "main": {"direction":"偏多|偏空|中性","score":0-100,"confidence":0-1,"summary":"综合结论摘要"},
  "zones": [{"id":"aggressive","label":"激进区","desc":"","action":""},{"id":"balanced","label":"平衡区","desc":"","action":""},{"id":"stable","label":"稳定区","desc":"","action":""}],
  "suggest": {"buy": true|false, "text": "建议文案"}
}
不要编造未提供的维度数据；若某维度缺失则以中性补全。"""


async def generate_analysis(
    symbol: str,
    user_notes: Optional[str],
    tenant_id: str,
    db: AsyncSession,
) -> AiAnalysisResult:
    """
    1. 聚合 kline / fundamental / capital_flow / news
    2. 依次调用四维 Subagent
    3. 主控 Orchestrator 汇总
    4. 解析并返回 AiAnalysisResult，失败则返回兜底结果
    """
    symbol = (symbol or "").strip()
    if not symbol:
        return _fallback_result(symbol, "请填写股票代码")

    # ---------- 1. 拉取数据 ----------
    try:
        kline_raw = await fetch_kline(symbol, period="daily", count=60)
        fundamental_raw = await fetch_fundamental(symbol, tenant_id=tenant_id)
        capital_raw = await get_capital_flow_data(symbol, days=30)
        news_raw = await fetch_news(symbol)
    except Exception as e:
        logger.exception("ai_analysis data aggregation failed for %s: %s", symbol, e)
        return _fallback_result(symbol, "数据拉取失败，请稍后重试")

    peer_raw: Dict[str, Any] = {}
    try:
        peer_raw = await get_peer_comparison_data(symbol)
    except Exception as e:
        logger.debug("get_peer_comparison_data for %s: %s", symbol, e)

    # 裁剪为简短 context 控制 token
    kline_bars = kline_raw if isinstance(kline_raw, list) else (kline_raw.get("bars") or [])[:30]
    kline_ctx = json.dumps(kline_bars[-20:], ensure_ascii=False)[:2000] if kline_bars else "[]"

    fund_ctx = _build_fundamental_context(fundamental_raw)
    peer_section = _format_peer_comparison_for_fundamental(peer_raw)
    if peer_section:
        fund_ctx += "\n\n" + peer_section
    fund_ctx = fund_ctx[:4600]

    cap_items = (capital_raw.get("items") or [])[:15]
    cap_ctx = json.dumps(cap_items, ensure_ascii=False)[:1500] if cap_items else "[]"

    news_list = (news_raw or [])[:15]
    news_ctx = _build_news_context(news_list)

    irm_list: List[Dict[str, Any]] = []
    try:
        irm_list = await get_irm_qa_by_symbol(symbol, limit=5)
    except Exception as e:
        logger.debug("get_irm_qa_by_symbol for %s: %s", symbol, e)
    irm_parts = []
    for q in (irm_list or [])[:5]:
        qc = (q.get("question_content") or "").strip()
        ac = (q.get("answer_content") or "").strip()
        c = (qc + " " + ac).strip() or (q.get("content") or "").strip()
        t = q.get("answer_time") or q.get("ask_time") or ""
        if c:
            irm_parts.append("%s 问:%s 答:%s" % (t[:16], qc[:150], ac[:150]))
    irm_ctx = "\n".join(irm_parts)[:800] if irm_parts else "暂无"

    # ---------- 2. LLM 配置 ----------
    api_key, endpoint, model = await _get_llm_config(db, tenant_id)
    if not api_key or not endpoint:
        logger.warning("ai_analysis: no LLM config for tenant %s", tenant_id)
        return _fallback_result(symbol, "未配置 LLM，请在管理后台配置中心设置 API Key 与端点")

    # ---------- 3. 四维 Subagent ----------
    dims: List[DimensionOut] = []
    dim_names = ["技术面", "资金面", "基本面", "舆情"]
    systems = [TECH_SYSTEM, CAPITAL_SYSTEM, FUNDAMENTAL_SYSTEM, SENTIMENT_SYSTEM]
    user_messages = [
        f"标的：{symbol}\n近期日K（最近20根）：\n{kline_ctx}",
        f"标的：{symbol}\n资金流向（最近若干日）：\n{cap_ctx}",
        f"标的：{symbol}\nF10 财务与概览：\n{fund_ctx}",
        f"标的：{symbol}\n近期资讯/公告摘要：\n{news_ctx}\n互动易：\n{irm_ctx}",
    ]

    for i, (name, sys_prompt, user_msg) in enumerate(zip(dim_names, systems, user_messages)):
        content = await _call_llm(endpoint, api_key, model, sys_prompt, user_msg)
        parsed = _extract_json(content) if content else None
        if parsed and isinstance(parsed, dict):
            dims.append(DimensionOut(
                name=name,
                direction=str(parsed.get("direction", "中性"))[:10],
                strength=int(parsed.get("strength", 50)) if isinstance(parsed.get("strength"), (int, float)) else 50,
                summary=str(parsed.get("summary", ""))[:500],
            ))
        else:
            dims.append(DimensionOut(name=name, direction="中性", strength=50, summary="数据不足或解析失败"))

    # ---------- 4. 主控 Orchestrator ----------
    dims_payload = [{"name": d.name, "direction": d.direction, "strength": d.strength, "summary": d.summary} for d in dims]
    orch_user = f"标的：{symbol}\n四维结论：{json.dumps(dims_payload, ensure_ascii=False)}"
    if user_notes and user_notes.strip():
        orch_user += f"\n用户补充说明：{user_notes.strip()}"
    orch_content = await _call_llm(endpoint, api_key, model, ORCHESTRATOR_SYSTEM, orch_user, timeout=90.0)
    parsed = _extract_json(orch_content) if orch_content else None

    if not parsed or not isinstance(parsed, dict):
        logger.warning("ai_analysis: orchestrator returned no valid JSON for %s", symbol)
        return AiAnalysisResult(
            dimensions=dims,
            main=MainOut(
                direction="中性",
                score=50,
                confidence=0.5,
                summary="主控汇总解析失败，仅展示各维度结论。",
            ),
            zones=[
                ZoneOut(id="aggressive", label="激进区", desc="高风险高收益", action="暂不建议"),
                ZoneOut(id="balanced", label="平衡区", desc="中等风险收益", action="观望"),
                ZoneOut(id="stable", label="稳定区", desc="低波动稳健", action="观望"),
            ],
            suggest=SuggestOut(buy=False, text="请稍后重新分析或检查 LLM 配置。"),
        )

    # 解析 main / zones / suggest，缺失则用默认
    main = parsed.get("main")
    if isinstance(main, dict):
        main_out = MainOut(
            direction=str(main.get("direction", "中性"))[:10],
            score=int(main.get("score", 50)) if isinstance(main.get("score"), (int, float)) else 50,
            confidence=float(main.get("confidence", 0.5)) if isinstance(main.get("confidence"), (int, float)) else 0.5,
            summary=str(main.get("summary", ""))[:1000],
        )
    else:
        main_out = MainOut(direction="中性", score=50, confidence=0.5, summary="综合结论解析失败")

    zones_raw = parsed.get("zones") or []
    zones_out: List[ZoneOut] = []
    for z in zones_raw[:5]:
        if isinstance(z, dict):
            zones_out.append(ZoneOut(
                id=str(z.get("id", "")) or "unknown",
                label=str(z.get("label", "")) or "",
                desc=str(z.get("desc", "")),
                action=str(z.get("action", "")),
            ))
    if not zones_out:
        zones_out = [
            ZoneOut(id="aggressive", label="激进区", desc="高风险高收益", action="可小仓试探"),
            ZoneOut(id="balanced", label="平衡区", desc="中等风险收益", action="可分批建仓"),
            ZoneOut(id="stable", label="稳定区", desc="低波动稳健", action="可持有或定投"),
        ]

    sug = parsed.get("suggest") or {}
    if isinstance(sug, dict):
        suggest_out = SuggestOut(
            buy=bool(sug.get("buy", False)),
            text=str(sug.get("text", ""))[:500],
        )
    else:
        suggest_out = SuggestOut(buy=False, text="请结合自身风险承受能力决策。")

    # dimensions 优先用主控返回的，若无效则用上面算出的 dims
    dims_parsed = parsed.get("dimensions")
    if isinstance(dims_parsed, list) and len(dims_parsed) >= 4:
        dims_final = []
        for d in dims_parsed[:4]:
            if isinstance(d, dict):
                dims_final.append(DimensionOut(
                    name=str(d.get("name", "")) or dim_names[len(dims_final)] if len(dims_final) < 4 else "未知",
                    direction=str(d.get("direction", "中性"))[:10],
                    strength=int(d.get("strength", 50)) if isinstance(d.get("strength"), (int, float)) else 50,
                    summary=str(d.get("summary", ""))[:500],
                ))
        if len(dims_final) == 4:
            dims = dims_final
    # else 保持前面 subagent 的 dims

    return AiAnalysisResult(
        dimensions=dims,
        main=main_out,
        zones=zones_out,
        suggest=suggest_out,
    )


async def confirm_analysis(
    *,
    tenant_id: str,
    actor_id: str,
    symbol: str,
    user_notes: Optional[str] = None,
    main_summary: Optional[str] = None,
    suggest_text: Optional[str] = None,
    ip_address: str = "",
    user_agent: str = "",
) -> None:
    """确认（采纳）分析结论，写入审计日志供后续步骤/复盘使用。"""
    detail = json.dumps(
        {
            "symbol": symbol,
            "user_notes": user_notes or "",
            "main_summary": main_summary or "",
            "suggest_text": suggest_text or "",
        },
        ensure_ascii=False,
    )
    await write_audit_log(
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="ai_analysis_confirm",
        resource_type="ai_analysis",
        resource_id=symbol,
        status="success",
        ip_address=ip_address,
        user_agent=user_agent,
        detail=detail,
    )
    logger.info("ai_analysis_confirm: symbol=%s actor=%s", symbol, actor_id)
