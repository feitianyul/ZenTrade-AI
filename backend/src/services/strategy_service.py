from typing import Any, Iterable, Optional, Tuple

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_session, with_tenant
from src.models.strategy import Strategy
from src.models.strategy_version import StrategyVersion
from src.services.strategy_version_service import create_version


async def _create_strategy_in_session(
    session: AsyncSession,
    *,
    tenant_id: str,
    owner_id: str,
    name: str,
    logic_code: str,
    params_json: Optional[dict[str, Any]],
    logic_desc: Optional[str] = None,
    source_report_id: Optional[str] = None,
) -> Strategy:
    """在给定 session 中创建策略并写入首版快照，供 create_strategy 与 create_strategy_from_analysis 复用。"""
    record = Strategy(
        tenant_id=tenant_id,
        name=name,
        logic_desc=logic_desc,
        logic_code=logic_code,
        params_json=params_json or {},
        status="draft",
        owner_id=owner_id,
        is_deleted=False,
        source_report_id=source_report_id,
    )
    session.add(record)
    await session.flush()
    version = StrategyVersion(
        tenant_id=tenant_id,
        strategy_id=record.id,
        version_no=1,
        content_snapshot={
            "logic_desc": logic_desc or "",
            "logic_code": logic_code,
            "params_json": params_json or {},
        },
        created_by=owner_id,
    )
    session.add(version)
    await session.commit()
    await session.refresh(record)
    return record


async def create_strategy(
    tenant_id: str,
    owner_id: str,
    name: str,
    logic_code: str,
    params_json: Optional[dict[str, Any]],
    logic_desc: Optional[str] = None,
    source_report_id: Optional[str] = None,
) -> Strategy:
    async for session in get_session():
        await _check_name_unique(session, tenant_id, name)
        return await _create_strategy_in_session(
            session,
            tenant_id=tenant_id,
            owner_id=owner_id,
            name=name,
            logic_code=logic_code,
            params_json=params_json,
            logic_desc=logic_desc,
            source_report_id=source_report_id,
        )
    raise RuntimeError("session unavailable")


def build_prompt_from_report_snapshot(snapshot: dict) -> str:
    """从分析报告 report_snapshot 中结构化抽取「条件」与「建议」，用于衍生策略 prompt。"""
    if not snapshot or not isinstance(snapshot, dict):
        return ""
    lines = ["以下为从分析报告提炼的条件与建议，仅供参考，不构成投资建议。", ""]

    # 【条件】
    cond_parts = []
    dimensions = snapshot.get("dimensions")
    if isinstance(dimensions, list):
        for d in dimensions:
            if not isinstance(d, dict):
                continue
            name = d.get("name") or ""
            direction = d.get("direction") or ""
            strength = d.get("strength")
            summary = (d.get("summary") or "").strip()
            s = f"{name}：{direction}"
            if strength is not None:
                s += f"，强度 {strength}"
            if summary:
                s += f"，{summary}"
            cond_parts.append(s)
    main = snapshot.get("main") if isinstance(snapshot.get("main"), dict) else {}
    if main:
        direction = main.get("direction") or ""
        score = main.get("score")
        confidence = main.get("confidence")
        summary = (main.get("summary") or "").strip()
        s = f"综合：{direction}"
        if score is not None:
            s += f"，得分 {score}"
        if confidence is not None:
            s += f"，置信度 {confidence}"
        if summary:
            s += f"，{summary}"
        cond_parts.append(s)
    zones = snapshot.get("zones")
    if isinstance(zones, list):
        for z in zones:
            if not isinstance(z, dict):
                continue
            label = z.get("label") or ""
            desc = (z.get("desc") or "").strip()
            action = (z.get("action") or "").strip()
            s = f"{label}"
            if desc:
                s += f" {desc}"
            if action:
                s += f"，建议：{action}"
            if s.strip():
                cond_parts.append(s.strip())
    if cond_parts:
        lines.append("【条件】")
        lines.extend(cond_parts)
        lines.append("")
    else:
        lines.append("【条件】暂无")
        lines.append("")

    # 【建议】
    suggest = snapshot.get("suggest") if isinstance(snapshot.get("suggest"), dict) else {}
    buy = suggest.get("buy") if isinstance(suggest, dict) else False
    text = (suggest.get("text") or "").strip() if isinstance(suggest, dict) else ""
    lines.append("【建议】")
    lines.append(f"偏多/可关注：{'是' if buy else '否'}" + (f"；{text}" if text else ""))
    return "\n".join(lines).strip()


async def create_strategy_from_analysis(
    report_id: str,
    tenant_id: str,
    owner_id: str,
) -> Optional[str]:
    """根据分析报告生成策略草稿并落库，写入 source_report_id；同一 report_id 重复调用返回已存在的 strategy_id（幂等）。报告不存在或非本租户返回 None。"""
    from src.services.ai_analysis_report_service import get_by_id
    from src.services.ai_service import parse_strategy_prompt

    async for session in get_session():
        report = await get_by_id(session, tenant_id, report_id)
        if report is None:
            return None
        # 幂等：已存在以该报告为来源的策略则直接返回（同一 report 可能有多条历史数据，取第一条）
        existing = await session.execute(
            with_tenant(select(Strategy), Strategy, tenant_id)
            .where(
                Strategy.source_report_id == report_id,
                Strategy.is_deleted.is_(False),
            )
            .limit(1)
        )
        existing_strategy = existing.scalars().first()
        if existing_strategy:
            return existing_strategy.id
        # 从报告 snapshot 结构化抽取「条件 + 建议」作为 prompt
        snapshot = report.report_snapshot or {}
        prompt = build_prompt_from_report_snapshot(snapshot)
        if not prompt.strip():
            prompt = "根据分析报告生成策略"
        prompt += "\n\n请根据上述条件与建议生成可执行策略。策略代码必须为 def run(ctx): 形式，仅使用 ctx 的接口（如 ctx.ma5、ctx.ma20、ctx.buy()、ctx.sell()），不要使用 strategy_logic(data, params) 或 data 字典。"
        parsed = await parse_strategy_prompt(prompt, db=session, tenant_id=tenant_id)
        # 名称加后缀「· 仅供参考」，并保证总长不超 128
        base_name = (parsed.name or "来自分析报告的策略").strip()
        suffix = "· 仅供参考"
        name = (base_name + suffix) if len(base_name) + len(suffix) <= 128 else (base_name[: 128 - len(suffix)] + suffix)
        try:
            await _check_name_unique(session, tenant_id, name)
        except ValueError:
            name = f"{name}-{report_id[:8]}"[:128]
        logic_code = (parsed.logic_code or "").strip() or "# 来自分析报告\npass"
        if logic_code and "仅供参考" not in logic_code:
            logic_code = "# 本策略由分析报告衍生，仅供参考，不构成投资建议。\n" + logic_code
        record = await _create_strategy_in_session(
            session,
            tenant_id=tenant_id,
            owner_id=owner_id,
            name=name,
            logic_code=logic_code,
            params_json=parsed.params_json,
            logic_desc=None,
            source_report_id=report_id,
        )
        return record.id
    return None


async def list_strategies(
    tenant_id: str,
    page: int,
    limit: int,
    owner_id: Optional[str] = None,
) -> Tuple[Iterable[Strategy], int]:
    async for session in get_session():
        base_query = with_tenant(select(Strategy), Strategy, tenant_id).where(
            Strategy.is_deleted.is_(False)
        )
        if owner_id:
            base_query = base_query.where(Strategy.owner_id == owner_id)
        total_stmt = select(func.count()).select_from(base_query.subquery())
        total_result = await session.execute(total_stmt)
        total = int(total_result.scalar() or 0)
        result = await session.execute(
            base_query.order_by(Strategy.created_at.desc()).offset((page - 1) * limit).limit(limit)
        )
        return result.scalars().all(), total
    return [], 0


async def _check_name_unique(session, tenant_id: str, name: str, exclude_id: Optional[str] = None) -> None:
    """检查同名策略，若已存在则抛出 ValueError。"""
    stmt = (
        with_tenant(select(func.count()), Strategy, tenant_id)
        .where(Strategy.is_deleted.is_(False), Strategy.name == name)
    )
    if exclude_id:
        stmt = stmt.where(Strategy.id != exclude_id)
    cnt = (await session.execute(stmt)).scalar() or 0
    if cnt > 0:
        raise ValueError(f"同名策略「{name}」已存在，请更换名称")


async def delete_strategy(tenant_id: str, strategy_id: str) -> bool:
    """软删除策略，不物理删除。"""
    async for session in get_session():
        query = with_tenant(select(Strategy), Strategy, tenant_id).where(
            Strategy.id == strategy_id,
            Strategy.is_deleted.is_(False),
        )
        result = await session.execute(query)
        strategy = result.scalar_one_or_none()
        if not strategy:
            return False
        strategy.is_deleted = True
        await session.commit()
        return True
    return False


async def get_strategy(tenant_id: str, strategy_id: str) -> Optional[Strategy]:
    async for session in get_session():
        query = with_tenant(select(Strategy), Strategy, tenant_id).where(
            Strategy.id == strategy_id,
            Strategy.is_deleted.is_(False),
        )
        result = await session.execute(query)
        return result.scalar_one_or_none()
    return None


async def update_strategy(
    tenant_id: str,
    strategy_id: str,
    name: Optional[str] = None,
    logic_code: Optional[str] = None,
    logic_desc: Optional[str] = None,
    params_json: Optional[dict[str, Any]] = None,
    status: Optional[str] = None,
) -> Optional[Strategy]:
    """更新策略的名称、逻辑描述、逻辑代码、参数、状态；每次保存后创建新版本快照。"""
    async for session in get_session():
        query = with_tenant(select(Strategy), Strategy, tenant_id).where(
            Strategy.id == strategy_id,
            Strategy.is_deleted.is_(False),
        )
        result = await session.execute(query)
        strategy = result.scalar_one_or_none()
        if not strategy:
            return None
        if name is not None:
            await _check_name_unique(session, tenant_id, name, exclude_id=strategy_id)
            strategy.name = name
        if logic_desc is not None:
            strategy.logic_desc = logic_desc
        if logic_code is not None:
            strategy.logic_code = logic_code
        if params_json is not None:
            strategy.params_json = params_json
        if status is not None:
            strategy.status = status
        await session.commit()
        await session.refresh(strategy)
        await create_version(
            tenant_id=tenant_id,
            strategy_id=strategy_id,
            content_snapshot={
                "logic_desc": strategy.logic_desc or "",
                "logic_code": strategy.logic_code or "",
                "params_json": strategy.params_json or {},
            },
            created_by=strategy.owner_id,
        )
        return strategy
    return None


async def update_strategy_backtest_summary(
    tenant_id: str,
    strategy_id: str,
    backtest_id: str,
    grade: str,
    metrics_summary: dict,
) -> None:
    """回测完成后将摘要回写到策略记录。"""
    async for session in get_session():
        query = with_tenant(select(Strategy), Strategy, tenant_id).where(
            Strategy.id == strategy_id,
            Strategy.is_deleted.is_(False),
        )
        result = await session.execute(query)
        strategy = result.scalar_one_or_none()
        if strategy:
            strategy.last_backtest_id = backtest_id
            strategy.last_backtest_grade = grade or None
            strategy.last_backtest_metrics = metrics_summary
            if strategy.status == "draft":
                strategy.status = "backtested"
            await session.commit()
        return
