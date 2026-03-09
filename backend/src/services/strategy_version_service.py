"""T170 - 策略版本管理与回滚服务"""

from typing import Any, Optional

from sqlalchemy import func, select

from src.core.db import get_session, with_tenant
from src.models.strategy import Strategy
from src.models.strategy_version import StrategyVersion


def _strategy_active_version_no(record: Strategy) -> Optional[int]:
    return getattr(record, "active_version_no", None)


async def create_version(
    tenant_id: str,
    strategy_id: str,
    content_snapshot: dict[str, Any],
    created_by: str,
) -> StrategyVersion:
    """创建新版本快照"""
    async for session in get_session():
        # 获取当前最大版本号
        max_q = with_tenant(
            select(func.max(StrategyVersion.version_no)), StrategyVersion, tenant_id
        ).where(StrategyVersion.strategy_id == strategy_id)
        result = await session.execute(max_q)
        current_max = result.scalar() or 0

        version = StrategyVersion(
            tenant_id=tenant_id,
            strategy_id=strategy_id,
            version_no=current_max + 1,
            content_snapshot=content_snapshot,
            created_by=created_by,
        )
        session.add(version)
        await session.commit()
        await session.refresh(version)
        return version
    raise RuntimeError("no session")


async def list_versions(
    tenant_id: str,
    strategy_id: str,
    limit: int = 20,
) -> list[StrategyVersion]:
    """列出策略版本"""
    async for session in get_session():
        query = (
            with_tenant(select(StrategyVersion), StrategyVersion, tenant_id)
            .where(StrategyVersion.strategy_id == strategy_id)
            .order_by(StrategyVersion.version_no.desc())
            .limit(limit)
        )
        result = await session.execute(query)
        return list(result.scalars().all())
    return []


async def list_versions_paginated(
    tenant_id: str,
    strategy_id: str,
    page: int = 1,
    page_size: int = 10,
    status_filter: Optional[str] = None,
    sort_by: str = "created_at",
    order: str = "desc",
) -> tuple[list[tuple[StrategyVersion, bool]], int]:
    """分页列出策略版本。sort_by: created_at|version_no|status；order: asc|desc。默认按创建时间最新到最旧。"""
    async for session in get_session():
        q_s = (
            with_tenant(select(Strategy), Strategy, tenant_id).where(Strategy.id == strategy_id)
        )
        r_s = await session.execute(q_s)
        strategy = r_s.scalar_one_or_none()
        active_no = _strategy_active_version_no(strategy) if strategy else None

        query = (
            with_tenant(select(StrategyVersion), StrategyVersion, tenant_id)
            .where(StrategyVersion.strategy_id == strategy_id)
        )
        result = await session.execute(query)
        all_versions = list(result.scalars().all())
        paired: list[tuple[StrategyVersion, bool]] = [
            (v, active_no is not None and v.version_no == active_no) for v in all_versions
        ]

        # 状态筛选
        if status_filter == "enabled":
            paired = [p for p in paired if p[1]]
        elif status_filter == "disabled":
            paired = [p for p in paired if not p[1]]
        total = len(paired)

        order_asc = order and order.lower() == "asc"
        if sort_by == "created_at":
            paired.sort(key=lambda x: (x[0].created_at or ""), reverse=not order_asc)
        elif sort_by == "version_no":
            paired.sort(key=lambda x: x[0].version_no, reverse=not order_asc)
        else:
            # status: desc=启用在前，asc=禁用在前；同状态内按版本号倒序
            paired.sort(key=lambda x: (0 if x[1] else 1, -x[0].version_no), reverse=order_asc)

        offset = (page - 1) * page_size
        items = paired[offset : offset + page_size]
        return (items, total)
    return ([], 0)


async def set_active_version(
    tenant_id: str,
    strategy_id: str,
    version_no: Optional[int],
) -> bool:
    """设置策略的启用版本；version_no 为 None 表示全部禁用。"""
    async for session in get_session():
        query = (
            with_tenant(select(Strategy), Strategy, tenant_id).where(Strategy.id == strategy_id)
        )
        result = await session.execute(query)
        strategy = result.scalar_one_or_none()
        if not strategy:
            return False
        strategy.active_version_no = version_no
        await session.commit()
        return True
    return False


async def delete_version(
    tenant_id: str,
    strategy_id: str,
    version_no: int,
) -> bool:
    """删除指定版本；若该版本为当前启用版本则同时清除启用状态。"""
    async for session in get_session():
        q_s = (
            with_tenant(select(Strategy), Strategy, tenant_id).where(Strategy.id == strategy_id)
        )
        r_s = await session.execute(q_s)
        strategy = r_s.scalar_one_or_none()
        if strategy and _strategy_active_version_no(strategy) == version_no:
            strategy.active_version_no = None
        q_v = (
            with_tenant(select(StrategyVersion), StrategyVersion, tenant_id)
            .where(StrategyVersion.strategy_id == strategy_id)
            .where(StrategyVersion.version_no == version_no)
        )
        r_v = await session.execute(q_v)
        v = r_v.scalar_one_or_none()
        if not v:
            return False
        await session.delete(v)
        await session.commit()
        return True
    return False


async def copy_version(
    tenant_id: str,
    strategy_id: str,
    version_no: int,
    created_by: str,
) -> Optional[StrategyVersion]:
    """复制指定版本为新版本（相同 content_snapshot）。"""
    version = await get_version(tenant_id, strategy_id, version_no)
    if not version or not version.content_snapshot:
        return None
    return await create_version(
        tenant_id, strategy_id, dict(version.content_snapshot), created_by
    )


async def get_version(
    tenant_id: str,
    strategy_id: str,
    version_no: int,
) -> Optional[StrategyVersion]:
    """获取指定版本"""
    async for session in get_session():
        query = (
            with_tenant(select(StrategyVersion), StrategyVersion, tenant_id)
            .where(StrategyVersion.strategy_id == strategy_id)
            .where(StrategyVersion.version_no == version_no)
        )
        result = await session.execute(query)
        return result.scalar_one_or_none()
    return None


async def rollback_to_version(
    tenant_id: str,
    strategy_id: str,
    version_no: int,
) -> dict[str, Any]:
    """回滚策略到指定版本"""
    version = await get_version(tenant_id, strategy_id, version_no)
    if not version:
        return {"status": "error", "message": f"version {version_no} not found"}

    snapshot = version.content_snapshot
    async for session in get_session():
        query = (
            with_tenant(select(Strategy), Strategy, tenant_id)
            .where(Strategy.id == strategy_id)
        )
        result = await session.execute(query)
        strategy = result.scalar_one_or_none()
        if not strategy:
            return {"status": "error", "message": "strategy not found"}

        strategy.logic_code = snapshot.get("logic_code", strategy.logic_code)
        strategy.params_json = snapshot.get("params_json", strategy.params_json)
        if "logic_desc" in snapshot:
            strategy.logic_desc = snapshot.get("logic_desc") or None
        await session.commit()
        return {
            "status": "rolled_back",
            "strategy_id": strategy_id,
            "rolled_back_to": version_no,
        }
    return {"status": "error"}


async def diff_versions(
    tenant_id: str,
    strategy_id: str,
    version_a: int,
    version_b: int,
) -> dict[str, Any]:
    """对比两个版本的差异"""
    va = await get_version(tenant_id, strategy_id, version_a)
    vb = await get_version(tenant_id, strategy_id, version_b)
    if not va or not vb:
        return {"status": "error", "message": "version not found"}

    sa = va.content_snapshot
    sb = vb.content_snapshot
    changed_keys = []
    for key in set(list(sa.keys()) + list(sb.keys())):
        if sa.get(key) != sb.get(key):
            changed_keys.append(key)

    return {
        "strategy_id": strategy_id,
        "version_a": version_a,
        "version_b": version_b,
        "changed_fields": changed_keys,
    }
