"""自愈服务

TODO: 接入 Prometheus/自定义监控后实现真实健康检测和自动修复。
      当前 detect_issues 返回空列表（无监控数据源）。
      接入步骤:
        1. 部署 Prometheus 或配置自定义监控采集
        2. 在 detect_issues 中查询 Prometheus metrics
        3. 在 apply_fix 中实现真实的修复操作（重启服务、清缓存等）
"""

import logging
from typing import Any, Dict, List

from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.alert import AlertCreate, AlertLevel
from src.services.alert_service import create_alert

logger = logging.getLogger(__name__)


async def check_and_heal(session: AsyncSession, tenant_id: str):
    """
    Check system health and apply self-healing strategies.
    """
    issues = await detect_issues()

    actions = []
    for issue in issues:
        action = await apply_fix(issue)
        actions.append(action)

        # Alert about the fix
        await create_alert(session, tenant_id, AlertCreate(
            title=f"Self-Heal Action: {issue['type']}",
            message=f"Detected {issue['details']}. Applied fix: {action}",
            level=AlertLevel.WARNING,
            source="self_heal_service",
            metadata={"issue": issue, "action": action}
        ))

    return actions


async def detect_issues() -> List[Dict[str, Any]]:
    """检测系统问题。TODO: 接入 Prometheus/监控系统获取真实指标"""
    # 当前无监控数据源，返回空列表
    logger.debug("self_heal detect_issues: 无监控数据源，跳过检测")
    return []


async def apply_fix(issue: Dict[str, Any]) -> str:
    """应用修复。TODO: 实现真实的修复操作"""
    issue_type = issue.get("type")
    logger.info("self_heal apply_fix: type=%s (TODO: 实现真实修复)", issue_type)
    if issue_type == "high_memory":
        # TODO: await restart_service()
        return "cleared_cache"
    return "no_action"
