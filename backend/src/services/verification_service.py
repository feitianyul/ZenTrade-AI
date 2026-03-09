"""T247 - 大V认证与排行榜规则

TODO: 设计认证流程后实现。
      当前返回框架数据，认证审批/状态持久化需要:
        1. 新建 verification_requests 表
        2. 在 apply_for_verification 中写入数据库
        3. 在 approve_verification 中更新状态并同步到 users 表
"""

from datetime import datetime
from typing import Any, Optional

# 认证条件
VERIFICATION_CRITERIA = {
    "min_followers": 100,
    "min_shared_strategies": 5,
    "min_total_likes": 500,
    "min_win_rate": 0.6,
    "min_active_days": 30,
}


async def apply_for_verification(
    tenant_id: str,
    user_id: str,
    real_name: str,
    credentials: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """申请大V认证"""
    return {
        "user_id": user_id,
        "status": "pending",
        "applied_at": datetime.utcnow().isoformat(),
        "criteria": VERIFICATION_CRITERIA,
    }


async def check_verification_eligibility(
    tenant_id: str,
    user_id: str,
    stats: dict[str, Any],
) -> dict[str, Any]:
    """检查认证资格"""
    violations = []
    for key, threshold in VERIFICATION_CRITERIA.items():
        if stats.get(key, 0) < threshold:
            violations.append(f"{key}: 需要 {threshold}, 当前 {stats.get(key, 0)}")
    return {
        "eligible": len(violations) == 0,
        "violations": violations,
        "criteria": VERIFICATION_CRITERIA,
    }


async def approve_verification(
    tenant_id: str, user_id: str, approver_id: str
) -> dict[str, Any]:
    """批准认证"""
    return {
        "user_id": user_id,
        "status": "verified",
        "verified_at": datetime.utcnow().isoformat(),
        "approver_id": approver_id,
    }


async def get_ranking_rules() -> list[dict[str, Any]]:
    """获取排行榜规则"""
    return [
        {"dimension": "收益率", "weight": 0.4, "period": "30d"},
        {"dimension": "胜率", "weight": 0.2, "period": "30d"},
        {"dimension": "最大回撤", "weight": 0.15, "period": "30d", "inverse": True},
        {"dimension": "粉丝数", "weight": 0.1, "period": "all"},
        {"dimension": "策略分享数", "weight": 0.1, "period": "all"},
        {"dimension": "活跃度", "weight": 0.05, "period": "7d"},
    ]
