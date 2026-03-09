"""T175 - AI 与合规缺陷处理流程配置"""

from datetime import datetime
from typing import Any, Optional

# 缺陷优先级
PRIORITY_CRITICAL = "critical"
PRIORITY_HIGH = "high"
PRIORITY_MEDIUM = "medium"
PRIORITY_LOW = "low"

# 缺陷状态
STATUS_OPEN = "open"
STATUS_IN_PROGRESS = "in_progress"
STATUS_RESOLVED = "resolved"
STATUS_CLOSED = "closed"

# 合规缺陷自动分级规则
COMPLIANCE_RULES = {
    "data_leak": PRIORITY_CRITICAL,
    "unauthorized_access": PRIORITY_CRITICAL,
    "ai_hallucination": PRIORITY_HIGH,
    "model_drift": PRIORITY_HIGH,
    "missing_audit_log": PRIORITY_MEDIUM,
    "config_mismatch": PRIORITY_MEDIUM,
    "ui_inconsistency": PRIORITY_LOW,
}


async def create_defect(
    tenant_id: str,
    defect_type: str,
    title: str,
    description: str,
    reporter_id: str,
    priority: Optional[str] = None,
) -> dict[str, Any]:
    """创建缺陷工单"""
    auto_priority = priority or COMPLIANCE_RULES.get(defect_type, PRIORITY_MEDIUM)
    return {
        "tenant_id": tenant_id,
        "defect_type": defect_type,
        "title": title,
        "description": description,
        "reporter_id": reporter_id,
        "priority": auto_priority,
        "status": STATUS_OPEN,
        "created_at": datetime.utcnow().isoformat(),
    }


async def update_defect_status(
    tenant_id: str,
    defect_id: str,
    new_status: str,
    assignee_id: Optional[str] = None,
) -> dict[str, Any]:
    """更新缺陷状态"""
    return {
        "defect_id": defect_id,
        "status": new_status,
        "assignee_id": assignee_id,
        "updated_at": datetime.utcnow().isoformat(),
    }


async def get_defect_workflow(defect_type: str) -> dict[str, Any]:
    """获取缺陷类型的处理流程"""
    workflows = {
        "data_leak": {
            "steps": ["isolate", "investigate", "patch", "audit_review", "close"],
            "sla_hours": 4,
            "auto_escalate": True,
        },
        "ai_hallucination": {
            "steps": ["flag_output", "review_prompt", "adjust_config", "retest", "close"],
            "sla_hours": 24,
            "auto_escalate": False,
        },
        "default": {
            "steps": ["triage", "investigate", "fix", "verify", "close"],
            "sla_hours": 72,
            "auto_escalate": False,
        },
    }
    return workflows.get(defect_type, workflows["default"])


async def list_defects(
    tenant_id: str,
    status: Optional[str] = None,
    priority: Optional[str] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """列出缺陷工单"""
    # 占位实现
    return []


async def get_compliance_summary(tenant_id: str) -> dict[str, Any]:
    """合规缺陷汇总"""
    return {
        "tenant_id": tenant_id,
        "total_open": 0,
        "critical": 0,
        "high": 0,
        "medium": 0,
        "low": 0,
    }
