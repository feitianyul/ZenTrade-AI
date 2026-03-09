"""T176 - 变更管理与回滚策略配置"""

from datetime import datetime
from typing import Any, Optional

# 变更类型
CHANGE_TYPES = ["schema", "config", "deployment", "strategy", "permission", "infra"]

# 变更状态
STATUS_DRAFT = "draft"
STATUS_PENDING_APPROVAL = "pending_approval"
STATUS_APPROVED = "approved"
STATUS_APPLIED = "applied"
STATUS_ROLLED_BACK = "rolled_back"
STATUS_FAILED = "failed"

# 内存变更记录
_change_records: list[dict[str, Any]] = []


async def create_change_request(
    tenant_id: str,
    change_type: str,
    title: str,
    description: str,
    requester_id: str,
    rollback_plan: Optional[str] = None,
) -> dict[str, Any]:
    """创建变更请求"""
    record = {
        "id": f"CR-{len(_change_records) + 1:04d}",
        "tenant_id": tenant_id,
        "change_type": change_type,
        "title": title,
        "description": description,
        "requester_id": requester_id,
        "rollback_plan": rollback_plan or "手动回滚",
        "status": STATUS_DRAFT,
        "created_at": datetime.utcnow().isoformat(),
    }
    _change_records.append(record)
    return record


async def approve_change(
    tenant_id: str, change_id: str, approver_id: str
) -> dict[str, Any]:
    """审批变更"""
    for record in _change_records:
        if record["id"] == change_id and record["tenant_id"] == tenant_id:
            record["status"] = STATUS_APPROVED
            record["approver_id"] = approver_id
            record["approved_at"] = datetime.utcnow().isoformat()
            return record
    return {"error": "change not found"}


async def apply_change(tenant_id: str, change_id: str) -> dict[str, Any]:
    """执行变更"""
    for record in _change_records:
        if record["id"] == change_id and record["tenant_id"] == tenant_id:
            if record["status"] != STATUS_APPROVED:
                return {"error": "change not approved"}
            record["status"] = STATUS_APPLIED
            record["applied_at"] = datetime.utcnow().isoformat()
            return record
    return {"error": "change not found"}


async def rollback_change(tenant_id: str, change_id: str) -> dict[str, Any]:
    """回滚变更"""
    for record in _change_records:
        if record["id"] == change_id and record["tenant_id"] == tenant_id:
            record["status"] = STATUS_ROLLED_BACK
            record["rolled_back_at"] = datetime.utcnow().isoformat()
            return record
    return {"error": "change not found"}


async def list_changes(
    tenant_id: str,
    status: Optional[str] = None,
    change_type: Optional[str] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """列出变更记录"""
    results = [r for r in _change_records if r["tenant_id"] == tenant_id]
    if status:
        results = [r for r in results if r["status"] == status]
    if change_type:
        results = [r for r in results if r["change_type"] == change_type]
    return results[:limit]


async def get_rollback_strategies() -> list[dict[str, Any]]:
    """获取可用回滚策略"""
    return [
        {"strategy": "auto_snapshot", "description": "变更前自动快照，失败自动恢复"},
        {"strategy": "blue_green", "description": "蓝绿部署，切换流量回滚"},
        {"strategy": "manual", "description": "手动执行回滚脚本"},
        {"strategy": "canary_rollback", "description": "金丝雀发布回滚，逐步恢复"},
    ]
