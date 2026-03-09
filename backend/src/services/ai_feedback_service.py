"""T237 - 用户反馈采集与权重自动调整"""

from datetime import datetime
from typing import Any, Optional

_feedbacks: list[dict[str, Any]] = []

FEEDBACK_TYPES = ["helpful", "not_helpful", "inaccurate", "too_slow", "suggestion"]


async def submit_feedback(
    tenant_id: str,
    user_id: str,
    ai_output_id: str,
    feedback_type: str,
    content: str = "",
    rating: Optional[int] = None,
) -> dict[str, Any]:
    """提交反馈"""
    record = {
        "id": f"fb-{len(_feedbacks) + 1:04d}",
        "tenant_id": tenant_id,
        "user_id": user_id,
        "ai_output_id": ai_output_id,
        "feedback_type": feedback_type,
        "content": content,
        "rating": rating,
        "created_at": datetime.utcnow().isoformat(),
    }
    _feedbacks.append(record)
    return record


async def get_feedback_summary(tenant_id: str) -> dict[str, Any]:
    """获取反馈汇总"""
    tenant_fb = [f for f in _feedbacks if f["tenant_id"] == tenant_id]
    total = len(tenant_fb)
    helpful = len([f for f in tenant_fb if f["feedback_type"] == "helpful"])
    return {
        "tenant_id": tenant_id,
        "total_feedbacks": total,
        "helpful_rate": helpful / total if total > 0 else 0,
        "by_type": {ft: len([f for f in tenant_fb if f["feedback_type"] == ft]) for ft in FEEDBACK_TYPES},
    }


async def compute_weight_adjustments(tenant_id: str) -> dict[str, Any]:
    """基于反馈计算权重调整建议"""
    summary = await get_feedback_summary(tenant_id)
    helpful_rate = summary.get("helpful_rate", 0)

    adjustments = {}
    if helpful_rate < 0.5:
        adjustments["temperature"] = -0.1
        adjustments["prompt_revision"] = True
    elif helpful_rate > 0.8:
        adjustments["temperature"] = 0.0
        adjustments["prompt_revision"] = False

    return {
        "tenant_id": tenant_id,
        "helpful_rate": helpful_rate,
        "adjustments": adjustments,
    }


async def list_feedbacks(
    tenant_id: str,
    feedback_type: Optional[str] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """列出反馈"""
    results = [f for f in _feedbacks if f["tenant_id"] == tenant_id]
    if feedback_type:
        results = [f for f in results if f["feedback_type"] == feedback_type]
    return results[:limit]
