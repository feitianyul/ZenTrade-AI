"""低频策略调度服务。

FR-029: 低频触发时机控制
  - 日级触发：收盘后10分钟（约15:10）执行
  - 周级触发：周五收盘后10分钟执行
  - 支持条件有效期设置
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# 调度频率
# ---------------------------------------------------------------------------

class ScheduleFrequency(str, Enum):
    DAILY = "daily"    # 每日
    WEEKLY = "weekly"  # 每周


# A股收盘时间
MARKET_CLOSE = time(15, 0)
# 收盘后延迟执行时间（分钟）
POST_CLOSE_DELAY_MINUTES = 10
# 周级触发日（周五=4）
WEEKLY_TRIGGER_WEEKDAY = 4


# ---------------------------------------------------------------------------
# 调度任务定义
# ---------------------------------------------------------------------------

@dataclass
class ScheduledJob:
    """调度任务。"""

    job_id: str = field(default_factory=lambda: f"sch_{uuid.uuid4().hex[:8]}")
    strategy_id: str = ""
    tenant_id: str = ""
    frequency: str = ScheduleFrequency.DAILY.value
    trigger_rule_id: Optional[str] = None
    next_run_at: Optional[str] = None  # ISO datetime
    expire_at: Optional[str] = None    # 条件有效期
    active: bool = True
    last_run_at: Optional[str] = None
    last_result: Optional[str] = None  # success / failed / skipped


# ---------------------------------------------------------------------------
# 计算下次执行时间
# ---------------------------------------------------------------------------

def compute_next_run(
    frequency: str,
    from_dt: Optional[datetime] = None,
) -> datetime:
    """根据频率计算下次调度时间。"""
    now = from_dt or datetime.now()

    # 收盘后10分钟执行
    trigger_time = time(
        MARKET_CLOSE.hour,
        MARKET_CLOSE.minute + POST_CLOSE_DELAY_MINUTES,
    )

    if frequency == ScheduleFrequency.WEEKLY.value:
        # 周五收盘后10分钟
        days_ahead = WEEKLY_TRIGGER_WEEKDAY - now.weekday()
        if days_ahead < 0 or (days_ahead == 0 and now.time() >= trigger_time):
            days_ahead += 7
        next_date = now.date() + timedelta(days=days_ahead)
        return datetime.combine(next_date, trigger_time)

    # 日级：今天或下一个交易日的收盘后10分钟
    if now.time() < trigger_time:
        return datetime.combine(now.date(), trigger_time)

    next_date = now.date() + timedelta(days=1)
    # 跳过周末
    while next_date.weekday() >= 5:
        next_date += timedelta(days=1)
    return datetime.combine(next_date, trigger_time)


# ---------------------------------------------------------------------------
# 服务函数
# ---------------------------------------------------------------------------

async def schedule_job(payload: Dict[str, Any]) -> ScheduledJob:
    """创建调度任务。"""
    frequency = payload.get("frequency", ScheduleFrequency.DAILY.value)
    next_run = compute_next_run(frequency)

    return ScheduledJob(
        strategy_id=payload.get("strategy_id", ""),
        tenant_id=payload.get("tenant_id", ""),
        frequency=frequency,
        trigger_rule_id=payload.get("trigger_rule_id"),
        next_run_at=next_run.isoformat(),
        expire_at=payload.get("expire_at"),
    )


async def cancel_job(job_id: str) -> Dict[str, Any]:
    """取消调度任务。"""
    return {"job_id": job_id, "active": False, "status": "cancelled"}


async def check_job_expiry(job: ScheduledJob) -> bool:
    """检查调度任务是否已过期。

    Returns:
        True 表示已过期。
    """
    if not job.expire_at:
        return False
    try:
        expire_dt = datetime.fromisoformat(job.expire_at)
        return datetime.now() > expire_dt
    except (ValueError, TypeError):
        return False


async def list_pending_jobs(
    tenant_id: str,
    before: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """列出待执行的调度任务（原型占位，实际需查询数据库）。"""
    # TODO: 查询数据库中 active=True 且 next_run_at <= before 的任务
    return []


async def mark_job_completed(
    job_id: str,
    result: str = "success",
) -> Dict[str, Any]:
    """标记任务执行完成并计算下次执行时间。"""
    # TODO: 更新数据库记录
    return {
        "job_id": job_id,
        "last_result": result,
        "last_run_at": datetime.now().isoformat(),
    }
