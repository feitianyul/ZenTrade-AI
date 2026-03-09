"""多层备份保留策略服务。

NFR-013: 数据备份：每日全量+每小时增量
PRD 3.1.7: 备份保留期限90天，支持本地+云端双备份
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from src.services.config_center_service import get_config, set_config


# ---------------------------------------------------------------------------
# 保留策略配置
# ---------------------------------------------------------------------------

class RetentionPolicy:
    """备份保留策略。

    默认策略：
      - 日级备份保留 7 天
      - 周级备份保留 4 周
      - 月级备份保留 3 个月
      - 总保留上限 90 天
    """

    def __init__(
        self,
        daily_keep: int = 7,
        weekly_keep: int = 4,
        monthly_keep: int = 3,
        max_retention_days: int = 90,
    ):
        self.daily_keep = daily_keep
        self.weekly_keep = weekly_keep
        self.monthly_keep = monthly_keep
        self.max_retention_days = max_retention_days


DEFAULT_POLICY = RetentionPolicy()


# ---------------------------------------------------------------------------
# 保留策略应用
# ---------------------------------------------------------------------------

async def apply_retention_policy(
    backups: List[Dict[str, Any]],
    policy: Optional[RetentionPolicy] = None,
) -> List[str]:
    """根据保留策略标记应删除的备份ID。

    保留规则：
    1. 最近 daily_keep 天的日备份全部保留
    2. 超出日范围的，保留最近 weekly_keep 周各一份
    3. 超出周范围的，保留最近 monthly_keep 月各一份
    4. 超过 max_retention_days 天的一律删除
    """
    cfg = policy or DEFAULT_POLICY
    now = datetime.now()
    cutoff = now - timedelta(days=cfg.max_retention_days)

    to_delete: List[str] = []

    sorted_backups = sorted(
        backups,
        key=lambda x: x.get("created_at", 0),
        reverse=True,
    )

    # 分桶
    daily_bucket: List[Dict[str, Any]] = []
    weekly_bucket: Dict[int, Dict[str, Any]] = {}  # week_number -> first backup
    monthly_bucket: Dict[str, Dict[str, Any]] = {}  # year-month -> first backup

    daily_cutoff = now - timedelta(days=cfg.daily_keep)
    weekly_cutoff = now - timedelta(weeks=cfg.weekly_keep)

    for b in sorted_backups:
        ts = b.get("created_at", 0)
        try:
            dt = datetime.fromtimestamp(ts) if isinstance(ts, (int, float)) else datetime.fromisoformat(str(ts))
        except (ValueError, TypeError, OSError):
            to_delete.append(b["id"])
            continue

        # 超过最大保留期限 → 删除
        if dt < cutoff:
            to_delete.append(b["id"])
            continue

        # 日级范围
        if dt >= daily_cutoff:
            daily_bucket.append(b)
            continue

        # 周级范围
        if dt >= weekly_cutoff:
            week_key = dt.isocalendar()[1]
            if week_key not in weekly_bucket:
                weekly_bucket[week_key] = b  # 保留每周最近一份
            else:
                to_delete.append(b["id"])
            continue

        # 月级范围
        month_key = dt.strftime("%Y-%m")
        if month_key not in monthly_bucket:
            monthly_bucket[month_key] = b  # 保留每月最近一份
        else:
            to_delete.append(b["id"])

    return to_delete


# ---------------------------------------------------------------------------
# 备份策略配置
# ---------------------------------------------------------------------------

async def build_backup_policy() -> Dict[str, Any]:
    """构建备份策略配置（含本地+云端双备份）。"""
    return {
        "schedule": {
            "full_backup": "daily",     # 每日全量备份
            "incremental": "hourly",    # 每小时增量备份
        },
        "retention": {
            "daily_keep": DEFAULT_POLICY.daily_keep,
            "weekly_keep": DEFAULT_POLICY.weekly_keep,
            "monthly_keep": DEFAULT_POLICY.monthly_keep,
            "max_days": DEFAULT_POLICY.max_retention_days,
        },
        "storage": {
            "local": True,                    # 本地备份
            "cloud": True,                    # 云端备份
            "cloud_provider": "oss",          # 对象存储
            "archive_storage": "s3_glacier",  # 归档存储
        },
        "scope": [
            "strategies",       # 策略数据
            "trade_records",    # 交易记录
            "account_info",     # 账户信息
            "ai_configs",       # AI配置
            "audit_logs",       # 审计日志
        ],
    }


# ---------------------------------------------------------------------------
# 备份策略 API（GET/PUT /backup-policy，规格 5.1）
# ---------------------------------------------------------------------------

BACKUP_POLICY_NAMESPACE = "backup_policy"
BACKUP_POLICY_KEY = "policy"

DEFAULT_BACKUP_POLICY = {
    "schedule_cron": "0 2 * * *",
    "full_interval_days": 1,
    "schedule_time": "02:00",
    "incremental_enabled": False,
    "retention_days": 90,
    "enabled": True,
}


def _cron_to_schedule_time(cron: str) -> str:
    """从 cron 如 '0 2 * * *' 解析出 HH:mm，无法解析时返回 02:00"""
    parts = (cron or "").strip().split()
    if len(parts) >= 2:
        try:
            minute, hour = int(parts[0]), int(parts[1])
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return f"{hour:02d}:{minute:02d}"
        except (ValueError, TypeError):
            pass
    return "02:00"


async def get_backup_policy_api(tenant_id: str) -> Dict[str, Any]:
    """获取备份策略（GET /backup-policy）。存于 config_entries namespace=backup_policy key=policy"""
    raw = await get_config(tenant_id, BACKUP_POLICY_NAMESPACE, BACKUP_POLICY_KEY)
    if not raw or raw.get("value") is None:
        return DEFAULT_BACKUP_POLICY.copy()
    val = raw.get("value")
    if isinstance(val, str):
        try:
            val = json.loads(val)
        except (TypeError, ValueError):
            return DEFAULT_BACKUP_POLICY.copy()
    if not isinstance(val, dict):
        return DEFAULT_BACKUP_POLICY.copy()
    out = {**DEFAULT_BACKUP_POLICY, **val}
    if "full_interval_days" not in val and "schedule_cron" in out:
        out["full_interval_days"] = DEFAULT_BACKUP_POLICY["full_interval_days"]
    if "schedule_time" not in val and "schedule_cron" in out:
        out["schedule_time"] = _cron_to_schedule_time(out.get("schedule_cron", ""))
    if "incremental_enabled" not in val:
        out["incremental_enabled"] = DEFAULT_BACKUP_POLICY["incremental_enabled"]
    return out


async def set_backup_policy_api(
    tenant_id: str,
    schedule_cron: Optional[str] = None,
    full_interval_days: Optional[int] = None,
    schedule_time: Optional[str] = None,
    incremental_enabled: Optional[bool] = None,
    retention_days: Optional[int] = None,
    enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    """保存备份策略（PUT /backup-policy）"""
    current = await get_backup_policy_api(tenant_id)
    if schedule_cron is not None:
        current["schedule_cron"] = schedule_cron
    if full_interval_days is not None:
        current["full_interval_days"] = max(1, min(365, full_interval_days))
    if schedule_time is not None:
        current["schedule_time"] = schedule_time
    if incremental_enabled is not None:
        current["incremental_enabled"] = incremental_enabled
    if retention_days is not None:
        current["retention_days"] = max(1, min(365, retention_days))
    if enabled is not None:
        current["enabled"] = enabled
    await set_config(
        tenant_id,
        BACKUP_POLICY_NAMESPACE,
        BACKUP_POLICY_KEY,
        json.dumps(current, ensure_ascii=False),
        value_type="json",
        description="备份策略：全量间隔、执行时间、增量开关、保留天数、是否启用",
    )
    return current


LAST_SCHEDULED_RUN_KEY = "last_scheduled_run"


async def get_last_scheduled_run(tenant_id: str) -> Optional[float]:
    """定时任务用：上次定时备份执行时间戳"""
    raw = await get_config(tenant_id, BACKUP_POLICY_NAMESPACE, LAST_SCHEDULED_RUN_KEY)
    if not raw or raw.get("value") is None:
        return None
    v = raw.get("value")
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    return None


async def set_last_scheduled_run(tenant_id: str, timestamp: float) -> None:
    """定时任务用：更新上次定时备份执行时间"""
    await set_config(
        tenant_id,
        BACKUP_POLICY_NAMESPACE,
        LAST_SCHEDULED_RUN_KEY,
        str(int(timestamp)),
        value_type="string",
        description="上次定时备份执行时间戳",
    )


LAST_INCREMENTAL_RUN_KEY = "last_incremental_run"


async def get_last_incremental_run(tenant_id: str) -> Optional[float]:
    """定时任务用：上次增量备份执行时间戳"""
    raw = await get_config(tenant_id, BACKUP_POLICY_NAMESPACE, LAST_INCREMENTAL_RUN_KEY)
    if not raw or raw.get("value") is None:
        return None
    v = raw.get("value")
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    return None


async def set_last_incremental_run(tenant_id: str, timestamp: float) -> None:
    """定时任务用：更新上次增量备份执行时间"""
    await set_config(
        tenant_id,
        BACKUP_POLICY_NAMESPACE,
        LAST_INCREMENTAL_RUN_KEY,
        str(int(timestamp)),
        value_type="string",
        description="上次增量备份执行时间戳",
    )
