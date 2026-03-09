"""统一使用北京时间（UTC+8）对外展示与日志写入；存储层一律 UTC。"""

from datetime import datetime, timezone, timedelta

# 北京时间 UTC+8
BEIJING_TZ = timezone(timedelta(hours=8))


def now_beijing() -> datetime:
    """当前北京时间（用于写文件日志等）。"""
    return datetime.now(BEIJING_TZ)


def now_beijing_naive() -> datetime:
    """当前北京时间，naive（无时区）。仅用于展示/日志或特殊业务，**不再用于落库**；落库请使用 datetime.utcnow()。"""
    return datetime.now(BEIJING_TZ).replace(tzinfo=None)


def utc_to_beijing_str(dt: datetime | None) -> str | None:
    """将 UTC 时间（或无时区视为 UTC）转为北京时间字符串 YYYY-MM-DD HH:MM:SS。"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
