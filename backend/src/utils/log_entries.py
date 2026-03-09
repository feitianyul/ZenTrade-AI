"""统一结构化日志条目格式 — 供数据预热、网络代理、行情数据源、数据备份等模块使用

格式约定：{ts, level, msg, **extra}
- ts: ISO8601 UTC 字符串，如 2026-02-22T10:30:00.123Z
- level: INFO | WARN | ERROR
- msg: 人类可读日志内容
- extra: 可选扩展字段（count, error, step, status 等）

前端期望：log_entries 数组，支持 INFO/WARN/ERROR 勾选过滤、详细模式、复制、复制 JSON。
"""

from datetime import datetime
from typing import Any


# 日志级别常量
LEVEL_INFO = "INFO"
LEVEL_WARN = "WARN"
LEVEL_ERROR = "ERROR"


def log_entry(level: str, msg: str, **extra: Any) -> dict:
    """创建一条结构化日志条目，与预热/代理/行情/备份日志格式一致。

    Args:
        level: INFO | WARN | ERROR
        msg: 日志内容
        **extra: 可选扩展字段

    Returns:
        {"ts": "...", "level": "...", "msg": "...", **extra}

    Example:
        log_entry("INFO", "预热开始", items=["hot", "sectors"])
        log_entry("ERROR", "Redis 写入失败", error=str(e))
    """
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return {"ts": ts, "level": level, "msg": msg, **extra}


def status_to_level(status: str) -> str:
    """将业务状态映射为日志级别，用于备份等 status 字段。

    Args:
        status: success | failed | warn 等

    Returns:
        INFO | WARN | ERROR
    """
    if status == "failed":
        return LEVEL_ERROR
    if status == "warn":
        return LEVEL_WARN
    return LEVEL_INFO
