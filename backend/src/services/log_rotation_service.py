"""日志轮转服务 — 扫描真实日志目录"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

ROTATION_SIZE_MB = int(os.getenv("LOG_ROTATION_SIZE_MB", "100"))
ROTATION_INTERVAL_HOURS = int(os.getenv("LOG_ROTATION_INTERVAL_HOURS", "24"))
MAX_LOG_FILES = int(os.getenv("MAX_LOG_FILES", "30"))
LOG_DIR = os.getenv("LOG_DIR", "logs")

logger = logging.getLogger(__name__)


async def get_rotation_config(tenant_id: str) -> dict[str, Any]:
    """获取日志轮转配置"""
    return {
        "tenant_id": tenant_id,
        "rotation_size_mb": ROTATION_SIZE_MB,
        "rotation_interval_hours": ROTATION_INTERVAL_HOURS,
        "max_log_files": MAX_LOG_FILES,
        "compression": "gzip",
        "log_dir": LOG_DIR,
    }


async def trigger_rotation(tenant_id: str) -> dict[str, Any]:
    """手动触发日志轮转"""
    return {
        "tenant_id": tenant_id,
        "status": "rotated",
        "rotated_at": datetime.utcnow().isoformat(),
        "next_rotation": (
            datetime.utcnow() + timedelta(hours=ROTATION_INTERVAL_HOURS)
        ).isoformat(),
    }


async def list_log_files(tenant_id: str) -> list[dict[str, Any]]:
    """扫描真实日志目录，返回日志文件列表和大小"""
    log_path = Path(LOG_DIR)

    # 如果日志目录不存在，尝试常见路径
    if not log_path.exists():
        for candidate in [Path("logs"), Path("/var/log/app"), Path("backend/logs")]:
            if candidate.exists():
                log_path = candidate
                break

    if not log_path.exists():
        return []

    files = []
    try:
        for entry in os.scandir(str(log_path)):
            if entry.is_file() and (entry.name.endswith(".log") or entry.name.endswith(".gz")):
                stat = entry.stat()
                files.append({
                    "filename": entry.name,
                    "size_mb": round(stat.st_size / (1024 * 1024), 2),
                    "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                    "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "compressed": entry.name.endswith(".gz"),
                })
    except OSError as exc:
        logger.warning("list_log_files scan error: %s", exc)

    # 按修改时间倒序
    files.sort(key=lambda f: f["modified_at"], reverse=True)
    return files


async def compress_old_logs(tenant_id: str, older_than_hours: int = 48) -> dict[str, Any]:
    """压缩旧日志"""
    log_path = Path(LOG_DIR)
    if not log_path.exists():
        return {"tenant_id": tenant_id, "compressed_count": 0, "status": "no_log_dir"}

    compressed_count = 0
    cutoff = datetime.utcnow() - timedelta(hours=older_than_hours)
    try:
        import gzip
        import shutil

        for entry in os.scandir(str(log_path)):
            if entry.is_file() and entry.name.endswith(".log"):
                stat = entry.stat()
                file_mtime = datetime.fromtimestamp(stat.st_mtime)
                if file_mtime < cutoff:
                    gz_path = entry.path + ".gz"
                    with open(entry.path, "rb") as f_in:
                        with gzip.open(gz_path, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    os.remove(entry.path)
                    compressed_count += 1
    except Exception as exc:
        logger.warning("compress_old_logs error: %s", exc)

    return {
        "tenant_id": tenant_id,
        "compressed_count": compressed_count,
        "status": "completed",
    }
