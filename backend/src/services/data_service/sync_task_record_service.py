"""同步任务记录服务 — 统一提供任务记录的 CRUD、日志、取消、入队

职责:
  - 任务创建、更新、删除
  - 任务日志双写（DB + 文件）
  - 任务入队、取消
  - 任务列表与日志查询
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# 同步任务日志目录，可通过环境变量 SYNC_LOG_DIR 配置（默认 logs/sync）
SYNC_LOG_DIR = os.environ.get("SYNC_LOG_DIR", "logs/sync")
# 日志保留天数，环境变量 SYNC_LOG_RETENTION_DAYS 或配置中心 sync_log_retention_days，默认 30
DEFAULT_SYNC_LOG_RETENTION_DAYS = 30

# 数据同步任务队列 Redis key
DATA_SYNC_QUEUE_KEY = "data_sync:queue"
# 取消信号 Redis 频道，API 发布 task_id，Worker 订阅后标记内存
DATA_SYNC_CANCEL_CHANNEL = "data_sync:cancel"

# Worker 进程内内存标记：收到取消信号时写入，_is_task_cancelled 优先检查
_cancelled_task_ids: set = set()


def add_task_cancelled(task_id: int) -> None:
    """Worker 收到取消信号时调用，标记任务已取消。"""
    if task_id:
        _cancelled_task_ids.add(task_id)


def clear_task_cancelled(task_id: int) -> None:
    """任务结束后清理标记，避免内存泄漏。"""
    if task_id:
        _cancelled_task_ids.discard(task_id)


class TaskCancelledError(Exception):
    """任务已被用户取消时抛出，Worker 收到后不再覆盖 status 为 failed。"""


async def _has_running_task(category: str) -> bool:
    """该分类是否存在 status='running' 的任务，用于避免同一分类并发或重复触发。"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTask
        async for session in get_session():
            stmt = select(DataSyncTask).where(
                DataSyncTask.category == category,
                DataSyncTask.status == "running",
            ).limit(1)
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            return row is not None
    except Exception:
        return False


async def _is_task_cancelled(task_id: int) -> bool:
    """查询任务是否已被取消。优先检查 Worker 内存标记（Redis 订阅），再查 DB。"""
    if not task_id:
        return False
    if task_id in _cancelled_task_ids:
        return True
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTask
        async for session in get_session():
            stmt = select(DataSyncTask.status).where(DataSyncTask.id == task_id).limit(1)
            result = await session.execute(stmt)
            status_val = result.scalar_one_or_none()
            return status_val == "cancelled"
    except Exception:
        return False


async def _raise_if_cancelled(task_id: int) -> None:
    """若任务已取消则抛出 TaskCancelledError，供长循环 handler 周期性调用。"""
    if await _is_task_cancelled(task_id):
        raise TaskCancelledError("任务已取消")


def _get_sync_log_dir() -> Path:
    """返回 project_root/logs/sync 目录（与 Worker/API 运行目录无关）"""
    p = Path(__file__).resolve()
    # backend/src/services/data_service/sync_task_record_service.py -> 5 层 parent = project root
    project_root = p.parent.parent.parent.parent.parent
    if os.path.isabs(SYNC_LOG_DIR):
        return Path(SYNC_LOG_DIR)
    return project_root / SYNC_LOG_DIR


def _append_task_log_sync(task_id: int, level: str, message: str) -> None:
    """同步写任务日志到文件：project_root/logs/sync/task_{task_id}.log"""
    if not task_id:
        return
    try:
        from src.core.time_util import now_beijing
        log_dir = _get_sync_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"task_{task_id}.log"
        ts = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{level}] {ts} {message}\n"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as exc:
        logger.warning("Append task log file failed (task_id=%s): %s", task_id, exc)


async def _append_task_log(task_id: int, level: str, message: str) -> None:
    """任务日志双写：写入 data_sync_task_logs 并追加到 logs/sync/task_{task_id}.log"""
    if not task_id:
        return
    try:
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTaskLog
        async for session in get_session():
            log_row = DataSyncTaskLog(task_id=task_id, level=level, message=message)
            session.add(log_row)
            await session.commit()
            break
    except Exception as exc:
        logger.warning("Append task log db failed (task_id=%s): %s", task_id, exc)
    _append_task_log_sync(task_id, level, message)


async def _create_task(category: str, sync_type: str) -> int:
    """创建同步任务记录, 返回 task_id。初始状态为 pending，Worker 领到任务后才设为 running。"""
    try:
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTask
        async for session in get_session():
            task = DataSyncTask(
                category=category,
                sync_type=sync_type,
                status="pending",
                started_at=datetime.utcnow(),
            )
            session.add(task)
            await session.commit()
            await session.refresh(task)
            return task.id
    except Exception:
        return 0


async def _create_task_or_use(category: str, sync_type: str, task_id: Optional[int] = None) -> int:
    """若 task_id 已提供则直接返回；否则创建新任务。供入队模式与 create_task 模式共用。"""
    if task_id and task_id > 0:
        return task_id
    return await _create_task(category, sync_type)


async def enqueue_sync_task(payload: Dict[str, Any]) -> bool:
    """将同步任务 payload 推入 Redis 队列；返回是否入队成功。"""
    try:
        from src.core.streams import get_redis_client
        client = await get_redis_client()
        await client.lpush(DATA_SYNC_QUEUE_KEY, json.dumps(payload, ensure_ascii=False))
        return True
    except Exception as e:
        logger.exception("enqueue_sync_task failed: %s", e)
        return False


async def _update_task(task_id: int, **kwargs) -> None:
    """更新同步任务记录。若任务已为 cancelled，不覆盖 status 为 success/failed。
    每次更新都会写入 updated_at，用于「长时间未更新」stale 判定。"""
    if not task_id:
        return
    try:
        from sqlalchemy import update
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTask
        if kwargs.get("status") in ("success", "failed") and await _is_task_cancelled(task_id):
            kwargs = {k: v for k, v in kwargs.items() if k != "status"}
        if not kwargs:
            return
        kwargs["updated_at"] = datetime.utcnow()
        async for session in get_session():
            await session.execute(
                update(DataSyncTask).where(DataSyncTask.id == task_id).values(**kwargs)
            )
            await session.commit()
    except Exception as exc:
        logger.warning("Update task failed: %s", exc)


async def _return_sync_skipped(
    task_id: int,
    message: str,
    *,
    category: Optional[str] = None,
    date_str: Optional[str] = None,
    sub_key: str = "",
) -> Dict[str, Any]:
    """增量时检测到无需拉取：写一条 INFO 日志、将任务标为 success、返回统一结构。不写业务库。
    当传入 category 与 date_str 时，更新 watermark 的 last_sync_at，使调度按 interval 限频。"""
    if category and date_str:
        try:
            from src.services.data_service.data_sync_service import _set_watermark
            await _set_watermark(category, date_str, sub_key)
        except Exception as exc:
            logger.warning("_return_sync_skipped set_watermark failed: %s", exc)
    if task_id:
        await _append_task_log(task_id, "INFO", message)
        await _update_task(
            task_id,
            status="success",
            total_count=0,
            success_count=0,
            error_count=0,
            finished_at=datetime.utcnow(),
        )
    return {"success": True, "skipped": True, "message": message}


async def cancel_sync_task(task_id: int) -> bool:
    """取消单个同步任务：仅当 status 为 running 或 pending 时更新为 cancelled。返回是否执行了取消。"""
    if not task_id:
        return False
    try:
        from sqlalchemy import select, update
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTask
        async for session in get_session():
            stmt = select(DataSyncTask).where(DataSyncTask.id == task_id)
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            if not row or row.status not in ("running", "pending"):
                return False
            await session.execute(
                update(DataSyncTask).where(DataSyncTask.id == task_id).values(
                    status="cancelled", finished_at=datetime.utcnow()
                )
            )
            await session.commit()
            try:
                from src.core.streams import get_redis_client
                r = await get_redis_client()
                await r.publish(DATA_SYNC_CANCEL_CHANNEL, str(task_id))
            except Exception as e:
                logger.debug("publish cancel signal failed: %s", e)
            return True
    except Exception as exc:
        logger.warning("cancel_sync_task failed: %s", exc)
        return False


async def cancel_all_running_sync_tasks() -> int:
    """取消所有 status 为 running 或 pending 的同步任务，返回被取消的数量。"""
    try:
        from sqlalchemy import select, update
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTask
        async for session in get_session():
            stmt = select(DataSyncTask.id).where(DataSyncTask.status.in_(("running", "pending")))
            result = await session.execute(stmt)
            ids = [r[0] for r in result.fetchall()]
            if not ids:
                return 0
            await session.execute(
                update(DataSyncTask)
                .where(DataSyncTask.status.in_(("running", "pending")))
                .values(status="cancelled", finished_at=datetime.utcnow())
            )
            await session.commit()
            try:
                from src.core.streams import get_redis_client
                r = await get_redis_client()
                for tid in ids:
                    await r.publish(DATA_SYNC_CANCEL_CHANNEL, str(tid))
            except Exception as e:
                logger.debug("publish cancel signals failed: %s", e)
            return len(ids)
    except Exception as exc:
        logger.warning("cancel_all_running_sync_tasks failed: %s", exc)
        return 0


async def delete_sync_task(task_id: int) -> bool:
    """删除单条同步任务记录。仅当 status != 'running' 时允许删除，返回是否删除成功。"""
    if not task_id:
        return False
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTask
        async for session in get_session():
            stmt = select(DataSyncTask).where(DataSyncTask.id == task_id)
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            if not row or row.status == "running":
                return False
            await session.delete(row)
            await session.commit()
            return True
    except Exception as exc:
        logger.warning("delete_sync_task failed: %s", exc)
        return False


async def delete_sync_tasks(task_ids: List[int]) -> int:
    """批量删除同步任务记录，仅删除 status != 'running' 的，返回成功删除条数。"""
    if not task_ids:
        return 0
    deleted = 0
    for tid in task_ids:
        if await delete_sync_task(tid):
            deleted += 1
    return deleted


async def _get_sync_log_retention_days() -> int:
    """从环境变量 SYNC_LOG_RETENTION_DAYS 或配置中心 sync_log_retention_days 读取，默认 30 天。"""
    try:
        v = os.environ.get("SYNC_LOG_RETENTION_DAYS")
        if v is not None and str(v).strip() != "":
            return max(1, int(v))
    except (ValueError, TypeError):
        pass
    try:
        from src.services.config_center_service import get_config
        result = await get_config("public", "default", "sync_log_retention_days")
        if result is not None:
            val = result.get("value", result) if isinstance(result, dict) else result
            if isinstance(val, (int, float)):
                return max(1, int(val))
            if isinstance(val, str) and val.strip() != "":
                return max(1, int(val))
    except Exception:
        pass
    return DEFAULT_SYNC_LOG_RETENTION_DAYS


async def cleanup_sync_logs() -> Dict[str, Any]:
    """按保留天数清理同步任务日志：删除 data_sync_task_logs 过期记录与 logs/sync 下过期 task_*.log 文件。"""
    retention_days = await _get_sync_log_retention_days()
    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    db_deleted = 0
    file_deleted = 0

    try:
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTaskLog
        async for session in get_session():
            from sqlalchemy import delete
            stmt = delete(DataSyncTaskLog).where(DataSyncTaskLog.created_at < cutoff)
            result = await session.execute(stmt)
            db_deleted = result.rowcount if hasattr(result, "rowcount") else 0
            await session.commit()
            break
    except Exception as exc:
        logger.warning("Cleanup sync task logs (db) failed: %s", exc)

    try:
        root = Path(__file__).resolve().parent.parent.parent.parent.parent
        log_dir = root / SYNC_LOG_DIR if not os.path.isabs(SYNC_LOG_DIR) else Path(SYNC_LOG_DIR)
        if log_dir.exists():
            for f in log_dir.glob("task_*.log"):
                try:
                    if f.stat().st_mtime < cutoff.timestamp():
                        f.unlink()
                        file_deleted += 1
                except Exception:
                    pass
    except Exception as exc:
        logger.warning("Cleanup sync task log files failed: %s", exc)

    return {"retention_days": retention_days, "db_deleted": db_deleted, "file_deleted": file_deleted}


def _items_to_log_entries(items: List[Dict[str, Any]], task_id: int) -> List[Dict[str, Any]]:
    """将 items（含 level/message/created_at）转为 log_entries 格式 {ts, level, msg}。WARNING→WARN 映射。"""
    entries = []
    for it in items:
        level = (it.get("level") or "INFO").upper()
        if level == "WARNING":
            level = "WARN"
        ts = it.get("created_at") or ""
        if isinstance(ts, str) and "T" in ts:
            ts = ts.replace("T", " ").split(".")[0]
        entries.append({
            "ts": ts,
            "level": level,
            "msg": it.get("message") or "",
            "task_id": task_id,
            "id": it.get("id"),
        })
    return entries


def _read_task_logs_from_file(task_id: int, page: int, page_size: int) -> Dict[str, Any]:
    """从 project_root/logs/sync/task_{task_id}.log 读取日志（DB 无数据时回退）"""
    import re
    log_file = _get_sync_log_dir() / f"task_{task_id}.log"
    if not log_file.exists():
        return {"items": [], "log_entries": [], "total": 0, "page": page, "page_size": page_size}
    try:
        text = log_file.read_text(encoding="utf-8", errors="replace")
        # 兼容两种时间格式: "YYYY-MM-DD HH:MM:SS" 与 "YYYY-MM-DDTHH:MM:SS.ffffff"
        pat = re.compile(r"^\[(\w+)\]\s+(\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+(.*)$", re.MULTILINE)
        all_items = []
        for m in pat.finditer(text):
            level, ts, msg = m.group(1), m.group(2), m.group(3).rstrip()
            all_items.append({"id": len(all_items) + 1, "task_id": task_id, "level": level, "message": msg, "created_at": ts})
        total = len(all_items)
        offset = (page - 1) * page_size
        items = all_items[offset : offset + page_size]
        log_entries = _items_to_log_entries(items, task_id)
        return {"items": items, "log_entries": log_entries, "total": total, "page": page, "page_size": page_size}
    except Exception:
        return {"items": [], "log_entries": [], "total": 0, "page": page, "page_size": page_size}


async def get_task_logs(task_id: int, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
    """分页获取任务日志，供 GET /data-sync/tasks/{task_id}/logs 使用。DB 无数据时回退读取文件日志。"""
    empty = {"items": [], "log_entries": [], "total": 0, "page": page, "page_size": page_size}
    if not task_id or page < 1 or page_size < 1:
        return empty
    try:
        from sqlalchemy import select, func
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTaskLog
        async for session in get_session():
            count_stmt = select(func.count()).select_from(DataSyncTaskLog).where(DataSyncTaskLog.task_id == task_id)
            total = (await session.execute(count_stmt)).scalar() or 0
            offset = (page - 1) * page_size
            stmt = (
                select(DataSyncTaskLog)
                .where(DataSyncTaskLog.task_id == task_id)
                .order_by(DataSyncTaskLog.created_at.asc())
                .offset(offset)
                .limit(page_size)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            from src.core.time_util import utc_to_beijing_str
            items = [
                {
                    "id": r.id,
                    "task_id": r.task_id,
                    "level": r.level,
                    "message": r.message,
                    "created_at": utc_to_beijing_str(r.created_at) if r.created_at else None,
                }
                for r in rows
            ]
            if total > 0:
                log_entries = _items_to_log_entries(items, task_id)
                return {"items": items, "log_entries": log_entries, "total": total, "page": page, "page_size": page_size}
            break
    except Exception:
        pass
    return _read_task_logs_from_file(task_id, page, page_size)


# 任务停滞阈值默认分钟数（用于「长时间未更新」stale 判定）
DEFAULT_SYNC_STALE_MINUTES = 30


async def _load_sync_stale_minutes() -> Dict[str, int]:
    """从配置中心读取按分类的任务停滞阈值(分钟)。返回 dict category_id -> 分钟，未配置的分类用默认 30。"""
    try:
        import os
        from src.services.config_center_service import get_config
        result = await get_config("public", "default", "sync_stale_minutes")
        if result is not None:
            val = result.get("value", result) if isinstance(result, dict) else result
            if isinstance(val, dict):
                return {k: int(v) if isinstance(v, (int, float)) else DEFAULT_SYNC_STALE_MINUTES
                        for k, v in val.items()}
            if isinstance(val, (int, float)):
                return {"_default": int(val)}
        env_val = os.environ.get("SYNC_STALE_MINUTES")
        if env_val is not None and str(env_val).strip():
            return {"_default": max(1, int(env_val))}
    except Exception as exc:
        logger.debug("_load_sync_stale_minutes: %s", exc)
    return {"_default": DEFAULT_SYNC_STALE_MINUTES}


def _sync_task_row_to_dict(r: Any, *, include_updated_at: bool = True) -> Dict[str, Any]:
    """将 DataSyncTask ORM 行转为与 get_sync_tasks 一致的字典（供 get_sync_tasks / get_sync_tasks_paged 共用）。"""
    from src.services.data_service.data_sync_service import SYNC_CATEGORIES

    def _category_name(cat_id: str) -> str:
        for c in SYNC_CATEGORIES:
            if c["id"] == cat_id:
                return c["name"]
        return cat_id or ""

    def _sync_type_name(st: str) -> str:
        if st == "full":
            return "全量"
        if st == "incremental":
            return "增量"
        if st == "resume":
            return "续传"
        return st or ""

    def _status_name(st: str) -> str:
        if st == "running":
            return "运行中"
        if st == "pending":
            return "排队中"
        if st == "success":
            return "成功"
        if st == "failed":
            return "失败"
        if st == "cancelled":
            return "已取消"
        return st or ""

    from src.core.time_util import utc_to_beijing_str
    out = {
        "id": r.id,
        "category": r.category,
        "category_name": _category_name(r.category or ""),
        "sync_type": r.sync_type,
        "sync_type_name": _sync_type_name(r.sync_type or ""),
        "status": r.status,
        "status_name": _status_name(r.status or ""),
        "total_count": r.total_count,
        "success_count": r.success_count,
        "error_count": r.error_count,
        "error_detail": r.error_detail,
        "failed_symbols": getattr(r, "failed_symbols", None),
        "started_at": utc_to_beijing_str(r.started_at) if r.started_at else None,
        "finished_at": utc_to_beijing_str(r.finished_at) if r.finished_at else None,
    }
    if include_updated_at:
        out["updated_at"] = utc_to_beijing_str(r.updated_at) if getattr(r, "updated_at", None) else None
    return out


async def get_sync_tasks(limit: int = 20) -> List[Dict[str, Any]]:
    """获取最近的同步任务列表（未分页，供其他调用方使用）"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTask
        async for session in get_session():
            stmt = select(DataSyncTask).order_by(DataSyncTask.id.desc())
            if limit > 0:
                stmt = stmt.limit(limit)
            result = await session.execute(stmt)
            rows = result.scalars().all()
            return [_sync_task_row_to_dict(r) for r in rows]
    except Exception as exc:
        logger.warning("get_sync_tasks failed: %s", exc, exc_info=True)
        return []


async def get_sync_tasks_paged(
    page: int = 1, page_size: int = 15
) -> Dict[str, Any]:
    """分页获取同步任务列表，返回 { items, total }。每条 running 任务根据按分类停滞阈值计算 stale。"""
    try:
        from datetime import timezone
        from sqlalchemy import select, func
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTask
        page = max(1, page)
        page_size = max(1, min(page_size, 10000))
        stale_minutes_map = await _load_sync_stale_minutes()
        now_utc = datetime.now(timezone.utc)
        async for session in get_session():
            count_stmt = select(func.count()).select_from(DataSyncTask)
            total_result = await session.execute(count_stmt)
            total = total_result.scalar() or 0
            stmt = (
                select(DataSyncTask)
                .order_by(DataSyncTask.id.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            items = [_sync_task_row_to_dict(r) for r in rows]
            for r, d in zip(rows, items):
                if r.status == "running":
                    last_activity = getattr(r, "updated_at", None) or r.started_at
                    if last_activity:
                        threshold = stale_minutes_map.get(r.category or "") or stale_minutes_map.get("_default", DEFAULT_SYNC_STALE_MINUTES)
                        last_utc = last_activity if (getattr(last_activity, "tzinfo", None)) else last_activity.replace(tzinfo=timezone.utc)
                        delta_seconds = (now_utc - last_utc).total_seconds()
                        d["stale"] = delta_seconds > threshold * 60
                    else:
                        d["stale"] = False
                else:
                    d["stale"] = False
            return {"items": items, "total": total}
    except Exception as exc:
        logger.warning("get_sync_tasks_paged failed: %s", exc, exc_info=True)
        return {"items": [], "total": 0}
