#!/usr/bin/env python3
"""查询指定数据同步任务的运行状态与进度

用法: python scripts/query_task_status.py <task_id>
示例: python scripts/query_task_status.py 8455
"""
import asyncio
import json
import os
import sys
from pathlib import Path

_backend = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend))
os.chdir(_backend)

try:
    from dotenv import load_dotenv
    load_dotenv(_backend / ".env", override=False)
except Exception:
    pass


async def main():
    if len(sys.argv) < 2:
        print("用法: python scripts/query_task_status.py <task_id>")
        sys.exit(1)
    task_id = int(sys.argv[1])

    from sqlalchemy import select
    from src.core.db import get_session
    from src.models.market_sync import DataSyncTask, DataSyncTaskLog

    # 1. 查询任务记录
    task = None
    async for session in get_session():
        stmt = select(DataSyncTask).where(DataSyncTask.id == task_id)
        result = await session.execute(stmt)
        task = result.scalar_one_or_none()
        break

    if not task:
        print(f"任务 {task_id} 不存在")
        sys.exit(1)

    print("=" * 60)
    print(f"任务 ID: {task.id}")
    print(f"分类: {task.category} ({task.sync_type})")
    print(f"状态: {task.status}")
    print(f"进度: 成功 {task.success_count} / 失败 {task.error_count} / 总数 {task.total_count}")
    if task.total_count and task.total_count > 0:
        pct = 100 * (task.success_count + task.error_count) / task.total_count
        print(f"完成度: {pct:.1f}%")
    if task.error_detail:
        print(f"错误: {task.error_detail[:200]}")
    print(f"开始: {task.started_at}")
    print(f"结束: {task.finished_at}")
    print("=" * 60)

    # 2. 查询最近日志
    logs = []
    async for session in get_session():
        stmt = (
            select(DataSyncTaskLog)
            .where(DataSyncTaskLog.task_id == task_id)
            .order_by(DataSyncTaskLog.created_at.desc())
            .limit(20)
        )
        result = await session.execute(stmt)
        logs = list(result.scalars().all())
        break

    print("\n最近 20 条日志 (倒序):")
    for log in reversed(logs):
        ts = log.created_at.strftime("%H:%M:%S") if log.created_at else ""
        print(f"  [{ts}] [{log.level}] {log.message[:120]}")

    # 3. 检查文件日志
    log_dir = _backend.parent / "logs" / "sync"
    log_file = log_dir / f"task_{task_id}.log"
    if log_file.exists():
        print(f"\n文件日志: {log_file}")
        lines = log_file.read_text(encoding="utf-8", errors="replace").strip().split("\n")
        print(f"  共 {len(lines)} 行，最后 5 行:")
        for line in lines[-5:]:
            print(f"    {line[:100]}")
    else:
        print(f"\n文件日志: {log_file} (尚未创建)")


if __name__ == "__main__":
    asyncio.run(main())
