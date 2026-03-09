#!/usr/bin/env python3
"""数据同步 Worker — 轮询 Redis 队列并执行 run_sync。

用法（在 backend 目录下）:
  python scripts/data_sync_worker.py

环境变量:
  DATA_SYNC_WORKER_POLL_TIMEOUT: BRPOP 超时(秒)，默认 5
  DATA_SYNC_WORKER_GRACEFUL: 1 时收到 SIGTERM 后等待当前任务完成再退出
"""
import asyncio
import json
import sys

# Windows: 使用 SelectorEventLoop 避免 ProactorEventLoop 在套接字资源紧张时触发 WinError 10055
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
import os
import signal

_backend = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _backend not in sys.path:
    sys.path.insert(0, _backend)
os.chdir(_backend)

try:
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(_backend) / ".env", override=False)
except Exception:
    pass

_POLL_TIMEOUT = int(os.environ.get("DATA_SYNC_WORKER_POLL_TIMEOUT", "5"))
_GRACEFUL = os.environ.get("DATA_SYNC_WORKER_GRACEFUL", "1") == "1"
_shutdown = False


def _on_signal(*_):
    global _shutdown
    _shutdown = True


async def _listen_for_cancels():
    """订阅 Redis 取消频道，收到 task_id 时标记到内存，供 run_sync 内 _is_task_cancelled 快速检测。"""
    from src.services.data_service.sync_task_record_service import (
        DATA_SYNC_CANCEL_CHANNEL,
        add_task_cancelled,
    )
    from src.core.streams import get_redis_client
    client = await get_redis_client()
    pubsub = client.pubsub()
    await pubsub.subscribe(DATA_SYNC_CANCEL_CHANNEL)
    try:
        async for message in pubsub.listen():
            if _shutdown:
                break
            if message.get("type") == "message":
                try:
                    task_id = int(message.get("data", 0))
                    if task_id:
                        add_task_cancelled(task_id)
                except (ValueError, TypeError):
                    pass
    except asyncio.CancelledError:
        pass
    finally:
        await pubsub.unsubscribe(DATA_SYNC_CANCEL_CHANNEL)
        await pubsub.aclose()


async def main():
    from src.services.data_service.data_sync_service import (
        DATA_SYNC_QUEUE_KEY,
        run_sync,
        _load_sync_worker_concurrency,
    )
    from src.services.data_service.sync_task_record_service import (
        TaskCancelledError,
        clear_task_cancelled,
    )
    from src.core.streams import get_redis_client

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    client = await get_redis_client()
    cancel_listener = asyncio.create_task(_listen_for_cancels())

    running_tasks: list[asyncio.Task] = []
    running_categories: set[str] = set()
    _concurrency_cache: list = [0.0, 3]  # [timestamp, value]
    _CONCURRENCY_REFRESH_INTERVAL = 15  # 秒

    async def _refresh_concurrency() -> int:
        import time
        now = time.monotonic()
        if now - _concurrency_cache[0] >= _CONCURRENCY_REFRESH_INTERVAL:
            try:
                n = await _load_sync_worker_concurrency()
            except Exception:
                n = _concurrency_cache[1]
            _concurrency_cache[0] = now
            _concurrency_cache[1] = n
        return _concurrency_cache[1]

    async def _run_one(payload: dict, raw: str):
        task_id = payload.get("task_id")
        category = payload.get("category")
        sync_type = payload.get("sync_type", "full")
        tenant_id = payload.get("tenant_id")
        symbols = payload.get("symbols")
        try:
            try:
                print(f"[Worker] Running {category} ({sync_type}) task_id={task_id}")
                kwargs = {"task_id": task_id, "tenant_id": tenant_id}
                if symbols:
                    kwargs["symbols"] = symbols
                await run_sync(category, sync_type, **kwargs)
                print(f"[Worker] Completed task_id={task_id}")
            except TaskCancelledError:
                print(f"[Worker] Cancelled task_id={task_id}")
            except Exception as e:
                print(f"[Worker] Failed task_id={task_id}: {e}")
        finally:
            clear_task_cancelled(task_id)
            running_categories.discard(category)

    def _on_task_done(t):
        try:
            running_tasks.remove(t)
        except ValueError:
            pass

    print("Data Sync Worker started. BRPOP timeout:", _POLL_TIMEOUT, "s")
    try:
        while not _shutdown:
            max_concurrency = await _refresh_concurrency()
            if len(running_tasks) < max_concurrency:
                result = await client.brpop(DATA_SYNC_QUEUE_KEY, timeout=_POLL_TIMEOUT)
                if result:
                    _, raw = result
                    payload = json.loads(raw)
                    category = payload.get("category")
                    if category in running_categories:
                        await client.rpush(DATA_SYNC_QUEUE_KEY, raw)
                        if running_tasks:
                            await asyncio.wait(running_tasks, return_when=asyncio.FIRST_COMPLETED)
                        else:
                            await asyncio.sleep(1)
                        continue
                    running_categories.add(category)
                    task = asyncio.create_task(_run_one(payload, raw))
                    running_tasks.append(task)
                    task.add_done_callback(_on_task_done)
            else:
                if running_tasks:
                    await asyncio.wait(running_tasks, return_when=asyncio.FIRST_COMPLETED)
                else:
                    await asyncio.sleep(_POLL_TIMEOUT)
            if _shutdown and _GRACEFUL:
                break
    finally:
        cancel_listener.cancel()
        try:
            await cancel_listener
        except asyncio.CancelledError:
            pass
        if _GRACEFUL and running_tasks:
            print("[Worker] Waiting for running tasks to finish...")
            await asyncio.gather(*running_tasks, return_exceptions=True)
    print("Data Sync Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0)
