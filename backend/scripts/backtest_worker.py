#!/usr/bin/env python3
"""回测独立 Worker — 轮询 pending 任务并执行。

用法（在 backend 目录下）:
  python scripts/backtest_worker.py

环境变量:
  BACKTEST_WORKER_POLL_INTERVAL: 轮询间隔(秒)，默认 3
  BACKTEST_WORKER_GRACEFUL: 1 时收到 SIGTERM 后等待当前任务完成再退出
"""
import asyncio
import os
import signal
import sys

_backend = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _backend not in sys.path:
    sys.path.insert(0, _backend)
os.chdir(_backend)

# 加载 backend/.env（单独运行 Worker 时也能读到 MYSQL_DSN）
try:
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(_backend) / ".env", override=False)
except Exception:
    pass

_POLL_INTERVAL = float(os.environ.get("BACKTEST_WORKER_POLL_INTERVAL", "3"))
_GRACEFUL = os.environ.get("BACKTEST_WORKER_GRACEFUL", "1") == "1"
_shutdown = False


def _on_signal(*_):
    global _shutdown
    _shutdown = True


async def main():
    from src.services.backtest_service import claim_next_pending_backtest_task, run_backtest_job

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    print("Backtest Worker started. Poll interval:", _POLL_INTERVAL, "s")
    while not _shutdown:
        claimed = await claim_next_pending_backtest_task()
        # #region agent log
        try:
            from pathlib import Path as _P
            _lp = _P(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
            _lp.parent.mkdir(parents=True, exist_ok=True)
            with open(_lp, "a", encoding="utf-8") as _f:
                __import__("json").dump({"hypothesisId": "H1", "location": "backtest_worker.main", "message": "poll", "data": {"claimed": bool(claimed), "backtest_id": claimed[2] if claimed else None}, "timestamp": __import__("time").time()}, _f, ensure_ascii=False)
                _f.write("\n")
        except Exception:
            pass
        # #endregion
        if claimed:
            tenant_id, strategy_id, backtest_id = claimed
            print(f"[Worker] Running backtest {backtest_id} (strategy={strategy_id})")
            try:
                await run_backtest_job(tenant_id, strategy_id, backtest_id)
                print(f"[Worker] Completed {backtest_id}")
            except Exception as e:
                print(f"[Worker] Failed {backtest_id}: {e}")
            if _shutdown and _GRACEFUL:
                break
        else:
            await asyncio.sleep(_POLL_INTERVAL)

    print("Backtest Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0)
