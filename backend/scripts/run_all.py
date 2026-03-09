#!/usr/bin/env python3
"""一条命令启动 API + 回测 Worker + 数据同步 Worker。

模式:
  single (默认): 一个窗口多进程，Worker 退出自动重启，Ctrl+C 同时退出
  multi: 每个进程一个窗口，独立运行
  hot-reload: 监听 src/、scripts/ 下 .py 变更，自动重启 Worker（需 pip install watchdog）

用法:
  python scripts/run_all.py              # single 模式
  python scripts/run_all.py --multi      # multi 模式
  python scripts/run_all.py --hot-reload  # 或 -r，热加载模式（源码变更自动重启 Worker）
  RUN_ALL_MODE=multi python scripts/run_all.py

端口与常见错误（Windows）:
  - WinError 10013: 端口被占用。用 netstat -ano | findstr :8000 查占用，结束进程或设 PORT=8001
  - WinError 10055: 套接字/缓冲区不足。先解决端口占用，或重启后再运行；Worker 已改用 SelectorEventLoop 缓解
"""
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)

# 加载 backend/.env，使子进程（API、Worker）继承 MYSQL_DSN 等
try:
    from dotenv import load_dotenv
    load_dotenv(Path(ROOT) / ".env", override=False)
except Exception:
    pass

# 模式: single=一窗多进程, multi=每进程一窗, hot-reload=源码变更自动重启 Worker
_MODE = os.environ.get("RUN_ALL_MODE", "single")
if "--multi" in sys.argv or "-m" in sys.argv:
    _MODE = "multi"
_HOT_RELOAD = "--hot-reload" in sys.argv or "-r" in sys.argv

_IS_WIN = sys.platform == "win32"
_CREATE_NEW_CONSOLE = getattr(subprocess, "CREATE_NEW_CONSOLE", 0) if _IS_WIN else 0

procs = []
worker_procs = []  # 仅 Worker 进程，热加载时只重启这些
procs_lock = threading.Lock()
worker_procs_lock = threading.Lock()
_shutdown = False
_file_observer = None  # 热加载模式下的文件监听器
_hot_reload_debounce_ts = 0.0  # 防抖：避免连续变更触发多次重启
_HOT_RELOAD_DEBOUNCE_SEC = 2.0

# 重启次数阈值：超过后仅告警，仍继续重启（Phase 1 先记录）
_RESTART_WARN_THRESHOLD = 10


def _do_cleanup():
    """设置停机标志并终止所有子进程"""
    global _shutdown, _file_observer
    _shutdown = True
    if _file_observer is not None:
        try:
            _file_observer.stop()
            _file_observer.join(timeout=2)
        except Exception:
            pass
        _file_observer = None
    with procs_lock:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
    sys.exit(0)


def cleanup(*args):
    _do_cleanup()


def _popen(cmd: list, multi_window: bool = False):
    kwargs = {"cwd": ROOT, "env": {**os.environ}}
    if multi_window and _IS_WIN:
        kwargs["creationflags"] = _CREATE_NEW_CONSOLE
    return subprocess.Popen(cmd, **kwargs)


def _run_worker_with_guard(script_name: str):
    """守护循环：Worker 退出后 sleep(5) 再重启，直到 _shutdown"""
    global _shutdown
    restart_count = 0
    while not _shutdown:
        cmd = [sys.executable, f"scripts/{script_name}"]
        p = _popen(cmd)
        with procs_lock:
            procs.append(p)
        with worker_procs_lock:
            worker_procs.append(p)
        try:
            p.wait()
        finally:
            with procs_lock:
                try:
                    procs.remove(p)
                except ValueError:
                    pass
            with worker_procs_lock:
                try:
                    worker_procs.remove(p)
                except ValueError:
                    pass
        if _shutdown:
            break
        restart_count += 1
        if restart_count >= _RESTART_WARN_THRESHOLD:
            print(f"[guard] {script_name} 重启次数已达 {restart_count}，请检查 Worker 配置或日志")
        else:
            print(f"[guard] {script_name} 已退出，{5}s 后重启 (#{restart_count})")
        for _ in range(5):
            if _shutdown:
                break
            time.sleep(1)
    print(f"[guard] {script_name} 守护已退出")


def _trigger_worker_restart():
    """热加载：终止所有 Worker，由 guard 自动重启（带防抖）"""
    global _hot_reload_debounce_ts
    now = time.time()
    if now - _hot_reload_debounce_ts < _HOT_RELOAD_DEBOUNCE_SEC:
        return
    _hot_reload_debounce_ts = now
    with worker_procs_lock:
        to_term = list(worker_procs)
    if to_term:
        print("[hot-reload] 检测到源码变更，正在重启 Worker...")
        for p in to_term:
            try:
                p.terminate()
            except Exception:
                pass


def _start_file_watcher():
    """启动文件监听，源码变更时重启 Worker"""
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
    except ImportError:
        print("[hot-reload] 未安装 watchdog，请执行: pip install watchdog")
        return None

    class ReloadHandler(FileSystemEventHandler):
        def on_modified(self, event):
            if event.is_directory:
                return
            if getattr(event, "src_path", "").endswith(".py"):
                _trigger_worker_restart()

    observer = Observer()
    for d in ["src", "scripts"]:
        path = Path(ROOT) / d
        if path.exists():
            observer.schedule(ReloadHandler(), str(path), recursive=True)
    observer.start()
    print("[hot-reload] 已监听 src/、scripts/，.py 变更将自动重启 Worker")
    return observer


_api_port = os.environ.get("PORT") or os.environ.get("PANDA_SERVER_PORT", "8000")
_multi = _MODE == "multi"

if _multi:
    print("Multi-window mode: each process in its own window.")
    _popen(
        [sys.executable, "-m", "uvicorn", "src.main:app", "--reload", "--host", "0.0.0.0", "--port", _api_port],
        multi_window=True,
    )
    _popen([sys.executable, "scripts/backtest_worker.py"], multi_window=True)
    _popen([sys.executable, "scripts/data_sync_worker.py"], multi_window=True)
    print(f"API (port {_api_port}), Backtest Worker, Data Sync Worker started in separate windows.")
    print("Close each window to stop that process.")
else:
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    api_proc = _popen(
        [sys.executable, "-m", "uvicorn", "src.main:app", "--reload", "--host", "0.0.0.0", "--port", _api_port]
    )
    with procs_lock:
        procs.append(api_proc)

    if _HOT_RELOAD:
        _file_observer = _start_file_watcher()

    t_sync = threading.Thread(target=_run_worker_with_guard, args=("data_sync_worker.py",), daemon=True)
    t_backtest = threading.Thread(target=_run_worker_with_guard, args=("backtest_worker.py",), daemon=True)
    t_sync.start()
    t_backtest.start()

    mode_hint = "热加载已启用，源码变更将自动重启 Worker。" if _HOT_RELOAD else "Worker 退出将自动重启。"
    print(f"API (port {_api_port}), Backtest Worker, Data Sync Worker 已启动。{mode_hint} Ctrl+C 停止全部。")
    api_proc.wait()
    _do_cleanup()
