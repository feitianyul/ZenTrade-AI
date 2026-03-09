"""公共模块：代理环境变量管理，供 API 请求路径与 data_sync 共用。"""

import os
from contextlib import contextmanager
from typing import Dict

_PROXY_KEYS = [
    "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
    "ALL_PROXY", "all_proxy",
]


@contextmanager
def no_proxy():
    """上下文管理器：临时移除代理环境变量，退出后恢复。"""
    saved: Dict[str, str] = {}
    for k in _PROXY_KEYS:
        v = os.environ.pop(k, None)
        if v is not None:
            saved[k] = v
    try:
        yield
    finally:
        for k, v in saved.items():
            os.environ[k] = v


def get_proxy_from_context() -> str | None:
    """从 data_sync/warmup 注入的 context var 获取单代理。需在 async 上下文中、to_thread 之前调用。"""
    try:
        from src.services.data_service.data_sync_service import _CURRENT_SYNC_PROXY
        return _CURRENT_SYNC_PROXY.get()
    except Exception:
        return None


@contextmanager
def proxy_context(proxy: str | None):
    """若 proxy 非空则设置代理环境变量；否则等同于 no_proxy。用于 to_thread 内，proxy 需由调用方在进入线程前传入。"""
    if proxy:
        proxy_url = f"http://{proxy}"
        saved: Dict[str, str] = {}
        for k in _PROXY_KEYS:
            v = os.environ.pop(k, None)
            if v is not None:
                saved[k] = v
        try:
            os.environ["HTTP_PROXY"] = proxy_url
            os.environ["HTTPS_PROXY"] = proxy_url
            yield
        finally:
            for k in _PROXY_KEYS:
                os.environ.pop(k, None)
            for k, v in saved.items():
                os.environ[k] = v
    else:
        with no_proxy():
            yield


def run_with_proxy(proxy: str | None, fn, *args, **kwargs):
    """在 proxy 环境下执行同步 fn（用于 to_thread 内）。"""
    with proxy_context(proxy):
        return fn(*args, **kwargs)
