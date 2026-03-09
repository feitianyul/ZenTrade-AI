"""AKShare 调用共享服务：重试、超时、代理注入。

供 data_sync_service 与 market_source_service.fetch_minute（pool 分支）共用，
实现统一的请求级重试、超时控制与代理热刷新逻辑。
"""

import asyncio
import logging
import os
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# 代理相关环境变量 key
_PROXY_KEYS = [
    "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
    "ALL_PROXY", "all_proxy",
]


def _clear_proxy_env() -> dict:
    """清除代理环境变量，返回保存的键值供恢复。"""
    saved: dict[str, str] = {}
    for k in _PROXY_KEYS:
        v = os.environ.pop(k, None)
        if v is not None:
            saved[k] = v
    return saved


def _restore_proxy_env(saved: dict) -> None:
    """恢复代理环境变量。"""
    for k, v in saved.items():
        os.environ[k] = v


async def _load_sync_ak_retry_config() -> tuple[int, float, int, int]:
    """从 config_center 加载重试配置。返回 (retry_count, backoff_base, timeout_seconds, replace_after_failures)。"""
    defaults = (3, 1.0, 60, 1)
    try:
        from src.services.config_center_service import get_config

        def _int_val(key: str, default: int) -> int:
            v = os.environ.get(key.upper())
            if v is not None:
                try:
                    return max(0, int(v))
                except ValueError:
                    pass
            return default

        def _float_val(key: str, default: float) -> float:
            v = os.environ.get(key.upper())
            if v is not None:
                try:
                    return max(0.0, float(v))
                except ValueError:
                    pass
            return default

        retry_count = _int_val("sync_ak_retry_count", 3)
        backoff_base = _float_val("sync_ak_retry_backoff_base", 1.0)
        timeout_seconds = _int_val("sync_ak_timeout_seconds", 60)
        replace_after = _int_val("sync_proxy_replace_after_failures", 1)

        result = await get_config("public", "default", "sync_ak_retry_count")
        if result and result.get("value") is not None:
            val = result.get("value", result)
            if isinstance(val, (int, float)):
                retry_count = max(0, int(val))
            elif isinstance(val, str) and val.strip():
                retry_count = max(0, int(val.strip()))

        result = await get_config("public", "default", "sync_ak_retry_backoff_base")
        if result and result.get("value") is not None:
            val = result.get("value", result)
            if isinstance(val, (int, float)):
                backoff_base = max(0.0, float(val))
            elif isinstance(val, str) and val.strip():
                backoff_base = max(0.0, float(val.strip()))

        result = await get_config("public", "default", "sync_ak_timeout_seconds")
        if result and result.get("value") is not None:
            val = result.get("value", result)
            if isinstance(val, (int, float)):
                timeout_seconds = max(1, int(val))
            elif isinstance(val, str) and val.strip():
                timeout_seconds = max(1, int(val.strip()))

        result = await get_config("public", "default", "sync_proxy_replace_after_failures")
        if result and result.get("value") is not None:
            val = result.get("value", result)
            if isinstance(val, (int, float)):
                replace_after = max(1, min(10, int(val)))
            elif isinstance(val, str) and val.strip():
                replace_after = max(1, min(10, int(val.strip())))

        return (retry_count, backoff_base, timeout_seconds, replace_after)
    except Exception as exc:
        logger.debug("Load sync_ak_retry_config failed: %s", exc)
    return defaults


async def set_sync_ak_config(
    retry_count: Optional[int] = None,
    timeout_seconds: Optional[int] = None,
    replace_after_failures: Optional[int] = None,
) -> bool:
    """保存 AK 重试配置到 config_center。"""
    import json as _json
    try:
        from src.services.config_center_service import set_config
        if retry_count is not None:
            n = max(0, min(10, int(retry_count)))
            await set_config("public", "default", "sync_ak_retry_count", _json.dumps(n), description="单次请求最大重试次数")
        if timeout_seconds is not None:
            n = max(1, min(300, int(timeout_seconds)))
            await set_config("public", "default", "sync_ak_timeout_seconds", _json.dumps(n), description="每次尝试超时(秒)")
        if replace_after_failures is not None:
            n = max(1, min(10, int(replace_after_failures)))
            await set_config("public", "default", "sync_proxy_replace_after_failures", _json.dumps(n), description="代理槽位连续失败几次后热刷新")
        return True
    except Exception as exc:
        logger.warning("set_sync_ak_config failed: %s", exc)
        return False


def _is_retryable(exc: BaseException) -> bool:
    """判断异常是否可重试（网络超时、连接错误等）。"""
    if isinstance(exc, asyncio.TimeoutError):
        return True
    if isinstance(exc, (ConnectionError, OSError)):
        return True
    exc_name = type(exc).__name__
    if exc_name in ("ConnectError", "ReadTimeout", "ConnectTimeout", "WriteTimeout"):
        return True
    # httpx / requests 等
    mod = type(exc).__module__
    if "httpx" in mod or "requests" in mod:
        if "timeout" in exc_name.lower() or "connect" in exc_name.lower():
            return True
    return False


def _apply_proxy_and_run(
    proxy_value: Optional[str],
    rate_limit_fn: Optional[Callable[[], None]],
    fn: Callable,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """在线程内：设置代理环境变量、可选限速、执行 fn。proxy 注入在模块内自行实现，不依赖 market_source。"""
    if rate_limit_fn:
        rate_limit_fn()
    saved = _clear_proxy_env()
    try:
        if proxy_value:
            proxy_url = f"http://{proxy_value}"
            os.environ["HTTP_PROXY"] = proxy_url
            os.environ["HTTPS_PROXY"] = proxy_url
        return fn(*args, **kwargs)
    finally:
        _restore_proxy_env(saved)


async def run_ak_with_retry(
    fn: Callable,
    *args: Any,
    sem: Optional[asyncio.Semaphore] = None,
    pool: Optional[Any] = None,
    rate_limit_fn: Optional[Callable[[], None]] = None,
    **kwargs: Any,
) -> Any:
    """带重试、超时、代理热刷新的 AKShare 同步函数调用。

    使用懒导入获取 context vars，避免与 data_sync_service 循环依赖。
    - pool is None 时从 _CURRENT_SYNC_PROXY_POOL.get() 取
    - sem is None 时从 _CURRENT_SYNC_AK_SEM.get() 取，取不到则用 _SEM
    - 仅当 isinstance(pool, SyncProxyPoolWithReserve) 时失败才 release(False)
    - 退避公式：backoff_base * (2 ** attempt)，实现 1s、2s、4s
    """
    # 懒导入，避免循环依赖
    def _get_pool():
        from src.services.data_service.data_sync_service import _CURRENT_SYNC_PROXY_POOL
        return _CURRENT_SYNC_PROXY_POOL.get()

    def _get_sem():
        from src.services.data_service.data_sync_service import _CURRENT_SYNC_AK_SEM, _SEM
        s = _CURRENT_SYNC_AK_SEM.get()
        return s if s is not None else _SEM

    def _get_proxy():
        from src.services.data_service.data_sync_service import _CURRENT_SYNC_PROXY
        return _CURRENT_SYNC_PROXY.get()

    def _is_pool_with_reserve(p):
        from src.services.data_service.data_sync_service import SyncProxyPoolWithReserve
        return isinstance(p, SyncProxyPoolWithReserve)

    if pool is None:
        pool = _get_pool()
    if sem is None:
        sem = _get_sem()

    retry_count, backoff_base, timeout_seconds, _ = await _load_sync_ak_retry_config()
    max_attempts = retry_count + 1

    last_exc: Optional[BaseException] = None
    for attempt in range(max_attempts):
        proxy_value: Optional[str] = None
        release: Optional[Callable[[bool], None]] = None

        if pool:
            proxy_value, release = await pool.acquire()
        else:
            proxy_value = _get_proxy()

        def _inner():
            return _apply_proxy_and_run(proxy_value, rate_limit_fn, fn, *args, **kwargs)

        released = False
        try:
            async with sem:
                result = await asyncio.wait_for(
                    asyncio.to_thread(_inner),
                    timeout=timeout_seconds,
                )
            if release:
                release(True)
            return result
        except Exception as exc:
            last_exc = exc
            if release:
                if _is_pool_with_reserve(pool):
                    try:
                        release(False)
                        released = True
                    except Exception:
                        pass
                if not released:
                    try:
                        release(True)
                    except Exception:
                        pass

            if attempt < max_attempts - 1 and _is_retryable(exc):
                delay = backoff_base * (2 ** attempt)
                logger.debug("run_ak_with_retry attempt %s failed, retry in %.1fs: %s", attempt + 1, delay, exc)
                await asyncio.sleep(delay)
                continue
            raise

    if last_exc:
        raise last_exc
    raise RuntimeError("run_ak_with_retry: unexpected exit")
