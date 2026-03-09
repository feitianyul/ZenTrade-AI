"""公共模块：行情 API 用外部请求执行（代理 + 超时 + 重试），不依赖 data_sync context。"""

import asyncio
import logging
from typing import Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

# 默认配置（config 未配置时使用）；东方财富等易触发 Remote end closed，多一次重试
_DEFAULT_TIMEOUT = 10.0
_DEFAULT_RETRY_COUNT = 3
_DEFAULT_USE_PROXY = True
_DEFAULT_DOMAIN_CONCURRENCY_RATIO = 0.6

# 动态域名限流器：(tenant_id, domain) -> (condition, current_usage list)
_domain_limiters: dict[tuple[str, str], tuple[asyncio.Condition, list]] = {}
_limiter_meta_lock = asyncio.Lock()


async def _get_market_config(tenant_id: str, key: str, default: any) -> any:
    """从 config_entries 读取 market_* 配置，namespace 为 default。"""
    try:
        from src.services.config_center_service import get_config
        cfg = await get_config(tenant_id, "default", key)
        if cfg is not None and cfg.get("value") is not None:
            return cfg["value"]
    except Exception as e:
        logger.debug("external_request_executor: get_config %s failed: %s", key, e)
    return default


async def _get_domain_limiter(tenant_id: str, domain: Optional[str]) -> tuple[asyncio.Condition, list]:
    """获取 (tenant_id, domain_key) 对应的 limiter：Condition, [current_usage]。"""
    domain_key = domain or ""
    async with _limiter_meta_lock:
        key = (tenant_id, domain_key)
        if key not in _domain_limiters:
            _domain_limiters[key] = (asyncio.Condition(), [0])
        return _domain_limiters[key]


async def _dynamic_domain_acquire(tenant_id: str, domain: Optional[str]) -> None:
    """DynamicDomainLimiter：根据当前可用代理数计算 limit，等待直到 current_usage < limit 后 +1。"""
    try:
        from src.services.data_service.proxy_pool_service import get_proxy_pool_available_count
        ratio = await _get_market_config(tenant_id, "market_domain_concurrency_ratio", _DEFAULT_DOMAIN_CONCURRENCY_RATIO)
        try:
            ratio = float(ratio)
        except (TypeError, ValueError):
            ratio = _DEFAULT_DOMAIN_CONCURRENCY_RATIO
        available = await get_proxy_pool_available_count(tenant_id, domain=domain)
        limit = max(1, int(available * ratio))
    except Exception as e:
        logger.debug("dynamic_domain_acquire: %s, use limit=1", e)
        limit = 1

    cond, usage_list = await _get_domain_limiter(tenant_id, domain)
    async with cond:
        await cond.wait_for(lambda: usage_list[0] < limit)
        usage_list[0] += 1


async def _dynamic_domain_release(tenant_id: str, domain: Optional[str]) -> None:
    """释放 DynamicDomainLimiter 占位。"""
    domain_key = domain or ""
    key = (tenant_id, domain_key)
    async with _limiter_meta_lock:
        if key not in _domain_limiters:
            return
        cond, usage_list = _domain_limiters[key]
    async with cond:
        usage_list[0] = max(0, usage_list[0] - 1)
        cond.notify()


async def run_external_with_retry(
    fn: Callable[[], T],
    *,
    tenant_id: str = "public",
    domain: Optional[str] = None,
    use_proxy: bool = True,
    timeout_seconds: float = 10.0,
    retry_count: int = 2,
    rate_limit_fn: Optional[Callable[[], None]] = None,
) -> T:
    """
    在代理（可选）+ 超时 + 重试 下执行同步 fn。
    - use_proxy 且 get_proxy 有值时，用 proxy_context 注入
    - get_proxy 返回 None 时降级直连
    - 超时、网络异常可重试，指数退避 1s/2s/4s
    """
    timeout = await _get_market_config(tenant_id, "market_external_timeout_seconds", timeout_seconds)
    try:
        timeout = float(timeout)
    except (TypeError, ValueError):
        timeout = _DEFAULT_TIMEOUT
    retries = await _get_market_config(tenant_id, "market_external_retry_count", retry_count)
    try:
        retries = int(retries)
    except (TypeError, ValueError):
        retries = _DEFAULT_RETRY_COUNT
    use_proxy_cfg = await _get_market_config(tenant_id, "market_use_proxy", _DEFAULT_USE_PROXY)
    if use_proxy_cfg is False or use_proxy_cfg == "false":
        use_proxy = False

    from src.services.data_service.proxy_executor import proxy_context
    from src.services.data_service.proxy_pool_service import get_proxy

    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            await _dynamic_domain_acquire(tenant_id, domain)
            try:
                proxy = await get_proxy(tenant_id, domain=domain) if use_proxy else None
                if use_proxy and not proxy:
                    logger.debug("run_external_with_retry: no proxy, direct")
                if rate_limit_fn is not None:
                    await asyncio.to_thread(rate_limit_fn)
                with proxy_context(proxy):
                    result = await asyncio.wait_for(
                        asyncio.to_thread(fn),
                        timeout=timeout,
                    )
                return result
            finally:
                await _dynamic_domain_release(tenant_id, domain)
        except asyncio.TimeoutError as e:
            last_exc = e
            logger.warning("run_external_with_retry attempt %s timeout: %s", attempt + 1, e)
        except Exception as e:
            last_exc = e
            logger.warning("run_external_with_retry attempt %s failed: %s", attempt + 1, e)
        if attempt < retries:
            delay = 2 ** (attempt + 1)  # 2, 4, 8 秒退避，给远端恢复时间
            await asyncio.sleep(delay)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("run_external_with_retry: unexpected")
