"""行情数据预热服务 — 手动/定时预热 hot/sectors/ranking/分时，结果写 Redis。"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

from src.utils.log_entries import log_entry

logger = logging.getLogger(__name__)

# 分时预热首版限制 20 只，避免 POST 超时
MINUTE_TOP_N_LIMIT = 20
# 整体预热超时默认 120s，可由环境变量或配置中心覆盖
_DEFAULT_WARMUP_TIMEOUT = 120
# Redis TTL 7 天
WARMUP_TTL = 7 * 24 * 3600
# is_running / progress TTL 防僵尸（5 分钟）
_WARMUP_RUNNING_TTL = 300
# 状态区最近 N 次预热记录
WARMUP_HISTORY_SIZE = 3
# 日志历史最多保留条数
WARMUP_LOG_HISTORY_SIZE = 200


async def check_warmup_running() -> bool:
    """检查是否有预热任务正在执行。"""
    try:
        from src.services.cache_policy_service import get_cached
        val = await get_cached("market:warmup:is_running")
        return val == "1"
    except Exception:
        return False


async def set_warmup_running(flag: bool) -> None:
    """设置/清除预热运行状态。"""
    try:
        from src.services.cache_policy_service import set_cached, invalidate
        if flag:
            await set_cached("market:warmup:is_running", "1", ttl=_WARMUP_RUNNING_TTL)
        else:
            await invalidate("market:warmup:is_running")
            await invalidate("market:warmup:progress")
    except Exception as e:
        logger.warning("set_warmup_running failed: %s", e)


async def _resolve_warmup_timeout(tenant_id: str | None) -> float:
    """从配置中心或环境变量解析预热总超时(秒)。0 表示不限制，有效范围 0-1200。"""
    import os
    try:
        from src.services.config_center_service import get_config
        tid = tenant_id or "public"
        cfg = await get_config(tid, "default", "market_warmup_timeout_seconds")
        if cfg and cfg.get("value") is not None and str(cfg.get("value", "")).strip():
            v = float(cfg["value"])
            if v < 0:
                return 0.0
            if v > 1200:
                return 1200.0
            return v
    except Exception:
        pass
    try:
        v = float(os.getenv("MARKET_WARMUP_TIMEOUT_SECONDS", str(_DEFAULT_WARMUP_TIMEOUT)))
        return max(0.0, min(1200.0, v))
    except (ValueError, TypeError):
        return float(_DEFAULT_WARMUP_TIMEOUT)


async def _resolve_warmup_concurrent_per_proxy(tenant_id: str | None) -> int:
    """每 IP 并发数，默认 1，范围 1-8。"""
    try:
        from src.services.config_center_service import get_config
        tid = tenant_id or "public"
        cfg = await get_config(tid, "default", "market_warmup_concurrent_per_proxy")
        if cfg and cfg.get("value") is not None and str(cfg.get("value", "")).strip():
            v = int(float(cfg["value"]))
            if 1 <= v <= 8:
                return v
    except Exception:
        pass
    return 1


async def _resolve_warmup_pool_pct(tenant_id: str | None) -> float:
    """IP 池在用比例，默认 0.6，范围 0.6-1.0。"""
    try:
        from src.services.config_center_service import get_config
        tid = tenant_id or "public"
        cfg = await get_config(tid, "default", "market_warmup_pool_pct")
        if cfg and cfg.get("value") is not None and str(cfg.get("value", "")).strip():
            v = float(cfg["value"])
            if 0.6 <= v <= 1.0:
                return v
    except Exception:
        pass
    return 0.6


def _code_to_symbol(code: str) -> str:
    """将纯代码转为 symbol 格式（fetch_minute 需要）。"""
    c = str(code).strip()
    if not c:
        return ""
    if c.startswith(("6", "5")):
        return f"{c}.SH"
    if c.startswith(("0", "3", "2")):
        return f"{c}.SZ"
    if c.startswith(("4", "8")):
        return f"{c}.BJ"
    return f"{c}.SZ"  # 默认深交所


async def run_market_warmup(
    items: list[str],
    tenant_id: str | None = None,
    trigger: str = "manual",
) -> dict:
    """按 items 依次执行预热，每步追加 log_entries，结果写 Redis。

    items: ["indices", "hot", "sectors", "ranking", "minute_top20"]
    tenant_id: 租户 ID，用于读取 market_warmup_use_proxy 及获取代理；手动触发时由 API 传入，定时预热时由调度传入。
    trigger: "manual" | "schedule"，用于日志区分手动/定时预热。
    """
    log_entries: list[dict] = []
    results: dict[str, Any] = {}

    def _append(level: str, msg: str, **extra: Any):
        entry = log_entry(level, msg, **extra)
        log_entries.append(entry)
        logger.info("warmup %s: %s", level, msg)

    # 代理上下文：hot/sectors/ranking 单代理；minute_top20 可选代理池+并发
    proxy_token = None
    pool_token = None
    ak_sem_token = None
    use_proxy = False
    domain = "eastmoney.com"
    if tenant_id:
        try:
            from src.services.config_center_service import get_config
            cfg = await get_config(tenant_id, "default", "market_warmup_use_proxy")
            val = cfg.get("value") if cfg else None
            use_proxy = str(val).lower() in ("true", "1", "yes") if val is not None else False
        except Exception:
            pass
    if use_proxy and tenant_id:
        try:
            from src.services.data_service.proxy_pool_service import get_proxy
            from src.services.data_service.data_sync_service import _CURRENT_SYNC_PROXY
            proxy = await get_proxy(tenant_id, domain=domain)
            if proxy:
                proxy_token = _CURRENT_SYNC_PROXY.set(proxy)
                _append("INFO", f"warmup using proxy for {domain}")
            else:
                _append("WARN", "代理已启用但未获取到代理，直连预热")
        except Exception as e:
            _append("WARN", f"warmup proxy setup failed: {e}")

    async def _run() -> dict:
        nonlocal proxy_token, pool_token, ak_sem_token
        from src.services.cache_policy_service import set_cached, get_cached

        if trigger == "schedule":
            _append("INFO", f"定时预热开始: {', '.join(items)}", items=items)
        else:
            _append("INFO", f"预热开始: {', '.join(items)}", items=items)

        total = len(items)
        current = 0

        async def _mark_starting(item_name: str) -> None:
            """标记即将开始某步骤，便于前端显示当前卡点（不增加 current）"""
            try:
                prog = json.dumps({"current": current, "total": total, "current_item": item_name})
                await set_cached("market:warmup:progress", prog, ttl=_WARMUP_RUNNING_TTL)
                await set_cached("market:warmup:log_entries", json.dumps(log_entries, ensure_ascii=False), ttl=WARMUP_TTL)
            except Exception:
                pass

        async def _write_progress(item_name: str) -> None:
            nonlocal current
            current += 1
            try:
                prog = json.dumps({"current": current, "total": total, "current_item": item_name})
                await set_cached("market:warmup:progress", prog, ttl=_WARMUP_RUNNING_TTL)
                await set_cached("market:warmup:log_entries", json.dumps(log_entries, ensure_ascii=False), ttl=WARMUP_TTL)
            except Exception:
                pass

        async def _flush_log_entries() -> None:
            """将 log_entries 立即写入 Redis，便于前端轮询时实时看到（如 minute_top20 长时间任务前）"""
            try:
                await set_cached("market:warmup:log_entries", json.dumps(log_entries, ensure_ascii=False), ttl=WARMUP_TTL)
            except Exception:
                pass

        _tid = tenant_id or "public"
        _warmup_ttl = 600  # 预热写入 Redis TTL 10 分钟，与调度间隔一致

        # 非交易时间：有数据不拉，没有数据还是要拉（补全缓存）
        from src.services.data_service.exchange_time_utils import is_trading_time
        _trading = await is_trading_time()

        def _redis_value_with_ts(items_list: list) -> str:
            now_iso = datetime.utcnow().isoformat() + "Z"
            return json.dumps({"data": items_list, "data_updated_at": now_iso}, ensure_ascii=False)

        if "indices" in items:
            _append("INFO", "开始 indices（大盘指数）…", step="indices")
            await _mark_starting("indices")
            _skip = False
            try:
                raw = await get_cached("market:indices:all")
                if raw:
                    try:
                        obj = json.loads(raw)
                        if isinstance(obj, dict) and obj.get("data") and len(obj["data"]) > 0:
                            _append("INFO", "indices 已有数据，跳过拉取（非交易时间有数据不拉）", skip_reason="has_data")
                            _skip = True
                    except Exception:
                        pass
            except Exception:
                pass
            if not _skip:
                try:
                    from src.services.data_service.hot_rank_service import fetch_indices_from_external, upsert_indices_snapshot
                    data = await fetch_indices_from_external(tenant_id=_tid)
                    results["indices"] = {"count": len(data), "ok": True}
                    _append("INFO", f"indices: {len(data)} 条", count=len(data))
                    if data:
                        await upsert_indices_snapshot(data)
                        await set_cached("market:indices:all", _redis_value_with_ts(data), ttl=_warmup_ttl)
                except Exception as e:
                    results["indices"] = {"ok": False, "error": str(e)}
                    _append("ERROR", f"indices failed: {e}", error=str(e))
            else:
                results["indices"] = {"skipped": True}
            await _write_progress("indices")

        if "hot" in items:
            _append("INFO", "开始 hot（热门排行）…", step="hot")
            await _mark_starting("hot")
            _skip_hot = False
            try:
                raw = await get_cached("market:hot_rank:hot")
                if raw:
                    try:
                        obj = json.loads(raw)
                        if isinstance(obj, dict) and obj.get("data") and len(obj["data"]) > 0:
                            _append("INFO", "hot 已有数据，跳过拉取（非交易时间有数据不拉）", skip_reason="has_data")
                            _skip_hot = True
                    except Exception:
                        pass
            except Exception:
                pass
            if not _skip_hot:
                try:
                    from src.services.data_service.hot_rank_service import fetch_hot_rank_from_external, upsert_spot_snapshot
                    data = await fetch_hot_rank_from_external(tenant_id=_tid)
                    results["hot"] = {"count": len(data), "ok": True}
                    _append("INFO", f"hot_rank: {len(data)} items", count=len(data))
                    if data and any((item.get("price") or 0) != 0 or (item.get("change_pct") or 0) != 0 for item in data[:5]):
                        await upsert_spot_snapshot(data)
                        await set_cached("market:hot_rank:hot", _redis_value_with_ts(data), ttl=_warmup_ttl)
                except Exception as e:
                    results["hot"] = {"ok": False, "error": str(e)}
                    _append("ERROR", f"hot_rank failed: {e}", error=str(e))
            else:
                results["hot"] = {"skipped": True}
            await _write_progress("hot")

        if "sectors" in items:
            _append("INFO", "开始 sectors（板块/概念）…", step="sectors")
            await _mark_starting("sectors")
            _skip_sec = False
            try:
                raw = await get_cached("market:sectors:all")
                if raw:
                    try:
                        obj = json.loads(raw)
                        if isinstance(obj, dict) and obj.get("data") and len(obj["data"]) > 0:
                            _append("INFO", "sectors 已有数据，跳过拉取（非交易时间有数据不拉）", skip_reason="has_data")
                            _skip_sec = True
                    except Exception:
                        pass
            except Exception:
                pass
            if not _skip_sec:
                try:
                    from src.services.data_service.hot_rank_service import fetch_sectors_from_external, upsert_sectors_snapshot
                    data = await fetch_sectors_from_external("all", tenant_id=_tid)
                    results["sectors"] = {"count": len(data), "ok": True}
                    _append("INFO", f"sectors: {len(data)} items", count=len(data))
                    if data and any((item.get("change_pct") or 0) != 0 for item in data):
                        await upsert_sectors_snapshot(data)
                        await set_cached("market:sectors:all", _redis_value_with_ts(data), ttl=_warmup_ttl)
                except Exception as e:
                    results["sectors"] = {"ok": False, "error": str(e)}
                    _append("ERROR", f"sectors failed: {e}", error=str(e))
            else:
                results["sectors"] = {"skipped": True}
            await _write_progress("sectors")

        if "ranking" in items:
            _append("INFO", "开始 ranking（个股排行）…", step="ranking")
            await _mark_starting("ranking")
            _skip_rank = False
            try:
                raw = await get_cached("market:ranking:change_pct:desc:30")
                if raw:
                    try:
                        obj = json.loads(raw)
                        if isinstance(obj, dict) and obj.get("data") and len(obj["data"]) > 0:
                            _append("INFO", "ranking 已有数据，跳过拉取（非交易时间有数据不拉）", skip_reason="has_data")
                            _skip_rank = True
                    except Exception:
                        pass
            except Exception:
                pass
            if not _skip_rank:
                try:
                    from src.services.data_service.hot_rank_service import fetch_ranking_from_external, upsert_spot_snapshot
                    data = await fetch_ranking_from_external("change_pct", "desc", 30, tenant_id=_tid)
                    results["ranking"] = {"count": len(data), "ok": True}
                    _append("INFO", f"ranking: {len(data)} items", count=len(data))
                    if data and any((item.get("price") or 0) != 0 or (item.get("change_pct") or 0) != 0 for item in data[:5]):
                        await upsert_spot_snapshot(data)
                        await set_cached("market:ranking:change_pct:desc:30", _redis_value_with_ts(data), ttl=_warmup_ttl)
                except Exception as e:
                    results["ranking"] = {"ok": False, "error": str(e)}
                    _append("ERROR", f"ranking failed: {e}", error=str(e))
            else:
                results["ranking"] = {"skipped": True}
            await _write_progress("ranking")

        if "minute_top20" in items:
            _append("INFO", "开始 minute_top20（分时前20）…", step="minute_top20")
            await _mark_starting("minute_top20")
            symbols: list[str] = []
            try:
                from src.services.data_service.hot_rank_service import get_hot_rank, get_ranking, get_stock_list
                hot, _ = await get_hot_rank()
                if hot:
                    symbols = [r.get("symbol", "") for r in hot[:20] if r.get("symbol")]
                if not symbols:
                    rank, _ = await get_ranking("change_pct", "desc", 20)
                    if rank:
                        symbols = [r.get("symbol", "") for r in rank if r.get("symbol")]
                if not symbols:
                    sl = await get_stock_list()
                    if sl:
                        symbols = [_code_to_symbol(s.get("code", "")) for s in sl[:20] if s.get("code")]
                symbols = [s for s in symbols if s][:MINUTE_TOP_N_LIMIT]
            except Exception as e:
                _append("WARN", f"minute_top20 symbol source failed: {e}", error=str(e))

            minute_ok = 0
            minute_err = 0
            if symbols:
                from src.services.data_service.market_source_service import fetch_minute
                from src.services.data_service.data_sync_service import (
                    _CURRENT_SYNC_PROXY,
                    _CURRENT_SYNC_PROXY_POOL,
                    _CURRENT_SYNC_AK_SEM,
                    SyncProxyPoolWithReserve,
                )
                # minute_top20 代理池+并发：池可用>=2 时切换为 pool+sem，否则沿用单代理
                if use_proxy and tenant_id:
                    try:
                        from src.services.data_service.proxy_pool_service import (
                            get_proxy_pool_available_count,
                            get_proxies_top_pct,
                        )
                        _append("INFO", f"minute_top20 代理检查: domain={domain}, tenant_id={tenant_id}", domain=domain)
                        await _flush_log_entries()
                        k = await get_proxy_pool_available_count(tenant_id, domain=domain)
                        if k < 2:
                            _append("INFO", f"代理池 {domain} 可用 {k}<2，沿用单代理 (总并发=1)", available=k, domain=domain)
                        elif k >= 2:
                            pool_pct = await _resolve_warmup_pool_pct(tenant_id)
                            per_proxy = await _resolve_warmup_concurrent_per_proxy(tenant_id)
                            active, reserve = await get_proxies_top_pct(tenant_id, pct=pool_pct, domain=domain)
                            if len(active) >= 2:
                                if proxy_token is not None:
                                    _CURRENT_SYNC_PROXY.reset(proxy_token)
                                    proxy_token = None
                                from src.services.data_service.akshare_call_service import _load_sync_ak_retry_config
                                _, _, _, replace_after = await _load_sync_ak_retry_config()
                                pool = SyncProxyPoolWithReserve(
                                    active, reserve,
                                    tenant_id=tenant_id, domain=domain,
                                    replace_after_failures=replace_after,
                                )
                                pool_token = _CURRENT_SYNC_PROXY_POOL.set(pool)
                                total_sem = len(active) * per_proxy
                                ak_sem_token = _CURRENT_SYNC_AK_SEM.set(asyncio.Semaphore(total_sem))
                                _append("INFO", f"minute_top20 using proxy pool N={len(active)}, per_proxy={per_proxy}, sem={total_sem}", available=k, active=len(active), per_proxy=per_proxy, sem=total_sem)
                            else:
                                _append("WARN", f"代理池 k={k} 但 get_proxies_top_pct 仅返回 {len(active)} 个在用，沿用单代理", available=k, active=len(active))
                        await _flush_log_entries()
                    except Exception as e:
                        _append("WARN", f"minute_top20 pool setup failed: {e}", error=str(e))
                        await _flush_log_entries()
                else:
                    if not use_proxy:
                        _append("INFO", "minute_top20: 代理未启用，直连拉取", use_proxy=False)
                    elif not tenant_id:
                        _append("INFO", "minute_top20: 无 tenant_id，沿用单代理", tenant_id=None)
                    await _flush_log_entries()
                from src.services.data_service.market_source_service import fetch_minute_from_external, _normalize_symbol as _norm_sym
                from src.services.data_service.hot_rank_service import upsert_minute_snapshot
                _append("INFO", f"running {len(symbols)} minute fetches (pull -> DB -> Redis)", tasks=len(symbols))
                await _flush_log_entries()
                for sym in symbols:
                    try:
                        data = await fetch_minute_from_external(sym, count=240, days=1)
                        if data and data.get("bars"):
                            await upsert_minute_snapshot(sym, data)
                            code = _norm_sym(sym)
                            await set_cached(f"market:minute:{code}", json.dumps(data, ensure_ascii=False), ttl=_warmup_ttl)
                            minute_ok += 1
                        else:
                            minute_err += 1
                    except Exception as out:
                        minute_err += 1
                        _append("WARN", f"minute {sym} failed: {out}", symbol=sym, error=str(out))
                _append("INFO", f"minute_top20: {minute_ok} ok, {minute_err} err (limit {MINUTE_TOP_N_LIMIT})",
                        ok=minute_ok, err=minute_err)
            else:
                _append("WARN", "minute_top20: no symbols available (cold start)")
            results["minute_top20"] = {"ok": minute_ok, "err": minute_err, "total": len(symbols)}
            await _write_progress("minute_top20")

        now_iso = datetime.utcnow().isoformat() + "Z"
        try:
            await set_cached("market:warmup:last_run", now_iso, ttl=WARMUP_TTL)
            await set_cached("market:warmup:result", json.dumps(results, ensure_ascii=False), ttl=WARMUP_TTL)
            await set_cached("market:warmup:log_entries", json.dumps(log_entries, ensure_ascii=False), ttl=WARMUP_TTL)
            history_raw = await get_cached("market:warmup:history")
            history = json.loads(history_raw) if history_raw else []
            history = [{"run_at": now_iso, "results": results, "trigger": trigger}] + history
            history = history[:WARMUP_HISTORY_SIZE]
            await set_cached("market:warmup:history", json.dumps(history, ensure_ascii=False), ttl=WARMUP_TTL)
            log_history_raw = await get_cached("market:warmup:log_history")
            log_history = json.loads(log_history_raw) if log_history_raw else []
            log_history = log_history + log_entries
            log_history = log_history[-WARMUP_LOG_HISTORY_SIZE:]
            await set_cached("market:warmup:log_history", json.dumps(log_history, ensure_ascii=False), ttl=WARMUP_TTL)
        except Exception as e:
            _append("ERROR", f"Redis write failed: {e}", error=str(e))

        return {
            "success": True,
            "results": results,
            "log_entries": log_entries,
            "last_run_at": now_iso,
        }

    timeout_sec = await _resolve_warmup_timeout(tenant_id)
    try:
        if timeout_sec <= 0:
            return await _run()
        return await asyncio.wait_for(_run(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        _append("ERROR", f"warmup timeout after {timeout_sec}s", timeout=timeout_sec)
        now_iso = datetime.utcnow().isoformat() + "Z"
        try:
            from src.services.cache_policy_service import set_cached, get_cached
            result_to_store = {**results, "error": "timeout"}
            await set_cached("market:warmup:last_run", now_iso, ttl=WARMUP_TTL)
            await set_cached("market:warmup:result", json.dumps(result_to_store, ensure_ascii=False), ttl=WARMUP_TTL)
            await set_cached("market:warmup:log_entries", json.dumps(log_entries, ensure_ascii=False), ttl=WARMUP_TTL)
            history_raw = await get_cached("market:warmup:history")
            history = json.loads(history_raw) if history_raw else []
            history = [{"run_at": now_iso, "results": result_to_store, "trigger": trigger}] + history
            history = history[:WARMUP_HISTORY_SIZE]
            await set_cached("market:warmup:history", json.dumps(history, ensure_ascii=False), ttl=WARMUP_TTL)
            log_history_raw = await get_cached("market:warmup:log_history")
            log_history = json.loads(log_history_raw) if log_history_raw else []
            log_history = log_history + log_entries
            log_history = log_history[-WARMUP_LOG_HISTORY_SIZE:]
            await set_cached("market:warmup:log_history", json.dumps(log_history, ensure_ascii=False), ttl=WARMUP_TTL)
        except Exception as e:
            logger.warning("warmup timeout: Redis write failed: %s", e)
        return {
            "success": False,
            "results": results,
            "log_entries": log_entries,
            "last_run_at": now_iso,
            "error": "timeout",
        }
    finally:
        await set_warmup_running(False)
        try:
            from src.services.data_service.data_sync_service import (
                _CURRENT_SYNC_PROXY,
                _CURRENT_SYNC_PROXY_POOL,
                _CURRENT_SYNC_AK_SEM,
            )
            if proxy_token is not None:
                _CURRENT_SYNC_PROXY.reset(proxy_token)
            if pool_token is not None:
                _CURRENT_SYNC_PROXY_POOL.reset(pool_token)
            if ak_sem_token is not None:
                _CURRENT_SYNC_AK_SEM.reset(ak_sem_token)
        except Exception:
            pass


async def get_warmup_status() -> dict:
    """从 Redis 读取上次预热状态、最近三次 history、运行中进度及日志（运行中=当前 run，非运行中=log_history）。"""
    try:
        from src.services.cache_policy_service import get_cached
        is_running = await check_warmup_running()
        progress_raw = await get_cached("market:warmup:progress")
        progress = json.loads(progress_raw) if progress_raw else None
        last_run = await get_cached("market:warmup:last_run")
        result_raw = await get_cached("market:warmup:result")
        result = json.loads(result_raw) if result_raw else {}
        if is_running:
            log_raw = await get_cached("market:warmup:log_entries")
            log_entries = json.loads(log_raw) if log_raw else []
        else:
            log_raw = await get_cached("market:warmup:log_history")
            log_entries = json.loads(log_raw) if log_raw else []
        history_raw = await get_cached("market:warmup:history")
        history = json.loads(history_raw) if history_raw else []
        out = {
            "is_running": is_running,
            "last_run_at": last_run or None,
            "results": result,
            "log_entries": log_entries,
            "history": history,
        }
        if progress:
            out["progress"] = progress
        return out
    except Exception as e:
        logger.warning("get_warmup_status failed: %s", e)
        return {"is_running": False, "last_run_at": None, "results": {}, "log_entries": [], "history": [], "error": str(e)}
