"""行情数据 — 热门排行 / 大盘指数 / 板块排行 / 个股排行 / 龙虎榜 / 机构持仓"""

import asyncio
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 代理上下文（公共模块，支持 data_sync/warmup 注入）
# ---------------------------------------------------------------------------
from src.services.data_service.proxy_executor import (
    get_proxy_from_context as _get_proxy_from_context,
    proxy_context as _proxy_context,
)

# ---------------------------------------------------------------------------
# 内存缓存 (TTL = 30s，避免高频请求 AKShare)
# ---------------------------------------------------------------------------
_cache: Dict[str, Any] = {}
_cache_ts: Dict[str, float] = {}
_CACHE_TTL = 30.0

# AKShare 外部接口超时（东方财富网可能慢/被封，避免行情页阻塞 40s+）
# 默认值，_resolve_akshare_timeout() 优先从配置中心读取；略大以减少 Remote end closed
_DEFAULT_AKSHARE_TIMEOUT = 20.0


async def _market_non_trading_reject_external() -> bool:
    """已取消：不再根据交易时间拒绝外部 API，始终返回 False。"""
    return False


async def _resolve_akshare_timeout() -> float:
    """从配置中心或环境变量解析 AKShare 超时(秒)。优先 public，若无则取任一 tenant 的配置。
    0 或无效范围(非 5 秒起) 视为使用默认 15s，与前端校验一致。无上限限制。"""
    try:
        from src.services.config_center_service import get_config, list_tenant_ids_for_key
        cfg = await get_config("public", "default", "akshare_timeout_seconds")
        if cfg and cfg.get("value") is not None and str(cfg.get("value", "")).strip():
            v = float(cfg["value"])
            if 5 <= v:
                return v
        tids = await list_tenant_ids_for_key("default", "akshare_timeout_seconds")
        if tids:
            cfg = await get_config(tids[0], "default", "akshare_timeout_seconds")
            if cfg and cfg.get("value") is not None and str(cfg.get("value", "")).strip():
                v = float(cfg["value"])
                if 5 <= v:
                    return v
    except Exception:
        pass
    try:
        v = float(os.getenv("AKSHARE_TIMEOUT", "20"))
        return v if 5 <= v else _DEFAULT_AKSHARE_TIMEOUT
    except (ValueError, TypeError):
        return _DEFAULT_AKSHARE_TIMEOUT


def _get_cache(key: str):
    if key in _cache and (time.time() - _cache_ts.get(key, 0)) < _CACHE_TTL:
        return _cache[key]
    return None


def _set_cache(key: str, val: Any):
    _cache[key] = val
    _cache_ts[key] = time.time()


def _parse_redis_data(raw: str) -> tuple[list[dict] | None, str | None]:
    """解析预热写入的 Redis 值：支持 {data: [...], data_updated_at: iso} 或兼容纯 list。返回 (data, data_updated_at)。"""
    if not raw:
        return None, None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict) and "data" in parsed:
            data = parsed.get("data")
            ts = parsed.get("data_updated_at")
            return (data if isinstance(data, list) else None, str(ts) if ts else None)
        if isinstance(parsed, list):
            return parsed, None
    except Exception:
        pass
    return None, None


def _try_import_ak():
    try:
        import akshare as ak
        return ak
    except ImportError:
        logger.warning("akshare not installed")
        return None


def _safe_str(val, default: str = "") -> str:
    """安全提取字符串值，防止 NaN / None / nan。"""
    if val is None:
        return default
    try:
        if isinstance(val, float) and math.isnan(val):
            return default
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    return s if s and s.lower() != "nan" else default


def _safe_float(val, default: float = 0.0) -> float:
    """安全提取浮点值。"""
    if val is None:
        return default
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _safe_int(val, default: int = 0) -> int:
    """安全提取整型值。"""
    return int(_safe_float(val, float(default)))


def _find_col(df_columns, *candidates) -> str | None:
    """在 DataFrame 列名中查找第一个匹配的列。"""
    cols = list(df_columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def _normalize_sina_code(code: str) -> str:
    """新浪代码 bj430017/sz000001/sh600000 转为纯数字与东财一致。"""
    if not code:
        return ""
    s = str(code).strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if s.startswith(prefix):
            return s[len(prefix):]
    return s


def _map_sina_spot_df_to_common(df) -> list[dict]:
    """新浪 stock_zh_a_spot 全市场 DataFrame 映射为与东财一致的结构；新浪无换手率填 0。"""
    if df is None or (hasattr(df, "__len__") and len(df) == 0):
        return []
    result = []
    for _, row in df.iterrows():
        code_raw = _safe_str(row.get("代码"))
        symbol = _normalize_sina_code(code_raw) or code_raw
        result.append({
            "symbol": symbol,
            "name": _safe_str(row.get("名称")),
            "price": _safe_float(row.get("最新价")),
            "change_pct": _safe_float(row.get("涨跌幅")),
            "change_amt": _safe_float(row.get("涨跌额")),
            "volume": _safe_int(row.get("成交量")),
            "turnover": _safe_int(row.get("成交额")),
            "turnover_rate": 0.0,
            "rank": 0,
        })
    return result


# ---------------------------------------------------------------------------
# 1) 热门排行 (返回完整股票信息, 全部使用真实数据) — 新浪主、东财备
# ---------------------------------------------------------------------------


async def get_hot_rank(*, tenant_id: str = "public", force_fetch_external: bool = False) -> tuple[list[dict], str | None]:
    """返回 (热门排行列表, data_updated_at ISO 或 None)。读路径仅 DB+Redis。"""
    cached = _get_cache("hot_rank")
    if cached is not None:
        return (cached if isinstance(cached, list) else [], None)

    # L2: Redis
    try:
        from src.services.cache_policy_service import get_cached
        l2 = await get_cached("market:hot_rank:hot")
        if l2:
            data, updated_at = _parse_redis_data(l2)
            if data:
                _set_cache("hot_rank", data)
                return data, updated_at
    except Exception:
        pass

    if force_fetch_external:
        return [], None

    # 可选：Redis 过期数据
    try:
        from src.services.cache_policy_service import get_cached as _gc
        stale = await _gc("market:hot_rank:hot")
        if stale:
            data, updated_at = _parse_redis_data(stale)
            if data:
                return data, updated_at
    except Exception:
        pass

    # L3: 读库快照（预热写入）
    snap, updated_at = await _get_spot_snapshot_from_db(limit=30, sort_by="change_pct", order="desc")
    if snap:
        _set_cache("hot_rank", snap)
        return snap, updated_at

    # 兜底：stock_info 基础列表（无涨跌幅）
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_data import StockInfo
        async for session in get_session():
            rows = (await session.execute(select(StockInfo).limit(30))).scalars().all()
            if rows:
                result = [{"symbol": r.code, "name": r.name, "code": r.code,
                           "price": 0, "change_pct": 0, "volume": 0, "turnover": 0} for r in rows]
                _set_cache("hot_rank", result)
                return result, None
    except Exception as exc:
        logger.debug("DB hot_rank fallback failed: %s", exc)
    return [], None


# ---------------------------------------------------------------------------
# 2) 大盘指数
# ---------------------------------------------------------------------------
# 5 个指数代码 -> 名称，与 _fetch_indices_from_sina 的 _SINA_INDEX_MAP 一致
_INDICES_TARGETS = {"000001": "上证指数", "399001": "深证成指", "399006": "创业板指", "000688": "科创50", "899050": "北证50"}


async def get_indices(*, tenant_id: str = "public") -> tuple[list[dict], str | None]:
    """返回 (主要大盘指数列表, data_updated_at ISO 或 None)。读路径仅 DB+Redis。"""
    cached = _get_cache("indices")
    if cached is not None:
        return (cached if isinstance(cached, list) else [], None)

    # L2: Redis
    try:
        from src.services.cache_policy_service import get_cached
        l2 = await get_cached("market:indices:all")
        if l2:
            data, updated_at = _parse_redis_data(l2)
            if data:
                _set_cache("indices", data)
                return data, updated_at
    except Exception:
        pass

    # L3: 读库快照（预热写入）
    snap, updated_at = await _get_indices_snapshot_from_db()
    if snap:
        _set_cache("indices", snap)
        return snap, updated_at
    return [], None


async def _fetch_indices_from_sina() -> list[dict] | None:
    """通过新浪实时行情接口获取大盘指数 (备用方案)。"""
    _SINA_INDEX_MAP = {
        "sh000001": ("000001", "上证指数"),
        "sz399001": ("399001", "深证成指"),
        "sz399006": ("399006", "创业板指"),
        "sh000688": ("000688", "科创50"),
        "bj899050": ("899050", "北证50"),
    }
    try:
        import requests as _req
        sess = _req.Session()
        sess.trust_env = False
        sess.headers["Referer"] = "https://finance.sina.com.cn"
        symbols = ",".join(_SINA_INDEX_MAP.keys())
        proxy = _get_proxy_from_context()
        with _proxy_context(proxy):
            resp = sess.get(f"https://hq.sinajs.cn/list={symbols}", timeout=8)
        resp.encoding = "gbk"
        lines = [ln.strip() for ln in resp.text.strip().split("\n") if ln.strip()]
        result = []
        for line in lines:
            # 格式: var hq_str_sh000001="名称,今开,昨收,现价,最高,最低,...,成交量(手),成交额,..."
            try:
                var_part, data_part = line.split("=", 1)
                sina_code = var_part.split("_")[-1]
                if sina_code not in _SINA_INDEX_MAP:
                    continue
                code, name = _SINA_INDEX_MAP[sina_code]
                parts = data_part.strip('";\r\n').split(",")
                if len(parts) < 10:
                    continue
                price = float(parts[3]) if parts[3] else 0.0
                pre_close = float(parts[2]) if parts[2] else 0.0
                change_amt = round(price - pre_close, 4) if pre_close > 0 else 0.0
                change_pct = round(change_amt / pre_close * 100, 2) if pre_close > 0 else 0.0
                volume = int(float(parts[8])) if parts[8] else 0
                turnover = int(float(parts[9])) if parts[9] else 0
                result.append({
                    "code": code,
                    "name": name,
                    "price": price,
                    "change_pct": change_pct,
                    "change_amt": change_amt,
                    "volume": volume,
                    "turnover": turnover,
                })
            except (ValueError, IndexError):
                continue
        if result:
            logger.info("Sina indices fetched: %d items", len(result))
            return result
    except Exception as exc:
        logger.warning("Sina indices failed: %s", exc)
    return None


async def fetch_indices_from_external(*, tenant_id: str = "public") -> list[dict]:
    """仅拉取大盘指数（AKShare/新浪），不写 Redis/DB。供预热专用。"""
    ak = _try_import_ak()
    if ak:
        try:
            timeout = await _resolve_akshare_timeout()
            from src.services.data_service.external_request_executor import run_external_with_retry
            import pandas as pd

            def _fetch_idx_merged():
                dfs = []
                for sym in ("沪深重要指数", "上证系列指数", "深证系列指数"):
                    try:
                        d = ak.stock_zh_index_spot_em(symbol=sym)
                        if d is not None and len(d) > 0:
                            dfs.append(d)
                    except Exception:
                        pass
                if not dfs:
                    return None
                merged = pd.concat(dfs, ignore_index=True)
                return merged.drop_duplicates(subset=["代码"], keep="first") if "代码" in merged.columns else merged

            df = await run_external_with_retry(
                _fetch_idx_merged,
                tenant_id=tenant_id,
                domain="eastmoney.com",
                timeout_seconds=timeout,
            )
            if df is not None and len(df) > 0:
                result = []
                for code, name in _INDICES_TARGETS.items():
                    row = df[df["代码"] == code]
                    if len(row) > 0:
                        r = row.iloc[0]
                        result.append({
                            "code": code,
                            "name": name,
                            "price": float(r.get("最新价", 0) or 0),
                            "change_pct": float(r.get("涨跌幅", 0) or 0),
                            "change_amt": float(r.get("涨跌额", 0) or 0),
                            "volume": int(r.get("成交量", 0) or 0),
                            "turnover": int(r.get("成交额", 0) or 0),
                        })
                if len(result) >= 5:
                    return result
        except Exception as exc:
            logger.debug("fetch_indices_from_external AKShare failed: %s", exc)
    result = await _fetch_indices_from_sina()
    return result if result else []


async def fetch_hot_rank_from_external(*, tenant_id: str = "public") -> list[dict]:
    """仅拉取热门排行（新浪/东财），不写 Redis/DB。供预热专用。"""
    ak = _try_import_ak()
    if not ak:
        return []
    result = []
    try:
        df_sina = await asyncio.to_thread(ak.stock_zh_a_spot)
        if df_sina is not None and len(df_sina) > 0:
            mapped = _map_sina_spot_df_to_common(df_sina)
            if mapped:
                mapped.sort(key=lambda x: (x.get("change_pct") or 0), reverse=True)
                result = mapped[:20]
    except Exception as exc:
        logger.debug("fetch_hot_rank_from_external Sina failed: %s", exc)
    if not result:
        try:
            timeout = await _resolve_akshare_timeout()
            from src.services.data_service.external_request_executor import run_external_with_retry
            df = await run_external_with_retry(
                ak.stock_zh_a_spot_em,
                tenant_id=tenant_id,
                domain="eastmoney.com",
                timeout_seconds=timeout,
                retry_count=3,
            )
            if df is not None and len(df) > 0:
                df = df.sort_values(by="涨跌幅", ascending=False).head(20)
                for _, row in df.iterrows():
                    result.append({
                        "symbol": _safe_str(row.get("代码")),
                        "name": _safe_str(row.get("名称")),
                        "price": _safe_float(row.get("最新价")),
                        "change_pct": _safe_float(row.get("涨跌幅")),
                        "change_amt": _safe_float(row.get("涨跌额")),
                        "volume": _safe_int(row.get("成交量")),
                        "turnover": _safe_int(row.get("成交额")),
                        "turnover_rate": _safe_float(row.get("换手率")),
                    })
        except Exception as exc:
            logger.debug("fetch_hot_rank_from_external EM failed: %s", exc)
    return result


async def fetch_sectors_from_external(sector_type: str = "all", *, tenant_id: str = "public") -> list[dict]:
    """仅拉取板块数据（新浪/东财），不写 Redis/DB。供预热专用。"""
    ak = _try_import_ak()
    if not ak:
        return []
    result = []
    try:
        ind_list, con_list = [], []
        if sector_type in ("industry", "all"):
            try:
                df = ak.stock_sector_spot(indicator="新浪行业")
                ind_list = _map_sina_sector_to_common(df, "industry")
            except Exception:
                pass
        if sector_type in ("concept", "all"):
            try:
                df = ak.stock_sector_spot(indicator="概念")
                con_list = _map_sina_sector_to_common(df, "concept")
            except Exception:
                pass
        result = (ind_list[:12] if ind_list else []) + (con_list[:12] if con_list else [])
    except Exception as exc:
        logger.debug("fetch_sectors_from_external Sina failed: %s", exc)
    if not result:
        try:
            timeout = await _resolve_akshare_timeout()
            from src.services.data_service.external_request_executor import run_external_with_retry
            ind_df = con_df = None
            if sector_type in ("industry", "all"):
                ind_df = await run_external_with_retry(
                    lambda: ak.stock_board_industry_name_em(),
                    tenant_id=tenant_id,
                    domain="eastmoney.com",
                    timeout_seconds=max(timeout, 30.0),
                    retry_count=3,
                )
            if sector_type in ("concept", "all"):
                con_df = await run_external_with_retry(
                    lambda: ak.stock_board_concept_name_em(),
                    tenant_id=tenant_id,
                    domain="eastmoney.com",
                    timeout_seconds=max(timeout, 30.0),
                    retry_count=3,
                )
            if ind_df is not None and len(ind_df) > 0:
                for _, row in ind_df.head(12).iterrows():
                    result.append({
                        "name": _safe_str(row.get("板块名称")),
                        "code": _safe_str(row.get("板块代码")),
                        "type": "industry",
                        "change_pct": _safe_float(row.get("涨跌幅")),
                        "turnover": _safe_int(row.get("成交额")),
                        "leader": _safe_str(row.get("领涨股票")),
                        "leader_pct": _safe_float(row.get("领涨股票-涨跌幅")),
                    })
            if con_df is not None and len(con_df) > 0:
                for _, row in con_df.head(12).iterrows():
                    result.append({
                        "name": _safe_str(row.get("板块名称")),
                        "code": _safe_str(row.get("板块代码")),
                        "type": "concept",
                        "change_pct": _safe_float(row.get("涨跌幅")),
                        "turnover": _safe_int(row.get("成交额")),
                        "leader": _safe_str(row.get("领涨股票")),
                        "leader_pct": _safe_float(row.get("领涨股票-涨跌幅")),
                    })
        except Exception as exc:
            logger.debug("fetch_sectors_from_external EM failed: %s", exc)
    return result


async def fetch_ranking_from_external(
    sort_by: str = "change_pct",
    order: str = "desc",
    limit: int = 30,
    *,
    tenant_id: str = "public",
) -> list[dict]:
    """仅拉取个股排行（新浪/东财），不写 Redis/DB。供预热专用。"""
    ak = _try_import_ak()
    if not ak:
        return []
    result = []
    try:
        df_sina = await asyncio.to_thread(ak.stock_zh_a_spot)
        if df_sina is not None and len(df_sina) > 0:
            mapped = _map_sina_spot_df_to_common(df_sina)
            if mapped:
                rev = order != "asc"
                key_fn = lambda x: (x.get(sort_by) if sort_by in x else 0) or 0
                mapped.sort(key=key_fn, reverse=rev)
                result = mapped[:limit]
                for r in result:
                    r.setdefault("pe", 0)
                    r.setdefault("market_cap", 0)
                    r.setdefault("amplitude", 0)
    except Exception as exc:
        logger.debug("fetch_ranking_from_external Sina failed: %s", exc)
    if not result:
        try:
            timeout = await _resolve_akshare_timeout()
            from src.services.data_service.external_request_executor import run_external_with_retry
            col_map = {"change_pct": "涨跌幅", "turnover": "成交额", "turnover_rate": "换手率", "volume": "成交量"}
            sort_col = col_map.get(sort_by, "涨跌幅")
            ascending = order != "desc"
            df = await run_external_with_retry(
                ak.stock_zh_a_spot_em,
                tenant_id=tenant_id,
                domain="eastmoney.com",
                timeout_seconds=timeout,
                retry_count=3,
            )
            if df is not None and len(df) > 0:
                df = df.sort_values(by=sort_col, ascending=ascending).head(limit)
                for _, row in df.iterrows():
                    result.append({
                        "symbol": _safe_str(row.get("代码")),
                        "name": _safe_str(row.get("名称")),
                        "price": _safe_float(row.get("最新价")),
                        "change_pct": _safe_float(row.get("涨跌幅")),
                        "change_amt": _safe_float(row.get("涨跌额")),
                        "volume": _safe_int(row.get("成交量")),
                        "turnover": _safe_int(row.get("成交额")),
                        "turnover_rate": _safe_float(row.get("换手率")),
                        "pe": _safe_float(row.get("市盈率-动态")),
                        "market_cap": _safe_float(row.get("总市值")),
                        "amplitude": _safe_float(row.get("振幅")),
                    })
        except Exception as exc:
            logger.debug("fetch_ranking_from_external EM failed: %s", exc)
    return result


# ---------------------------------------------------------------------------
# 3) 板块数据 (行业 / 概念) — 新浪主、东财备
# ---------------------------------------------------------------------------


def _map_sina_sector_to_common(sina_df, sector_type: str) -> list[dict]:
    """新浪 DataFrame 映射为统一结构。总成交额文档为万元，×10000 转为元。"""
    if sina_df is None or (hasattr(sina_df, "__len__") and len(sina_df) == 0):
        return []
    result = []
    for _, row in sina_df.iterrows():
        turnover_raw = _safe_float(row.get("总成交额"))
        turnover = int(turnover_raw * 10000) if turnover_raw else 0
        result.append({
            "name": _safe_str(row.get("板块")),
            "code": _safe_str(row.get("label")),
            "type": sector_type,
            "change_pct": _safe_float(row.get("涨跌幅")),
            "turnover": turnover,
            "leader": _safe_str(row.get("股票名称")),
            "leader_pct": _safe_float(row.get("个股-涨跌幅")),
        })
    return result


async def get_sectors(sector_type: str = "all") -> tuple[list[dict], str | None]:
    """返回 (板块数据列表, data_updated_at ISO 或 None)。读路径仅 DB+Redis。"""
    cache_key = f"sectors_{sector_type}"
    cached = _get_cache(cache_key)
    if cached is not None:
        return (cached if isinstance(cached, list) else [], None)

    redis_key = f"market:sectors:{sector_type}"
    try:
        from src.services.cache_policy_service import get_cached
        l2 = await get_cached(redis_key)
        if l2:
            data, updated_at = _parse_redis_data(l2)
            if data:
                _set_cache(cache_key, data)
                return data, updated_at
    except Exception:
        pass

    snap, updated_at = await _get_sectors_snapshot_from_db(sector_type)
    if snap:
        _set_cache(cache_key, snap)
        return snap, updated_at

    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import StockSector
        async for session in get_session():
            q = select(StockSector)
            if sector_type != "all":
                q = q.where(StockSector.sector_type == sector_type)
            q = q.limit(50)
            rows = (await session.execute(q)).scalars().all()
            if rows:
                return [
                    {"name": r.sector_name, "code": r.sector_code, "type": r.sector_type,
                     "change_pct": 0, "turnover": 0, "leader": "", "leader_pct": 0}
                    for r in rows
                ], None
    except Exception as exc:
        logger.debug("DB sector fallback failed: %s", exc)
    return [], None


# ---------------------------------------------------------------------------
# 4) 个股排行 (涨幅/跌幅/成交额/换手率)
# ---------------------------------------------------------------------------

async def get_ranking(sort_by: str = "change_pct", order: str = "desc", limit: int = 30, *, tenant_id: str = "public", force_fetch_external: bool = False) -> tuple[list[dict], str | None]:
    """返回 (个股排行列表, data_updated_at ISO 或 None)。读路径仅 DB+Redis。"""
    cache_key = f"ranking_{sort_by}_{order}_{limit}"
    cached = _get_cache(cache_key)
    if cached is not None:
        return (cached if isinstance(cached, list) else [], None)

    redis_key = f"market:ranking:{sort_by}:{order}:{limit}"
    try:
        from src.services.cache_policy_service import get_cached
        l2 = await get_cached(redis_key)
        if l2:
            data, updated_at = _parse_redis_data(l2)
            if data:
                _set_cache(cache_key, data)
                return data, updated_at
    except Exception:
        pass

    if force_fetch_external:
        return [], None

    try:
        from src.services.cache_policy_service import get_cached as _gc
        stale = await _gc(redis_key)
        if stale:
            data, updated_at = _parse_redis_data(stale)
            if data:
                return data, updated_at
    except Exception:
        pass

    snap, updated_at = await _get_spot_snapshot_from_db(limit=limit, sort_by=sort_by, order=order)
    if snap:
        _set_cache(cache_key, snap)
        return snap, updated_at

    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_data import StockInfo
        async for session in get_session():
            rows = (await session.execute(select(StockInfo).limit(limit))).scalars().all()
            if rows:
                return [{"symbol": r.code, "name": r.name, "price": 0, "change_pct": 0,
                         "volume": 0, "turnover": 0, "turnover_rate": 0} for r in rows], None
    except Exception as exc:
        logger.debug("DB ranking fallback failed: %s", exc)
    return [], None


# ---------------------------------------------------------------------------
# 4b) 预热快照写库 / 读库兜底（Redis 未命中时用最后一次预热数据）
# ---------------------------------------------------------------------------

async def upsert_spot_snapshot(items: list[dict]) -> None:
    """将热门/排行结果写入 market_spot_snapshot，供非交易时读库兜底。按当日覆盖。
    若整批均为 0（无有效行情），跳过写入，避免用全 0 覆盖已有真实数据。"""
    if not items:
        return
    has_valid = any(
        (item.get("price") or 0) != 0 or (item.get("change_pct") or 0) != 0
        for item in items
    )
    if not has_valid:
        logger.debug("upsert_spot_snapshot skipped: all items have zero price/change_pct")
        return
    try:
        from src.core.time_util import now_beijing
        from src.core.db import get_session
        from src.models.market_data import MarketSpotSnapshot
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        snapshot_date = now_beijing().strftime("%Y-%m-%d")
        now = datetime.utcnow()
        rows = []
        for it in items:
            symbol = (it.get("symbol") or it.get("code") or "").strip()
            if not symbol:
                continue
            rows.append({
                "snapshot_date": snapshot_date,
                "symbol": symbol,
                "name": _safe_str(it.get("name"), ""),
                "price": _safe_float(it.get("price")),
                "change_pct": _safe_float(it.get("change_pct")),
                "change_amt": _safe_float(it.get("change_amt")),
                "volume": _safe_float(it.get("volume")),
                "turnover": _safe_float(it.get("turnover")),
                "turnover_rate": _safe_float(it.get("turnover_rate")),
                "updated_at": now,
            })
        if not rows:
            return
        async for session in get_session():
            stmt = mysql_insert(MarketSpotSnapshot).values(rows)
            stmt = stmt.on_duplicate_key_update(
                name=stmt.inserted.name,
                price=stmt.inserted.price,
                change_pct=stmt.inserted.change_pct,
                change_amt=stmt.inserted.change_amt,
                volume=stmt.inserted.volume,
                turnover=stmt.inserted.turnover,
                turnover_rate=stmt.inserted.turnover_rate,
                updated_at=stmt.inserted.updated_at,
            )
            await session.execute(stmt)
            await session.commit()
            break
        logger.debug("upsert_spot_snapshot: %s rows for date %s", len(rows), snapshot_date)
    except Exception as exc:
        logger.warning("upsert_spot_snapshot failed: %s", exc)


async def _get_spot_snapshot_from_db(
    limit: int = 30,
    sort_by: str = "change_pct",
    order: str = "desc",
) -> tuple[list[dict], str | None]:
    """从 market_spot_snapshot 读最新日期的快照。返回 (list, data_updated_at ISO 或 None)。"""
    try:
        from sqlalchemy import select, func, desc, asc
        from src.core.db import get_session
        from src.models.market_data import MarketSpotSnapshot

        col = getattr(MarketSpotSnapshot, sort_by, None) if sort_by in (
            "change_pct", "turnover", "turnover_rate", "volume", "price"
        ) else MarketSpotSnapshot.change_pct
        order_fn = desc if order == "desc" else asc

        async for session in get_session():
            subq = select(func.max(MarketSpotSnapshot.snapshot_date)).select_from(MarketSpotSnapshot).scalar_subquery()
            stmt = (
                select(MarketSpotSnapshot)
                .where(MarketSpotSnapshot.snapshot_date == subq)
                .order_by(order_fn(col))
                .limit(limit)
            )
            rows = (await session.execute(stmt)).scalars().all()
            if rows:
                updated_at = None
                if hasattr(rows[0], "updated_at") and rows[0].updated_at:
                    updated_at = rows[0].updated_at.isoformat() if hasattr(rows[0].updated_at, "isoformat") else str(rows[0].updated_at)
                return [
                    {
                        "symbol": r.symbol,
                        "name": r.name,
                        "price": r.price,
                        "change_pct": r.change_pct,
                        "change_amt": r.change_amt,
                        "volume": r.volume,
                        "turnover": r.turnover,
                        "turnover_rate": r.turnover_rate,
                    }
                    for r in rows
                ], updated_at
            break
    except Exception as exc:
        logger.debug("_get_spot_snapshot_from_db failed: %s", exc)
    return [], None


async def upsert_indices_snapshot(items: list[dict]) -> None:
    """将大盘指数结果写入 market_indices_snapshot，按日保留历史。"""
    if not items:
        return
    try:
        from src.core.time_util import now_beijing
        from src.core.db import get_session
        from src.models.market_data import MarketIndicesSnapshot
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        snapshot_date = now_beijing().strftime("%Y-%m-%d")
        now = datetime.utcnow()
        rows = []
        for it in items:
            code = (it.get("code") or "").strip()
            if not code:
                continue
            rows.append({
                "snapshot_date": snapshot_date,
                "code": code,
                "name": _safe_str(it.get("name"), ""),
                "price": _safe_float(it.get("price")),
                "change_pct": _safe_float(it.get("change_pct")),
                "change_amt": _safe_float(it.get("change_amt")),
                "volume": _safe_float(it.get("volume")),
                "turnover": _safe_float(it.get("turnover")),
                "updated_at": now,
            })
        if not rows:
            return
        async for session in get_session():
            stmt = mysql_insert(MarketIndicesSnapshot).values(rows)
            stmt = stmt.on_duplicate_key_update(
                name=stmt.inserted.name,
                price=stmt.inserted.price,
                change_pct=stmt.inserted.change_pct,
                change_amt=stmt.inserted.change_amt,
                volume=stmt.inserted.volume,
                turnover=stmt.inserted.turnover,
                updated_at=stmt.inserted.updated_at,
            )
            await session.execute(stmt)
            await session.commit()
            break
        logger.debug("upsert_indices_snapshot: %s rows for date %s", len(rows), snapshot_date)
    except Exception as exc:
        logger.warning("upsert_indices_snapshot failed: %s", exc)


async def upsert_sectors_snapshot(items: list[dict]) -> None:
    """将板块结果写入 market_sectors_snapshot，按日保留历史。"""
    if not items:
        return
    try:
        from src.core.time_util import now_beijing
        from src.core.db import get_session
        from src.models.market_data import MarketSectorsSnapshot
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        snapshot_date = now_beijing().strftime("%Y-%m-%d")
        now = datetime.utcnow()
        rows = []
        for it in items:
            code = (it.get("code") or "").strip()
            sector_type = (it.get("type") or "industry").strip() or "industry"
            if not code:
                continue
            # leader/leader_pct 缺失时写空串与 0，避免 NULL 导致「部分字段为空」
            leader = _safe_str(it.get("leader"), "")
            leader_pct_val = _safe_float(it.get("leader_pct"))
            rows.append({
                "snapshot_date": snapshot_date,
                "sector_type": sector_type,
                "code": code,
                "name": _safe_str(it.get("name"), ""),
                "change_pct": _safe_float(it.get("change_pct")),
                "turnover": _safe_float(it.get("turnover")),
                "leader": leader,
                "leader_pct": leader_pct_val,
                "updated_at": now,
            })
        if not rows:
            return
        async for session in get_session():
            stmt = mysql_insert(MarketSectorsSnapshot).values(rows)
            stmt = stmt.on_duplicate_key_update(
                name=stmt.inserted.name,
                change_pct=stmt.inserted.change_pct,
                turnover=stmt.inserted.turnover,
                leader=stmt.inserted.leader,
                leader_pct=stmt.inserted.leader_pct,
                updated_at=stmt.inserted.updated_at,
            )
            await session.execute(stmt)
            await session.commit()
            break
        logger.debug("upsert_sectors_snapshot: %s rows for date %s", len(rows), snapshot_date)
    except Exception as exc:
        logger.warning("upsert_sectors_snapshot failed: %s", exc)


async def upsert_minute_snapshot(symbol: str, data: dict) -> None:
    """将单只股票分时数据写入 market_minute_snapshot，供读路径 L3。唯一 (snapshot_date, symbol) 增量 upsert。"""
    if not data or not isinstance(data.get("bars"), list):
        return
    try:
        from src.core.time_util import now_beijing
        from src.core.db import get_session
        from src.models.market_data import MarketMinuteSnapshot
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        snapshot_date = now_beijing().strftime("%Y-%m-%d")
        bars_json = json.dumps(data.get("bars", []), ensure_ascii=False)
        pre_close = float(data.get("pre_close") or 0)
        code = (symbol or "").strip()
        if not code:
            return
        async for session in get_session():
            stmt = mysql_insert(MarketMinuteSnapshot).values(
                snapshot_date=snapshot_date,
                symbol=code,
                pre_close=pre_close,
                bars=bars_json,
                updated_at=datetime.utcnow(),
            )
            stmt = stmt.on_duplicate_key_update(
                pre_close=stmt.inserted.pre_close,
                bars=stmt.inserted.bars,
                updated_at=stmt.inserted.updated_at,
            )
            await session.execute(stmt)
            await session.commit()
            break
        logger.debug("upsert_minute_snapshot: %s for %s", code, snapshot_date)
    except Exception as exc:
        logger.warning("upsert_minute_snapshot failed for %s: %s", symbol, exc)


async def _get_minute_snapshot_from_db(symbol: str, days: int = 1) -> tuple[dict | None, str | None]:
    """从 market_minute_snapshot 读指定 symbol 最新日期的分时。返回 ({pre_close, bars} 或 None, data_updated_at ISO 或 None)。"""
    try:
        from sqlalchemy import select, func
        from src.core.db import get_session
        from src.models.market_data import MarketMinuteSnapshot

        code = (symbol or "").strip()
        if not code:
            return None, None
        async for session in get_session():
            subq = select(func.max(MarketMinuteSnapshot.snapshot_date)).select_from(MarketMinuteSnapshot).where(MarketMinuteSnapshot.symbol == code).scalar_subquery()
            stmt = select(MarketMinuteSnapshot).where(MarketMinuteSnapshot.symbol == code, MarketMinuteSnapshot.snapshot_date == subq).limit(1)
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            if row:
                bars = json.loads(row.bars) if isinstance(row.bars, str) else (row.bars or [])
                updated_at = None
                if hasattr(row, "updated_at") and row.updated_at:
                    updated_at = row.updated_at.isoformat() if hasattr(row.updated_at, "isoformat") else str(row.updated_at)
                return {"pre_close": float(row.pre_close or 0), "bars": bars}, updated_at
            break
    except Exception as exc:
        logger.debug("_get_minute_snapshot_from_db failed for %s: %s", symbol, exc)
    return None, None


async def _get_indices_snapshot_from_db() -> tuple[list[dict], str | None]:
    """从 market_indices_snapshot 读最新日期的快照。返回 (list, data_updated_at ISO 或 None)。"""
    try:
        from sqlalchemy import select, func
        from src.core.db import get_session
        from src.models.market_data import MarketIndicesSnapshot

        async for session in get_session():
            subq = select(func.max(MarketIndicesSnapshot.snapshot_date)).select_from(MarketIndicesSnapshot).scalar_subquery()
            stmt = select(MarketIndicesSnapshot).where(MarketIndicesSnapshot.snapshot_date == subq).order_by(MarketIndicesSnapshot.code)
            rows = (await session.execute(stmt)).scalars().all()
            if rows:
                updated_at = None
                if hasattr(rows[0], "updated_at") and rows[0].updated_at:
                    updated_at = rows[0].updated_at.isoformat() if hasattr(rows[0].updated_at, "isoformat") else str(rows[0].updated_at)
                return [
                    {
                        "code": r.code,
                        "name": r.name,
                        "price": r.price,
                        "change_pct": r.change_pct,
                        "change_amt": r.change_amt,
                        "volume": int(r.volume) if r.volume is not None else 0,
                        "turnover": int(r.turnover) if r.turnover is not None else 0,
                    }
                    for r in rows
                ], updated_at
            break
    except Exception as exc:
        logger.debug("_get_indices_snapshot_from_db failed: %s", exc)
    return [], None


async def _get_sectors_snapshot_from_db(sector_type: str = "all") -> tuple[list[dict], str | None]:
    """从 market_sectors_snapshot 读最新日期的快照。返回 (list, data_updated_at ISO 或 None)。"""
    try:
        from sqlalchemy import select, func
        from src.core.db import get_session
        from src.models.market_data import MarketSectorsSnapshot

        async for session in get_session():
            subq = select(func.max(MarketSectorsSnapshot.snapshot_date)).select_from(MarketSectorsSnapshot).scalar_subquery()
            stmt = select(MarketSectorsSnapshot).where(MarketSectorsSnapshot.snapshot_date == subq)
            if sector_type != "all":
                stmt = stmt.where(MarketSectorsSnapshot.sector_type == sector_type)
            stmt = stmt.order_by(MarketSectorsSnapshot.sector_type, MarketSectorsSnapshot.code).limit(100)
            rows = (await session.execute(stmt)).scalars().all()
            if rows:
                updated_at = None
                if hasattr(rows[0], "updated_at") and rows[0].updated_at:
                    updated_at = rows[0].updated_at.isoformat() if hasattr(rows[0].updated_at, "isoformat") else str(rows[0].updated_at)
                return [
                    {
                        "name": r.name,
                        "code": r.code,
                        "type": r.sector_type,
                        "change_pct": r.change_pct,
                        "turnover": r.turnover or 0,
                        "leader": r.leader or "",
                        "leader_pct": r.leader_pct if r.leader_pct is not None else 0,
                    }
                    for r in rows
                ], updated_at
            break
    except Exception as exc:
        logger.debug("_get_sectors_snapshot_from_db failed: %s", exc)
    return [], None


# ---------------------------------------------------------------------------
# 5) 股票搜索 (全量 A 股代码/名称列表)
# ---------------------------------------------------------------------------

_STOCK_LIST_CACHE_TTL = 86400.0  # 24 小时


def _get_stock_list_sync() -> List[Dict[str, str]]:
    """(同步) 获取全量 A 股代码/名称列表，缓存 24 小时。"""
    key = "stock_list"
    if key in _cache and (time.time() - _cache_ts.get(key, 0)) < _STOCK_LIST_CACHE_TTL:
        return _cache[key]

    ak = _try_import_ak()
    if ak:
        try:
            # 由 get_stock_list 的 to_thread 外层设置 _proxy_context
            df = ak.stock_info_a_code_name()
            if df is not None and len(df) > 0:
                result = []
                for _, row in df.iterrows():
                    code = _safe_str(row.get("code"))
                    name = _safe_str(row.get("name"))
                    if code and name:
                        result.append({"code": code, "name": name})
                if result:
                    logger.info("Stock list loaded: %d stocks", len(result))
                    _cache["stock_list"] = result
                    _cache_ts["stock_list"] = time.time()
                    return result
        except Exception as exc:
            logger.warning("AKShare stock_info_a_code_name failed: %s", exc)

    return []


async def _load_stock_list_from_db() -> List[Dict[str, str]]:
    """L3: 从 MySQL stock_info 表加载股票列表。"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_data import StockInfo
        async for session in get_session():
            stmt = select(StockInfo.code, StockInfo.name)
            rows = (await session.execute(stmt)).all()
            if rows:
                return [{"code": r.code, "name": r.name} for r in rows]
    except Exception as exc:
        logger.warning("Load stock list from DB failed: %s", exc)
    return []


async def _save_stock_list_to_db(stock_list: List[Dict[str, str]]) -> None:
    """将全量列表持久化到 MySQL stock_info 表 (upsert)。"""
    try:
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert
        from src.core.db import get_session
        from src.models.market_data import StockInfo
        async for session in get_session():
            # 批量 upsert — 兼容 SQLite
            for s in stock_list:
                stmt = sqlite_insert(StockInfo).values(code=s["code"], name=s["name"])
                stmt = stmt.on_conflict_do_update(
                    index_elements=["code"],
                    set_={"name": stmt.excluded.name},
                )
                await session.execute(stmt)
            await session.commit()
            logger.info("Stock list persisted to DB: %d rows", len(stock_list))
    except Exception as exc:
        logger.warning("Save stock list to DB failed: %s", exc)


async def get_stock_list() -> List[Dict[str, str]]:
    """(异步) 获取全量 A 股代码/名称列表 — 四级: 内存 -> Redis -> MySQL -> AKShare。"""
    key = "stock_list"
    if key in _cache and (time.time() - _cache_ts.get(key, 0)) < _STOCK_LIST_CACHE_TTL:
        return _cache[key]

    # L2: Redis
    try:
        from src.services.cache_policy_service import get_cached
        l2 = await get_cached("market:stock_list:all")
        if l2:
            data = json.loads(l2)
            if isinstance(data, list) and data:
                _cache["stock_list"] = data
                _cache_ts["stock_list"] = time.time()
                logger.info("Stock list loaded from Redis: %d stocks", len(data))
                return data
    except Exception:
        pass

    # L3: MySQL
    db_data = await _load_stock_list_from_db()
    if db_data:
        _cache["stock_list"] = db_data
        _cache_ts["stock_list"] = time.time()
        logger.info("Stock list loaded from DB: %d stocks", len(db_data))
        # 回填 Redis
        try:
            from src.services.cache_policy_service import set_cached
            await set_cached("market:stock_list:all", json.dumps(db_data, ensure_ascii=False), ttl=86400)
        except Exception:
            pass
        return db_data

    # L4: AKShare (最慢)
    try:
        timeout = await _resolve_akshare_timeout()
        proxy = _get_proxy_from_context()
        def _run():
            with _proxy_context(proxy):
                return _get_stock_list_sync()
        result = await asyncio.wait_for(asyncio.to_thread(_run), timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("AKShare stock_list timeout after %.0fs", timeout)
        return []

    # 写入 Redis + MySQL
    if result:
        try:
            from src.services.cache_policy_service import set_cached
            await set_cached("market:stock_list:all", json.dumps(result, ensure_ascii=False), ttl=86400)
        except Exception:
            pass
        asyncio.create_task(_save_stock_list_to_db(result))

    return result


async def search_stocks(query: str, limit: int = 20) -> list[dict]:
    """搜索股票，四级优先级匹配: 精确代码 > 代码前缀 > 代码包含 > 名称包含。

    例: 搜索 "00630" 可以匹配到 "000630" (铜陵有色)，优先于 "006300" (龙头股份)。
    """
    q = query.strip().lower()
    if not q:
        return []

    stock_list = await get_stock_list()
    if not stock_list:
        return []

    exact: list[dict] = []
    prefix: list[dict] = []
    code_contains: list[dict] = []
    name_match: list[dict] = []

    for s in stock_list:
        code = s["code"]
        name_lower = s["name"].lower()
        if code == q:
            exact.append(s)
        elif code.startswith(q):
            prefix.append(s)
        elif q in code:
            code_contains.append(s)
        elif q in name_lower:
            name_match.append(s)

    # 合并结果: 精确 > 前缀 > 代码包含 > 名称
    results = exact + prefix + code_contains + name_match
    return results[:limit]


# ---------------------------------------------------------------------------
# 6) 板块详情 (成分股列表)
# ---------------------------------------------------------------------------
_SECTOR_DETAIL_TTL = 120.0  # 板块详情缓存 2 分钟


async def get_sector_detail(code: str, name: str = "") -> dict:
    """获取板块详情: 板块统计 + 成分股列表。
    AKShare 接口:
      - 行业: ak.stock_board_industry_cons_em(symbol=name)
      - 概念: ak.stock_board_concept_cons_em(symbol=name)
    """
    cache_key = f"sector_detail_{code}"
    cached = _get_cache_long(cache_key, _SECTOR_DETAIL_TTL)
    if cached is not None:
        return cached

    # L2: Redis
    try:
        from src.services.cache_policy_service import get_cached
        redis_key = f"market:sector_detail:{code}"
        l2 = await get_cached(redis_key)
        if l2:
            data = json.loads(l2)
            if isinstance(data, dict) and data.get("stocks"):
                _set_cache_long(cache_key, data, _SECTOR_DETAIL_TTL)
                return data
    except Exception:
        pass

    # 判断类型: BK04xx 一般是行业, BK08xx 一般是概念; 也从 name 推断
    sector_type = "concept" if code.startswith("BK08") else "industry"

    ak = _try_import_ak()
    if ak and name:
        try:
            timeout = await _resolve_akshare_timeout()
            proxy = _get_proxy_from_context()
            def _fetch_detail():
                with _proxy_context(proxy):
                    if sector_type == "industry":
                        try:
                            return ak.stock_board_industry_cons_em(symbol=name), "industry"
                        except Exception:
                            return ak.stock_board_concept_cons_em(symbol=name), "concept"
                    else:
                        try:
                            return ak.stock_board_concept_cons_em(symbol=name), "concept"
                        except Exception:
                            return ak.stock_board_industry_cons_em(symbol=name), "industry"

            df, actual_type = await asyncio.wait_for(asyncio.to_thread(_fetch_detail), timeout=timeout)
            if df is not None and len(df) > 0:
                stocks = []
                for _, row in df.iterrows():
                    stocks.append({
                        "code": _safe_str(row.get("代码")),
                        "name": _safe_str(row.get("名称")),
                        "price": _safe_float(row.get("最新价")),
                        "change_pct": _safe_float(row.get("涨跌幅")),
                        "change_amt": _safe_float(row.get("涨跌额")),
                        "volume": _safe_int(row.get("成交量")),
                        "turnover": _safe_int(row.get("成交额")),
                        "turnover_rate": _safe_float(row.get("换手率")),
                        "amplitude": _safe_float(row.get("振幅")),
                    })
                # 查找涨幅最大的作为领涨股
                leader = ""
                leader_pct = 0.0
                if stocks:
                    top = max(stocks, key=lambda s: s.get("change_pct", 0))
                    leader = top.get("name", "")
                    leader_pct = top.get("change_pct", 0)

                # 计算板块涨跌幅（成分股平均涨幅）
                pcts = [s["change_pct"] for s in stocks if s["change_pct"] != 0]
                avg_pct = round(sum(pcts) / len(pcts), 2) if pcts else 0.0

                # 板块总成交额
                total_turnover = sum(s.get("turnover", 0) for s in stocks)

                result = {
                    "code": code,
                    "name": name,
                    "type": actual_type,
                    "change_pct": avg_pct,
                    "turnover": total_turnover,
                    "leader": leader,
                    "leader_pct": leader_pct,
                    "stock_count": len(stocks),
                    "stocks": stocks,
                }
                _set_cache_long(cache_key, result, _SECTOR_DETAIL_TTL)
                # 写入 Redis
                try:
                    from src.services.cache_policy_service import set_cached
                    await set_cached(
                        f"market:sector_detail:{code}",
                        json.dumps(result, ensure_ascii=False),
                        ttl=120,
                    )
                except Exception:
                    pass
                return result
        except Exception as exc:
            logger.warning("AKShare sector detail failed for %s(%s): %s", name, code, exc)

    return {
        "code": code,
        "name": name or code,
        "type": "unknown",
        "change_pct": 0,
        "turnover": 0,
        "leader": "",
        "leader_pct": 0,
        "stock_count": 0,
        "stocks": [],
    }


# ---------------------------------------------------------------------------
# 7) 龙虎榜数据
# ---------------------------------------------------------------------------
_LHB_CACHE_TTL = 300.0  # 龙虎榜数据变化频率低，缓存 5 分钟


async def get_lhb_data(days: int = 3) -> tuple[list[dict], str | None]:
    """返回 (最近 N 天龙虎榜列表, data_updated_at ISO 或 None)。读路径仅 DB+Redis。"""
    cache_key = f"lhb_{days}"
    redis_key = f"market:lhb:{days}"

    cached = _get_cache_long(cache_key, _LHB_CACHE_TTL)
    if cached is not None:
        return (cached if isinstance(cached, list) else [], None)

    try:
        from src.services.cache_policy_service import get_cached
        l2_raw = await get_cached(redis_key)
        if l2_raw:
            data, updated_at = _parse_redis_data(l2_raw)
            if isinstance(data, list) and len(data) > 0:
                _set_cache_long(cache_key, data, _LHB_CACHE_TTL)
                return data, updated_at
    except Exception:
        pass

    result, updated_at = await _get_lhb_from_db(days)
    if result:
        _set_cache_long(cache_key, result, _LHB_CACHE_TTL)
        try:
            from src.services.cache_policy_service import set_cached
            await set_cached(redis_key, json.dumps(result, ensure_ascii=False), ttl=300)
        except Exception:
            pass
        return result, updated_at
    return [], None


async def _get_lhb_from_db(days: int) -> tuple[list[dict], str | None]:
    """从 stock_lhb 表读最近 N 天的龙虎榜。返回 (list, data_updated_at ISO 或 None)。"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import StockLHB

        since = (datetime.now() - timedelta(days=days + 5)).strftime("%Y-%m-%d")
        async for session in get_session():
            stmt = (
                select(StockLHB)
                .where(StockLHB.trade_date >= since)
                .order_by(StockLHB.trade_date.desc(), StockLHB.id.desc())
                .limit(500)
            )
            rows = (await session.execute(stmt)).scalars().all()
            if not rows:
                break
            updated_at = None
            if hasattr(rows[0], "updated_at") and rows[0].updated_at:
                updated_at = rows[0].updated_at.isoformat() if hasattr(rows[0].updated_at, "isoformat") else str(rows[0].updated_at)
            result = []
            for r in rows:
                result.append({
                    "code": str(r.symbol or ""),
                    "name": str(r.symbol_name or ""),
                    "date": str(r.trade_date or ""),
                    "close": float(r.close_price or 0),
                    "change_pct": float(r.change_pct or 0),
                    "net_buy": float(r.net_buy or 0),
                    "buy_amt": float(r.buy_amount or 0),
                    "sell_amt": float(r.sell_amount or 0),
                    "reason": str(r.reason or ""),
                })
            return result, updated_at
    except Exception as exc:
        logger.debug("_get_lhb_from_db failed: %s", exc)
    return [], None


# ---------------------------------------------------------------------------
# 7) 机构持仓 / 北向资金
# ---------------------------------------------------------------------------
_INST_CACHE_TTL = 300.0


def _row_to_inst_item(r: Any) -> dict:
    """NorthboundHoldStock 或 DataFrame row 转为前端所需格式"""
    def _v(attr: str, key: str, default: Any = 0):
        if hasattr(r, attr):
            return getattr(r, attr, default)
        return r.get(key, default) if hasattr(r, "get") else default
    return {
        "code": str(_v("code", "代码", "")),
        "name": str(_v("name", "名称", "") or ""),
        "close": float(_v("close", "今日收盘价") or 0),
        "change_pct": float(_v("change_pct", "今日涨跌幅") or 0),
        "hold_shares": float(_v("hold_shares", "持股股数") or 0),
        "hold_value": float(_v("hold_value", "持股市值") or 0),
        "float_ratio": float(_v("float_ratio", "持股数量占A股百分比") or 0),
        "increase_shares": float(_v("increase_shares", "增持股数") or 0),
        "increase_value": float(_v("increase_value", "增持市值") or 0),
        "sector": str(_v("sector", "所属板块", "") or ""),
    }


async def get_institutional_data(
    market: str = "北向",
    indicator: str = "今日排行",
) -> tuple[list[dict], str | None]:
    """返回 (北向/机构持仓列表, data_updated_at ISO 或 None)。读路径仅 DB+Redis。"""
    cache_key = f"inst_{market}_{indicator}"
    redis_key = f"market:inst:{market}:{indicator}"

    cached = _get_cache_long(cache_key, _INST_CACHE_TTL)
    if cached is not None:
        return (cached if isinstance(cached, list) else [], None)

    try:
        from src.services.cache_policy_service import get_cached
        l2_raw = await get_cached(redis_key)
        if l2_raw:
            data, updated_at = _parse_redis_data(l2_raw)
            if isinstance(data, list) and len(data) > 0:
                _set_cache_long(cache_key, data, _INST_CACHE_TTL)
                return data, updated_at
    except Exception:
        pass

    try:
        from sqlalchemy import select, func
        from src.core.db import get_session
        from src.models.market_sync import NorthboundHoldStock

        async for session in get_session():
            sub = select(func.max(NorthboundHoldStock.trade_date)).where(
                NorthboundHoldStock.market == market,
                NorthboundHoldStock.indicator == indicator,
            )
            result = await session.execute(sub)
            latest_date = result.scalar_one_or_none()
            if latest_date is None:
                break
            # 部分驱动/环境返回 Row，需取首列得到日期字符串
            if hasattr(latest_date, "__getitem__") and not isinstance(latest_date, str):
                latest_date = latest_date[0] if len(latest_date) else None
            if not latest_date:
                break
            latest_date_str = str(latest_date).strip()
            stmt = (
                select(NorthboundHoldStock)
                .where(
                    NorthboundHoldStock.market == market,
                    NorthboundHoldStock.indicator == indicator,
                    NorthboundHoldStock.trade_date == latest_date_str,
                )
                .order_by(NorthboundHoldStock.hold_value.desc())
                .limit(100)
            )
            rows = (await session.execute(stmt)).scalars().all()
            if rows:
                updated_at = None
                if hasattr(rows[0], "updated_at") and rows[0].updated_at:
                    updated_at = rows[0].updated_at.isoformat() if hasattr(rows[0].updated_at, "isoformat") else str(rows[0].updated_at)
                result_list = [_row_to_inst_item(r) for r in rows]
                _set_cache_long(cache_key, result_list, _INST_CACHE_TTL)
                try:
                    from src.services.cache_policy_service import set_cached
                    await set_cached(redis_key, json.dumps(result_list, ensure_ascii=False), ttl=300)
                except Exception:
                    pass
                return result_list, updated_at
            break
    except Exception as exc:
        logger.warning("get_institutional_data L3 DB read failed: %s", exc, exc_info=True)
    return [], None


# ---------------------------------------------------------------------------
# 长 TTL 缓存辅助 (用于龙虎榜/机构持仓等低频数据)
# ---------------------------------------------------------------------------

def _get_cache_long(key: str, ttl: float):
    if key in _cache and (time.time() - _cache_ts.get(key, 0)) < ttl:
        return _cache[key]
    return None


def _set_cache_long(key: str, val: Any, ttl: float):  # noqa: ARG001 — ttl 仅语义提示
    _cache[key] = val
    _cache_ts[key] = time.time()
