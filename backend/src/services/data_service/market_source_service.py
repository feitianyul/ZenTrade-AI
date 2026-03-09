"""行情数据源管理 & 分类探测服务"""

import asyncio
import json
import os
import time
import logging
import threading
from typing import Any, Dict, Iterable, List

from src.schemas.market import MarketQuote

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 代理绕过工具（公共模块，与 data_sync 兼容）
# ---------------------------------------------------------------------------
from src.services.data_service.proxy_executor import (
    get_proxy_from_context as _get_proxy_from_context,
    no_proxy as _no_proxy,
    proxy_context as _proxy_context,
)

# ---------------------------------------------------------------------------
# 全局限速器 — 防止频繁请求被限流/封 IP；新浪与东财分开，避免双源并发打爆同一源
# ---------------------------------------------------------------------------
_ak_lock = threading.Lock()
_ak_last_call: float = 0.0
_AK_MIN_INTERVAL = float(os.environ.get("AK_MIN_INTERVAL", "1.0"))  # 东财/akshare 最小间隔(秒)，可调大防限流

_sina_lock = threading.Lock()
_sina_last_call: float = 0.0
_SINA_MIN_INTERVAL = float(os.environ.get("SINA_MIN_INTERVAL", "1.0"))  # 新浪源最小间隔(秒)


def _ak_rate_limit():
    """东财/AKShare 限速: 连续请求间隔 >= AK_MIN_INTERVAL。"""
    global _ak_last_call
    with _ak_lock:
        now = time.time()
        elapsed = now - _ak_last_call
        if elapsed < _AK_MIN_INTERVAL:
            time.sleep(_AK_MIN_INTERVAL - elapsed)
        _ak_last_call = time.time()


def _sina_rate_limit():
    """新浪源限速: 连续请求间隔 >= SINA_MIN_INTERVAL，避免打爆 quotes.sina.cn。"""
    global _sina_last_call
    with _sina_lock:
        now = time.time()
        elapsed = now - _sina_last_call
        if elapsed < _SINA_MIN_INTERVAL:
            time.sleep(_SINA_MIN_INTERVAL - elapsed)
        _sina_last_call = time.time()

# ---------------------------------------------------------------------------
# 内存缓存 (各接口独立 TTL)
# ---------------------------------------------------------------------------
_cache: Dict[str, Any] = {}
_cache_ts: Dict[str, float] = {}


def _get_cache(key: str, ttl: float):
    if key in _cache and (time.time() - _cache_ts.get(key, 0)) < ttl:
        return _cache[key]
    return None


def _set_cache(key: str, val: Any):
    _cache[key] = val
    _cache_ts[key] = time.time()


# Cache TTL 配置 (默认值, 可被配置中心覆盖)
_TTL_QUOTE = 5.0        # 实时报价 5s
_TTL_KLINE = 60.0       # 日/周/月K 60s
_TTL_MINUTE = 10.0      # 分时数据 10s
_TTL_FUNDAMENTAL = 3600.0  # 基本面 1h
_TTL_NEWS = 300.0       # 资讯 5min

# ---------------------------------------------------------------------------
# 配置中心动态数据源
# ---------------------------------------------------------------------------
_market_config: Dict[str, Any] = {}
_market_config_ts: float = 0
_MARKET_CONFIG_TTL = 60.0  # 60s 重载一次


async def _load_market_config() -> Dict[str, Any]:
    """从配置中心加载行情数据源配置, 缓存 60s。"""
    global _market_config, _market_config_ts
    if time.time() - _market_config_ts < _MARKET_CONFIG_TTL and _market_config:
        return _market_config
    try:
        from src.services.config_center_service import get_config
        cfg: Dict[str, Any] = {}
        for key in (
            "market_source", "market_backup_source", "market_refresh_interval",
            "market_api_key", "market_backup_api_key",
            "market_non_trading_reject_external",
            "market_quote_depth_ttl_trading", "market_quote_depth_ttl_non_trading",
        ):
            r = await get_config("public", "default", key)
            if r:
                cfg[key] = r.get("value", r) if isinstance(r, dict) else r
        if cfg:
            _market_config = cfg
            _market_config_ts = time.time()
            logger.info("Market config reloaded: %s", cfg)
    except Exception as exc:
        logger.debug("Load market config failed (using defaults): %s", exc)
    return _market_config


async def _get_refresh_interval() -> float:
    """获取配置的刷新间隔 (秒)，用于覆盖内存缓存 TTL。"""
    cfg = await _load_market_config()
    try:
        return float(cfg.get("market_refresh_interval", _TTL_QUOTE))
    except (TypeError, ValueError):
        return _TTL_QUOTE


async def _market_non_trading_reject_external() -> bool:
    """已取消：不再根据交易时间拒绝外部 API，始终返回 False。"""
    return False


async def _get_source_priority() -> tuple[str, str]:
    """返回 (主数据源, 备用数据源)。
    默认: primary=sina, backup=akshare
    """
    cfg = await _load_market_config()
    primary = str(cfg.get("market_source", "sina")).lower()
    backup = str(cfg.get("market_backup_source", "akshare")).lower()
    return primary, backup

# ---------------------------------------------------------------------------
# 数据源定义
# ---------------------------------------------------------------------------

MARKET_SOURCES = {
    "akshare": {"name": "AKShare (免费)", "needs_key": False},
}

# 测试分类定义 — 与 SYNC_CATEGORIES 对齐 (13 项)
PROBE_CATEGORIES = [
    {"id": "stock_list",   "name": "A股列表",          "desc": "沪深京A股全量代码",         "icon": "fa-list"},
    {"id": "kline",        "name": "K线行情",          "desc": "获取 000001.SZ 最近5天K线", "icon": "fa-chart-bar"},
    {"id": "quote",        "name": "实时报价",         "desc": "获取 000001.SZ 实时报价",    "icon": "fa-bolt"},
    {"id": "fundamental",  "name": "财务指标",         "desc": "ROE/EPS等财务分析数据",      "icon": "fa-file-invoice-dollar"},
    {"id": "technical",    "name": "技术面",           "desc": "技术指标(MA/MACD)",          "icon": "fa-wave-square"},
    {"id": "margin",       "name": "融资融券",         "desc": "融资余额/融券数据",           "icon": "fa-balance-scale"},
    {"id": "block_trade",  "name": "大宗交易",         "desc": "买卖营业部/溢价率",           "icon": "fa-handshake"},
    {"id": "capital_flow", "name": "资金流向",         "desc": "主力/大单净流入",             "icon": "fa-money-bill-wave"},
    {"id": "dividend",     "name": "分红配股",         "desc": "送转派息方案",                "icon": "fa-gift"},
    {"id": "sector",       "name": "行业/概念板块",    "desc": "板块列表及成分股",            "icon": "fa-sitemap"},
    {"id": "hot_rank",     "name": "龙虎榜/热门榜单",  "desc": "当日龙虎榜与热门榜单",        "icon": "fa-fire"},
    {"id": "institutional","name": "机构持仓/北向资金", "desc": "北向持股数据",                "icon": "fa-university"},
    {"id": "announcement", "name": "公告/涨跌停",      "desc": "最新公告与涨跌停池",          "icon": "fa-bullhorn"},
]


def list_sources() -> Iterable[str]:
    return ["primary", "backup"]


# ---------------------------------------------------------------------------
# 实时报价 — 新浪直连 (最快, <200ms)
# ---------------------------------------------------------------------------

def _normalize_symbol(symbol: str) -> str:
    """统一代码格式: '300251.SZ' / 'SZ300251' / 'sz300251' / '300251' -> '300251'"""
    s = symbol.strip().upper()
    # 去掉后缀: 300251.SZ -> 300251
    if '.' in s:
        s = s.split('.')[0]
    # 去掉前缀: SZ300251 -> 300251
    for prefix in ('SH', 'SZ', 'BJ'):
        if s.startswith(prefix) and len(s) > 2:
            s = s[2:]
            break
    return s


def _strip_suffix(symbol: str) -> str:
    """Remove .SH / .SZ suffix -> pure numeric code. (兼容别名)"""
    return _normalize_symbol(symbol)


def _sina_prefix(code: str) -> str:
    """根据代码判断新浪前缀 sh/sz。"""
    if code.startswith(("0", "3", "2")):
        return "sz"
    return "sh"


def _symbol_to_sina(symbol: str) -> str:
    """6 位代码转新浪所需前缀: 上交所 5/6→sh，深交所 0/3→sz。仅支持上交所与深交所。"""
    s = (symbol or "").strip()
    if not s or len(s) < 6:
        return s
    if s.startswith(("sh", "sz")):
        return s.lower()
    if s[0] in "56":
        return "sh" + s
    if s[0] in "03":
        return "sz" + s
    return "sz" + s


def _primary_source_by_exchange(symbol: str) -> str:
    """按交易所分流，仅上交所与深交所：上交所(5/6)→em(东财)，深交所(0/3)→sina(新浪)。用于实时行情、分时、分钟K 双源主主+互备。"""
    s = (symbol or "").strip()
    if not s:
        return "em"
    if s.startswith("sh"):
        return "em"
    if s.startswith("sz"):
        return "sina"
    if len(s) >= 1:
        if s[0] in "56":
            return "em"
        if s[0] in "03":
            return "sina"
    return "em"


def _fetch_quote_sync(symbol: str) -> dict[str, Any] | None:
    """(同步) 新浪实时报价。代理由调用方通过 _proxy_context 设置。"""
    _sina_rate_limit()
    try:
        import requests as _req
        code = _strip_suffix(symbol)
        sina_symbol = f"{_sina_prefix(code)}{code}"

        sess = _req.Session()
        sess.trust_env = False
        sess.headers["Referer"] = "https://finance.sina.com.cn"
        resp = sess.get(f"https://hq.sinajs.cn/list={sina_symbol}", timeout=5)
        resp.encoding = "gbk"
        text = resp.text.strip()
        parts = text.split('"')[1].split(",") if '"' in text else []
        if len(parts) >= 32:
            name = parts[0]
            price = float(parts[3]) if parts[3] else 0.0
            pre_close = float(parts[2]) if parts[2] else 0.0
            change = round(price - pre_close, 4) if pre_close > 0 else 0.0
            change_pct = round(change / pre_close * 100, 2) if pre_close > 0 else 0.0
            volume = float(parts[8]) if parts[8] else 0.0
            amount = float(parts[9]) if parts[9] else 0.0
            return MarketQuote(
                symbol=symbol,
                price=price,
                change=change_pct,
                volume=volume,
                name=name,
                open=float(parts[1]) if parts[1] else 0.0,
                high=float(parts[4]) if parts[4] else 0.0,
                low=float(parts[5]) if parts[5] else 0.0,
                pre_close=pre_close,
                amount=amount,
            ).model_dump()
    except Exception as exc:
        logger.warning("fetch_quote sina failed for %s: %s", symbol, exc)
    return None


def _fetch_quote_akshare_sync(symbol: str) -> dict[str, Any] | None:
    """备用: AKShare stock_zh_a_spot_em 获取单只报价。"""
    _ak_rate_limit()
    code = _normalize_symbol(symbol)
    try:
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        row = df[df["代码"] == code]
        if len(row) > 0:
            r = row.iloc[0]
            price = float(r.get("最新价", 0) or 0)
            pre_close = float(r.get("昨收", 0) or 0)
            change = round(price - pre_close, 4) if pre_close > 0 else 0.0
            change_pct = round(change / pre_close * 100, 2) if pre_close > 0 else 0.0
            return MarketQuote(
                symbol=symbol,
                price=price,
                change=change_pct,
                volume=float(r.get("成交量", 0) or 0),
                name=str(r.get("名称", "")),
                open=float(r.get("今开", 0) or 0),
                high=float(r.get("最高", 0) or 0),
                low=float(r.get("最低", 0) or 0),
                pre_close=pre_close,
                amount=float(r.get("成交额", 0) or 0),
            ).model_dump()
    except Exception as exc:
        logger.warning("fetch_quote akshare failed for %s: %s", symbol, exc)
    return None


async def fetch_quote(symbol: str, *, skip_cache: bool = False, tenant_id: str = "public") -> dict[str, Any]:
    """获取实时报价 — 动态数据源 + 三级降级 -> Redis stale -> 空 mock 标记。skip_cache=True 时跳过 L1/L2 强制拉源。"""
    code = _normalize_symbol(symbol)
    redis_key = f"market:quote:{code}"
    cache_key = f"quote_{code}"
    refresh_interval = await _get_refresh_interval()

    if not skip_cache:
        # L1: 内存缓存
        cached = _get_cache(cache_key, refresh_interval)
        if cached is not None:
            return cached

        # L2: Redis 缓存
        try:
            from src.services.cache_policy_service import get_cached
            l2_raw = await get_cached(redis_key)
            if l2_raw:
                data = json.loads(l2_raw)
                _set_cache(cache_key, data)
                return data
        except Exception:
            pass

    # 非交易时间：不请求外部 API，仅返回 Redis 过期数据或 K 线兜底（Phase 1）
    from src.services.data_service.exchange_time_utils import is_trading_time
    trading = await is_trading_time()
    if not trading and await _market_non_trading_reject_external():
        try:
            from src.services.cache_policy_service import get_cached as _gc
            stale = await _gc(redis_key)
            if stale:
                data = json.loads(stale)
                data["_stale"] = True
                return data
        except Exception:
            pass
        from src.services.data_service.kline_fallback import build_quote_from_kline
        fallback = await build_quote_from_kline(symbol, code)
        if fallback:
            return fallback
        return {"symbol": symbol, "price": 0, "change": 0, "volume": 0, "_unavailable": True, "_error": "非交易时间，暂无实时数据"}

    # 双源主主+互备：按交易所分流：按交易所分流；使用 run_external_with_retry（代理池+超时+重试）（Phase 1）
    from src.services.data_service.external_request_executor import run_external_with_retry
    primary = _primary_source_by_exchange(code)
    first_src = "sina" if primary == "sina" else "akshare"
    second_src = "akshare" if first_src == "sina" else "sina"
    result = None
    for src_name in (first_src, second_src):
        try:
            if src_name == "sina":
                result = await run_external_with_retry(
                    lambda s=symbol: _fetch_quote_sync(s),
                    tenant_id=tenant_id,
                    domain="finance.sina.com.cn",
                    rate_limit_fn=_sina_rate_limit,
                )
            else:
                result = await run_external_with_retry(
                    lambda s=symbol: _fetch_quote_akshare_sync(s),
                    tenant_id=tenant_id,
                    domain="eastmoney.com",
                    rate_limit_fn=_ak_rate_limit,
                )
            if result:
                logger.debug("Quote for %s from %s", symbol, src_name)
                break
        except Exception as e:
            logger.debug("Quote %s from %s failed: %s", symbol, src_name, e)

    if result:
        _set_cache(cache_key, result)
        # 写入 Redis
        try:
            from src.services.cache_policy_service import set_cached
            await set_cached(redis_key, json.dumps(result, ensure_ascii=False), ttl=int(refresh_interval))
        except Exception:
            pass
        return result

    # 降级: Redis stale (即使过期也尝试读取)
    try:
        from src.services.cache_policy_service import get_cached as _gc
        stale = await _gc(redis_key)
        if stale:
            data = json.loads(stale)
            data["_stale"] = True
            return data
    except Exception:
        pass

    # 最终: 空报价 + _unavailable 标记
    return {"symbol": symbol, "price": 0, "change": 0, "volume": 0, "_unavailable": True, "_error": "所有数据源均不可用，请稍后重试"}


# ---------------------------------------------------------------------------
# 五档深度行情 — 新浪实时盘口
# ---------------------------------------------------------------------------

def _fetch_depth_sync(symbol: str) -> Dict[str, Any] | None:
    """(同步) 新浪五档盘口数据。
    hq.sinajs.cn 返回中:
      parts[10]-parts[19]: 买一~买五 (价格,数量 交替)
      parts[20]-parts[29]: 卖一~卖五 (价格,数量 交替)
    买卖盘格式: [价格, 数量(手)] × 5
    """
    _sina_rate_limit()
    try:
        import requests as _req
        code = _normalize_symbol(symbol)
        sina_symbol = f"{_sina_prefix(code)}{code}"
        sess = _req.Session()
        sess.trust_env = False
        sess.headers["Referer"] = "https://finance.sina.com.cn"
        resp = sess.get(f"https://hq.sinajs.cn/list={sina_symbol}", timeout=5)
        resp.encoding = "gbk"
        text = resp.text.strip()
        parts = text.split('"')[1].split(",") if '"' in text else []
        if len(parts) >= 30:
            bids = []
            asks = []
            # 买一~买五: parts[10]=买一报价, parts[11]=买一数量, ...
            for i in range(5):
                bp_idx = 10 + i * 2
                bq_idx = 11 + i * 2
                bp = float(parts[bp_idx]) if parts[bp_idx] else 0.0
                bq = int(float(parts[bq_idx])) if parts[bq_idx] else 0
                if bp > 0:
                    bids.append([bp, bq])
            # 卖一~卖五: parts[20]=卖一报价, parts[21]=卖一数量, ...
            for i in range(5):
                ap_idx = 20 + i * 2
                aq_idx = 21 + i * 2
                ap = float(parts[ap_idx]) if parts[ap_idx] else 0.0
                aq = int(float(parts[aq_idx])) if parts[aq_idx] else 0
                if ap > 0:
                    asks.append([ap, aq])
            if bids or asks:
                return {"symbol": symbol, "bids": bids, "asks": asks}
    except Exception as exc:
        logger.warning("fetch_depth sina failed for %s: %s", symbol, exc)
    return None


async def fetch_depth(symbol: str, *, tenant_id: str = "public") -> Dict[str, Any]:
    """获取五档盘口深度数据 — L1 内存 -> L2 Redis -> 外部API -> 写回。"""
    code = _normalize_symbol(symbol)
    cache_key = f"depth_{code}"
    redis_key = f"market:depth:{code}"

    # L1: 内存缓存 (3s)
    cached = _get_cache(cache_key, 3.0)
    if cached is not None:
        return cached

    # L2: Redis 缓存 (5s)
    try:
        from src.services.cache_policy_service import get_cached
        l2_raw = await get_cached(redis_key)
        if l2_raw:
            data = json.loads(l2_raw)
            if isinstance(data, dict) and (data.get("bids") or data.get("asks")):
                _set_cache(cache_key, data)
                return data
    except Exception:
        pass

    # 非交易时间：不请求外部 API，仅返回 Redis 过期数据（Phase 1）
    from src.services.data_service.exchange_time_utils import is_trading_time
    if not await is_trading_time() and await _market_non_trading_reject_external():
        try:
            from src.services.cache_policy_service import get_cached as _gc
            stale = await _gc(redis_key)
            if stale:
                data = json.loads(stale)
                if isinstance(data, dict):
                    data["_stale"] = True
                    return data
        except Exception:
            pass
        return {"symbol": symbol, "bids": [], "asks": [], "_unavailable": True, "_error": "非交易时间，暂无实时数据"}

    # L3: 外部 API（run_external_with_retry：代理池+超时+重试）（Phase 1）
    from src.services.data_service.external_request_executor import run_external_with_retry
    result = None
    try:
        result = await run_external_with_retry(
            lambda: _fetch_depth_sync(symbol),
            tenant_id=tenant_id,
            domain="finance.sina.com.cn",
            rate_limit_fn=_sina_rate_limit,
        )
    except Exception as e:
        logger.debug("fetch_depth run_external_with_retry failed for %s: %s", symbol, e)
    if result:
        _set_cache(cache_key, result)
        try:
            from src.services.cache_policy_service import set_cached
            await set_cached(redis_key, json.dumps(result, ensure_ascii=False), ttl=5)
        except Exception:
            pass
        return result

    return {"symbol": symbol, "bids": [], "asks": [], "_unavailable": True, "_error": "所有数据源均不可用，请稍后重试"}


# ---------------------------------------------------------------------------
# K-Line Data (日/周/月 + 分钟级)
# ---------------------------------------------------------------------------

def _fetch_kline_daily_sync(symbol: str, period: str, count: int) -> List[Dict[str, Any]] | None:
    """(同步) 日/周/月 K线 — AKShare stock_zh_a_hist (东方财富源，支持 daily/weekly/monthly)。"""
    _ak_rate_limit()
    try:
        import akshare as ak
        code = _strip_suffix(symbol)
        # stock_zh_a_hist 的 period 参数: "daily" / "weekly" / "monthly"
        ak_adjust = "qfq"
        df = ak.stock_zh_a_hist(symbol=code, period=period, adjust=ak_adjust)
        if df is not None and len(df) > 0:
            df = df.tail(count)
            bars = []
            for _, row in df.iterrows():
                # stock_zh_a_hist 返回中文列名
                bars.append({
                    "date": str(row.get("日期", row.get("date", ""))),
                    "open": float(row.get("开盘", row.get("open", 0))),
                    "high": float(row.get("最高", row.get("high", 0))),
                    "low": float(row.get("最低", row.get("low", 0))),
                    "close": float(row.get("收盘", row.get("close", 0))),
                    "volume": float(row.get("成交量", row.get("volume", 0))),
                    "turnover": float(row.get("成交额", row.get("amount", 0))) if ("成交额" in row.index or "amount" in row.index) else None,
                })
            return bars
    except Exception as exc:
        logger.warning("fetch_kline akshare failed for %s/%s: %s", symbol, period, exc)
    return None


def _fetch_kline_minute_em_sync(symbol: str, period_min: str, count: int) -> List[Dict[str, Any]] | None:
    """分钟K线 — 东财 stock_zh_a_hist_min_em。"""
    _ak_rate_limit()
    try:
        import akshare as ak
        code = _strip_suffix(symbol)
        df = ak.stock_zh_a_hist_min_em(symbol=code, period=period_min, adjust="")
        if df is not None and len(df) > 0:
            df = df.tail(count)
            bars = []
            for _, row in df.iterrows():
                t_str = str(row.get("时间", row.name))
                bars.append({
                    "date": t_str,
                    "open": float(row.get("开盘", 0)),
                    "high": float(row.get("最高", 0)),
                    "low": float(row.get("最低", 0)),
                    "close": float(row.get("收盘", 0)),
                    "volume": float(row.get("成交量", 0)),
                    "turnover": float(row.get("成交额", 0)) if "成交额" in row.index else None,
                })
            return bars
    except Exception as exc:
        logger.warning("fetch_kline_minute em failed for %s/%smin: %s", symbol, period_min, exc)
    return None


def _fetch_kline_minute_sina_sync(symbol: str, period_min: str, count: int) -> List[Dict[str, Any]] | None:
    """分钟K线 — 新浪 stock_zh_a_minute。列 day/open/high/low/close/volume。"""
    _ak_rate_limit()
    try:
        import akshare as ak
        sina_sym = _symbol_to_sina(symbol)
        df = ak.stock_zh_a_minute(symbol=sina_sym, period=period_min, adjust="")
        if df is None or len(df) == 0:
            return None
        df = df.tail(count)
        bars = []
        for _, row in df.iterrows():
            day_val = row.get("day", row.name)
            t_str = str(day_val) if day_val else ""
            bars.append({
                "date": t_str,
                "open": float(row.get("open", 0)),
                "high": float(row.get("high", 0)),
                "low": float(row.get("low", 0)),
                "close": float(row.get("close", 0)),
                "volume": float(row.get("volume", 0)),
                "turnover": None,
            })
        return bars
    except Exception as exc:
        logger.warning("fetch_kline_minute sina failed for %s/%smin: %s", symbol, period_min, exc)
    return None


def _fetch_kline_minute_sync(symbol: str, period_min: str, count: int) -> List[Dict[str, Any]] | None:
    """分钟K线 — 双源主主+互备：按交易所分流，东财/新浪，失败换另一源。"""
    code = _normalize_symbol(symbol)
    primary = _primary_source_by_exchange(code)
    bars = _fetch_kline_minute_em_sync(symbol, period_min, count) if primary == "em" else _fetch_kline_minute_sina_sync(symbol, period_min, count)
    if bars:
        return bars
    bars = _fetch_kline_minute_sina_sync(symbol, period_min, count) if primary == "em" else _fetch_kline_minute_em_sync(symbol, period_min, count)
    return bars


async def fetch_kline(symbol: str, period: str = "daily", count: int = 60) -> List[Dict[str, Any]]:
    """Fetch K-line data. period: daily/weekly/monthly/1min/5min/15min/30min/60min.
    三级缓存: L1 内存 -> L2 Redis -> L3 ClickHouse(日K) / 外部API -> 写回全部层。
    """
    code = _normalize_symbol(symbol)
    cache_key = f"kline_{code}_{period}_{count}"
    ttl = _TTL_MINUTE if "min" in period else _TTL_KLINE
    redis_ttl = 60 if "min" in period else 3600

    # L1: 内存缓存
    cached = _get_cache(cache_key, ttl)
    if cached is not None:
        return cached

    # L2: Redis 缓存
    redis_key = f"market:kline:{code}:{period}"
    try:
        from src.services.cache_policy_service import get_cached
        l2_raw = await get_cached(redis_key)
        if l2_raw:
            data = json.loads(l2_raw)
            if isinstance(data, list) and data:
                _set_cache(cache_key, data)
                return data
    except Exception:
        pass

    # L3a: ClickHouse (仅日/周/月K)
    if period in ("daily", "weekly", "monthly"):
        try:
            from src.services.data_service.kline_storage import load_kline_from_ch
            ch_bars = await load_kline_from_ch(code, period, count)
            if ch_bars:
                _set_cache(cache_key, ch_bars)
                try:
                    from src.services.cache_policy_service import set_cached
                    await set_cached(redis_key, json.dumps(ch_bars, ensure_ascii=False), ttl=redis_ttl)
                except Exception:
                    pass
                return ch_bars
        except Exception as exc:
            logger.debug("ClickHouse kline load failed: %s", exc)

    # L4: 外部 API
    bars = None
    proxy = _get_proxy_from_context()
    def _run_kline():
        with _proxy_context(proxy):
            if period in ("daily", "weekly", "monthly"):
                return _fetch_kline_daily_sync(symbol, period, count)
            period_min = period.replace("min", "") if period.endswith("min") else ""
            return _fetch_kline_minute_sync(symbol, period_min, count) if period_min else None
    bars = await asyncio.to_thread(_run_kline)

    if bars:
        _set_cache(cache_key, bars)
        # 写回 Redis
        try:
            from src.services.cache_policy_service import set_cached
            await set_cached(redis_key, json.dumps(bars, ensure_ascii=False), ttl=redis_ttl)
        except Exception:
            pass
        # 写回 ClickHouse (仅日/周/月K, 后台异步)
        if period in ("daily", "weekly", "monthly"):
            try:
                from src.services.data_service.kline_storage import save_kline_to_ch
                asyncio.create_task(save_kline_to_ch(code, period, bars))
            except Exception:
                pass
        return bars

    # 最终: 返回空 list，不可用信息由路由层通过 KlineResponse.unavailable/message 传递
    return []


# ---------------------------------------------------------------------------
# Minute / Time-sharing Data (分时/五日分时) — 双源主主+互备：东财 + 新浪
# ---------------------------------------------------------------------------

def _fetch_minute_em_sync(symbol: str, count: int, days: int = 1) -> Dict[str, Any] | None:
    """分时 — 东财 stock_zh_a_hist_min_em。"""
    _ak_rate_limit()
    try:
        import akshare as ak
        code = _strip_suffix(symbol)
        total_count = count * days
        df = ak.stock_zh_a_hist_min_em(symbol=code, period="1", adjust="")
        if df is None or len(df) == 0:
            return None
        df = df.tail(total_count)
        pre_close = 0.0
        try:
            quote = _fetch_quote_sync(symbol)
            if quote and quote.get("pre_close"):
                pre_close = float(quote["pre_close"])
        except Exception:
            pass
        cum_vol, cum_amt = 0.0, 0.0
        bars = []
        for _, row in df.iterrows():
            price = float(row.get("收盘", 0))
            vol = float(row.get("成交量", 0))
            amt = float(row.get("成交额", 0)) if "成交额" in row.index else price * vol
            cum_vol += vol
            cum_amt += amt
            avg_p = round(cum_amt / cum_vol, 2) if cum_vol > 0 else price
            chg = round((price - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0
            t_str = str(row.get("时间", row.name))
            if " " in t_str:
                t_str = t_str.split(" ")[-1][:5]
            bars.append({"time": t_str, "price": price, "avg_price": avg_p, "volume": vol, "change_pct": chg})
        if pre_close == 0 and bars:
            pre_close = bars[0]["price"] * 0.99
        return {"pre_close": pre_close, "bars": bars}
    except Exception as exc:
        logger.warning("fetch_minute em failed for %s: %s", symbol, exc)
    return None


def _fetch_minute_sina_sync(symbol: str, count: int, days: int = 1) -> Dict[str, Any] | None:
    """分时 — 新浪 stock_zh_a_minute。列 day/open/high/low/close/volume。"""
    _sina_rate_limit()
    _ak_rate_limit()
    try:
        import akshare as ak
        sina_sym = _symbol_to_sina(symbol)
        df = ak.stock_zh_a_minute(symbol=sina_sym, period="1", adjust="")
        if df is None or len(df) == 0:
            return None
        total_count = count * days
        df = df.tail(total_count)
        pre_close = 0.0
        try:
            quote = _fetch_quote_sync(symbol)
            if quote and quote.get("pre_close"):
                pre_close = float(quote["pre_close"])
        except Exception:
            pass
        cum_vol, cum_amt = 0.0, 0.0
        bars = []
        for _, row in df.iterrows():
            price = float(row.get("close", 0))
            vol = float(row.get("volume", 0))
            cum_vol += vol
            cum_amt += price * vol
            avg_p = round(cum_amt / cum_vol, 2) if cum_vol > 0 else price
            chg = round((price - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0
            day_val = row.get("day", row.name)
            t_str = str(day_val).split(" ")[-1][:5] if day_val and " " in str(day_val) else str(day_val)[:5]
            bars.append({"time": t_str, "price": price, "avg_price": avg_p, "volume": vol, "change_pct": chg})
        if pre_close == 0 and bars:
            pre_close = bars[0]["price"] * 0.99
        return {"pre_close": pre_close, "bars": bars}
    except Exception as exc:
        logger.warning("fetch_minute sina failed for %s: %s", symbol, exc)
    return None


def _fetch_minute_sync(symbol: str, count: int, days: int = 1) -> Dict[str, Any] | None:
    """分时 — 双源主主+互备：按交易所分流，仅上交所与深交所；东财(上交所) / 新浪(深交所)，失败换另一源。"""
    code = _normalize_symbol(symbol)
    primary = _primary_source_by_exchange(code)
    result = _fetch_minute_em_sync(symbol, count, days) if primary == "em" else _fetch_minute_sina_sync(symbol, count, days)
    if result:
        return result
    result = _fetch_minute_sina_sync(symbol, count, days) if primary == "em" else _fetch_minute_em_sync(symbol, count, days)
    return result


async def fetch_minute_from_external(symbol: str, count: int = 240, days: int = 1) -> Dict[str, Any] | None:
    """仅拉取分时（不写 Redis/DB）。供预热专用；支持代理池上下文。"""
    pool = None
    sem = None
    try:
        from src.services.data_service.data_sync_service import (
            _CURRENT_SYNC_PROXY_POOL,
            _CURRENT_SYNC_AK_SEM,
        )
        pool = _CURRENT_SYNC_PROXY_POOL.get()
        sem = _CURRENT_SYNC_AK_SEM.get()
    except Exception:
        pass
    if pool and sem:
        from src.services.data_service.akshare_call_service import run_ak_with_retry
        return await run_ak_with_retry(
            _fetch_minute_sync, symbol, count=count, days=days,
            sem=sem, pool=pool, rate_limit_fn=None,
        )
    return await asyncio.to_thread(_fetch_minute_sync, symbol, count, days)


async def fetch_minute(symbol: str, count: int = 240, days: int = 1, *, tenant_id: str = "public") -> Dict[str, Any]:
    """获取分时数据 — L1 内存 -> L2 Redis -> L3 DB 快照；不直连 AKShare。days=1 当日，days=5 五日。"""
    code = _normalize_symbol(symbol)
    cache_key = f"minute_{code}_{count}_{days}"
    redis_key = f"market:minute:{code}" if days == 1 else f"market:minute:5day:{code}"

    # L1: 内存缓存
    cached = _get_cache(cache_key, _TTL_MINUTE)
    if cached is not None:
        return cached

    # L2: Redis 缓存
    try:
        from src.services.cache_policy_service import get_cached
        l2_raw = await get_cached(redis_key)
        if l2_raw:
            data = json.loads(l2_raw)
            if isinstance(data, dict) and data.get("bars"):
                _set_cache(cache_key, data)
                return data
    except Exception:
        pass

    # L3: DB 快照（market_minute_snapshot，预热写入）
    from src.services.data_service.hot_rank_service import _get_minute_snapshot_from_db
    db_data, db_updated_at = await _get_minute_snapshot_from_db(code, days)
    if db_data and db_data.get("bars"):
        if db_updated_at:
            db_data["data_updated_at"] = db_updated_at
        _set_cache(cache_key, db_data)
        try:
            from src.services.cache_policy_service import set_cached
            await set_cached(redis_key, json.dumps(db_data, ensure_ascii=False), ttl=30)
        except Exception:
            pass
        return db_data

    # L3 未命中：走外源 → 落库 → Redis → 返回（含 data_updated_at）
    try:
        external_data = await fetch_minute_from_external(symbol, count, days)
        if external_data and isinstance(external_data.get("bars"), list) and len(external_data["bars"]) > 0:
            from src.services.data_service.hot_rank_service import upsert_minute_snapshot
            from src.services.cache_policy_service import set_cached
            from datetime import datetime
            await upsert_minute_snapshot(code, external_data)
            external_data["data_updated_at"] = datetime.utcnow().isoformat() + "Z"
            _set_cache(cache_key, external_data)
            await set_cached(redis_key, json.dumps(external_data, ensure_ascii=False), ttl=30)
            return external_data
    except Exception as exc:
        logger.debug("fetch_minute L3-miss external fallback failed for %s: %s", code, exc)

    # 非交易时间：尝试返回 Redis 过期数据
    from src.services.data_service.exchange_time_utils import is_trading_time
    if not await is_trading_time():
        try:
            from src.services.cache_policy_service import get_cached as _gc
            stale = await _gc(redis_key)
            if stale:
                data = json.loads(stale)
                if isinstance(data, dict) and data.get("bars"):
                    data["_stale"] = True
                    return data
        except Exception:
            pass

    # 降级: Redis stale
    try:
        from src.services.cache_policy_service import get_cached as _gc
        stale = await _gc(redis_key)
        if stale:
            data = json.loads(stale)
            if isinstance(data, dict):
                data["_stale"] = True
                return data
    except Exception:
        pass

    return {"pre_close": 0, "bars": [], "_unavailable": True, "_error": "暂无分时数据，请触发预热或稍后重试"}


# ---------------------------------------------------------------------------
# Fundamental / F10 Data
# ---------------------------------------------------------------------------

def _fetch_fundamental_sync(symbol: str) -> Dict[str, Any] | None:
    """(同步) 基本面数据 — AKShare stock_individual_info_em (概览) + stock_financial_analysis_indicator (财务指标)。
    返回结构化 items 列表，包含 pe_ratio, pb_ratio, total_shares, eps 等关键字段。
    """
    _ak_rate_limit()
    try:
        import akshare as ak
        code = _strip_suffix(symbol)
        items: list[dict] = []

        # 数据源 1: stock_individual_info_em — 个股基本信息 (总股本/流通股/行业/上市日期等)
        try:
            df_info = ak.stock_individual_info_em(symbol=code)
            if df_info is not None and len(df_info) > 0:
                for _, row in df_info.iterrows():
                    item_name = str(row.iloc[0]) if len(row) > 0 else ""
                    item_val = str(row.iloc[1]) if len(row) > 1 else ""
                    if item_name:
                        items.append({"item": item_name, "value": item_val})
        except Exception as exc:
            logger.debug("stock_individual_info_em failed for %s: %s", code, exc)

        # 数据源 2: stock_financial_analysis_indicator — 财务分析指标 (PE/PB/EPS/ROE 等)
        _ak_rate_limit()
        try:
            df_fin = ak.stock_financial_analysis_indicator(symbol=code)
            if df_fin is not None and len(df_fin) > 0:
                # 取最新一期数据 (第一行)
                latest = df_fin.iloc[0]
                fin_fields = {
                    "摊薄每股收益(元)": "eps",
                    "每股净资产_调整后(元)": "bps",
                    "净资产收益率_摊薄(%)": "roe",
                    "主营业务收入(万元)": "revenue",
                    "净利润(万元)": "net_profit",
                    "每股经营性现金流(元)": "cash_per_share",
                }
                for col_name, eng_key in fin_fields.items():
                    if col_name in latest.index:
                        val = latest[col_name]
                        items.append({"item": col_name, "value": str(val), "key": eng_key})
                # 报告期标注
                if "日期" in latest.index:
                    items.insert(0, {"item": "财报期", "value": str(latest["日期"])})
        except Exception as exc:
            logger.debug("stock_financial_analysis_indicator failed for %s: %s", code, exc)

        # 数据源 3: 原 stock_financial_abstract 作为兜底
        if len(items) < 3:
            _ak_rate_limit()
            try:
                df_abs = ak.stock_financial_abstract(symbol=code)
                if df_abs is not None and len(df_abs) > 0:
                    latest_col = df_abs.columns[2] if len(df_abs.columns) > 2 else None
                    for _, row in df_abs.iterrows():
                        item_name = f"{row.iloc[0]}-{row.iloc[1]}" if len(row) > 1 else str(row.iloc[0])
                        item_val = str(row[latest_col]) if latest_col and latest_col in row.index else ""
                        items.append({"item": item_name, "value": item_val})
            except Exception as exc2:
                logger.debug("stock_financial_abstract failed for %s: %s", code, exc2)

        if items:
            return {"name": code, "items": items[:30]}
    except Exception as exc:
        logger.warning("fetch_fundamental akshare failed for %s: %s", symbol, exc)
    return None


async def _merge_fundamental_extended(code: str, result: Dict[str, Any]) -> Dict[str, Any]:
    """Phase 2：合并 F10 扩展（十大股东、分红配股、股东户数）到 fundamental 结果。"""
    # 若已有非空 top_holders 可跳过；否则从 DB 补全（解决缓存为旧数据、DB 已写入十大股东但页面仍空的问题）
    if result.get("top_holders"):
        return result
    try:
        from src.services.data_service.market_read_service import get_fundamental_extended_from_db
        extended = await get_fundamental_extended_from_db(code)
        result["top_holders"] = extended.get("top_holders", [])
        result["dividends"] = extended.get("dividends", [])
        result["holder_count"] = extended.get("holder_count", [])
    except Exception as exc:
        logger.debug("_merge_fundamental_extended failed for %s: %s", code, exc)
    return result


async def fetch_fundamental(symbol: str, *, tenant_id: str = "public") -> Dict[str, Any]:
    """获取基本面数据 — L1 内存 -> L2 Redis -> L3 stock_financial -> L4 外部API -> 写回。含 Phase 2 扩展 top_holders/dividends/holder_count。"""
    code = _normalize_symbol(symbol)
    cache_key = f"fundamental_{code}"
    redis_key = f"market:fundamental:{code}"
    ttl_fundamental_redis = 604800  # 7d

    # L1: 内存缓存（返回前合并扩展数据，保证十大股东/分红/股东户数等从 DB 实时取到）
    cached = _get_cache(cache_key, _TTL_FUNDAMENTAL)
    if cached is not None:
        cached = await _merge_fundamental_extended(code, cached)
        return cached

    # L2: Redis 缓存
    try:
        from src.services.cache_policy_service import get_cached
        l2_raw = await get_cached(redis_key)
        if l2_raw:
            data = json.loads(l2_raw)
            data = await _merge_fundamental_extended(code, data)
            _set_cache(cache_key, data)
            try:
                from src.services.cache_policy_service import set_cached
                await set_cached(redis_key, json.dumps(data, ensure_ascii=False), ttl=ttl_fundamental_redis)
            except Exception:
                pass
            return data
    except Exception:
        pass

    # L3: 读 stock_financial（按 symbol 取最新 report_date），仅财务指标子集；概览仍依赖 L4
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import StockFinancial

        async for session in get_session():
            stmt = (
                select(StockFinancial)
                .where(StockFinancial.symbol == code)
                .order_by(StockFinancial.report_date.desc())
                .limit(1)
            )
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            if row is not None:
                items: List[Dict[str, Any]] = []
                if row.report_date:
                    items.append({"item": "财报期", "value": str(row.report_date)})
                for col, label in [
                    ("roe", "净资产收益率(%)"),
                    ("eps", "基本每股收益(元)"),
                    ("gross_margin", "销售毛利率(%)"),
                    ("net_margin", "销售净利率(%)"),
                    ("debt_ratio", "资产负债比率(%)"),
                    ("current_ratio", "流动比率"),
                ]:
                    val = getattr(row, col, None)
                    if val is not None:
                        items.append({"item": label, "value": str(val), "key": col})
                if items:
                    result_l3 = {"name": code, "items": items}
                    result_l3 = await _merge_fundamental_extended(code, result_l3)
                    _set_cache(cache_key, result_l3)
                    try:
                        from src.services.cache_policy_service import set_cached
                        await set_cached(redis_key, json.dumps(result_l3, ensure_ascii=False), ttl=ttl_fundamental_redis)
                    except Exception:
                        pass
                    return result_l3
            break
    except Exception as exc:
        logger.debug("fetch_fundamental L3 stock_financial failed for %s: %s", symbol, exc)

    # L4: 外部 API 兜底（run_external_with_retry）（Phase 1）；超时 30s（基本面接口较慢）
    from src.services.data_service.external_request_executor import run_external_with_retry
    result = None
    try:
        result = await run_external_with_retry(
            lambda: _fetch_fundamental_sync(symbol),
            tenant_id=tenant_id,
            domain="eastmoney.com",
            rate_limit_fn=_ak_rate_limit,
            timeout_seconds=30.0,
        )
    except Exception as e:
        logger.debug("fetch_fundamental L4 run_external_with_retry failed for %s: %s", symbol, e)
    # L4 失败时回退：直连一次（兼容 b074abb 旧版无超时、无代理的 to_thread 行为）
    if not result:
        try:
            result = await asyncio.to_thread(_fetch_fundamental_sync, symbol)
        except Exception as e2:
            logger.debug("fetch_fundamental L4 fallback to_thread failed for %s: %s", symbol, e2)
    if result:
        result = await _merge_fundamental_extended(code, result)
        _set_cache(cache_key, result)
        try:
            from src.services.cache_policy_service import set_cached
            await set_cached(redis_key, json.dumps(result, ensure_ascii=False), ttl=ttl_fundamental_redis)
        except Exception:
            pass
        return result

    # 最终: 空基本面 + _unavailable 标记
    return {"name": symbol, "items": [], "_unavailable": True, "_error": "所有数据源均不可用，请稍后重试"}


# ---------------------------------------------------------------------------
# 资讯/公告 — AKShare stock_news_em
# ---------------------------------------------------------------------------

def _fetch_news_sync(symbol: str) -> List[Dict[str, Any]] | None:
    """(同步) 个股资讯 — AKShare stock_news_em。"""
    _ak_rate_limit()
    try:
        import akshare as ak
        code = _strip_suffix(symbol)
        df = ak.stock_news_em(symbol=code)
        if df is not None and len(df) > 0:
            items = []
            for _, row in df.head(30).iterrows():
                items.append({
                    "title": str(row.get("新闻标题", "")),
                    "content": str(row.get("新闻内容", ""))[:200],
                    "date": str(row.get("发布时间", "")),
                    "source": str(row.get("文章来源", "")),
                    "url": str(row.get("新闻链接", "")),
                })
            return items
    except Exception as exc:
        logger.warning("fetch_news akshare failed for %s: %s", symbol, exc)
    return None


async def fetch_news(symbol: str) -> List[Dict[str, Any]]:
    """获取个股资讯 — L1 内存 -> L2 Redis -> 外部API -> 写回。"""
    code = _normalize_symbol(symbol)
    cache_key = f"news_{code}"
    redis_key = f"market:news:{code}"

    # L1: 内存缓存
    cached = _get_cache(cache_key, _TTL_NEWS)
    if cached is not None:
        return cached

    # L2: Redis 缓存
    try:
        from src.services.cache_policy_service import get_cached
        l2_raw = await get_cached(redis_key)
        if l2_raw:
            data = json.loads(l2_raw)
            if isinstance(data, list):
                _set_cache(cache_key, data)
                return data
    except Exception:
        pass

    # L3: DB (stock_news 表)
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import StockNews
        async for session in get_session():
            stmt = (
                select(StockNews)
                .where(StockNews.symbol == code)
                .order_by(StockNews.publish_time.desc())
                .limit(30)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            if rows:
                items = [
                    {
                        "title": r.title,
                        "content": (r.content or "")[:200],
                        "date": r.publish_time,
                        "source": r.source,
                        "url": r.url,
                    }
                    for r in rows
                ]
                _set_cache(cache_key, items)
                from src.services.cache_policy_service import set_cached
                await set_cached(redis_key, json.dumps(items, ensure_ascii=False), ttl=600)
                return items
            break
    except Exception:
        pass

    # L4: 外部 API 兜底
    proxy = _get_proxy_from_context()
    def _run_news():
        with _proxy_context(proxy):
            return _fetch_news_sync(symbol)
    result = await asyncio.to_thread(_run_news)
    if result:
        _set_cache(cache_key, result)
        # 写回 Redis (TTL 10min)
        try:
            from src.services.cache_policy_service import set_cached
            await set_cached(redis_key, json.dumps(result, ensure_ascii=False), ttl=600)
        except Exception:
            pass
        return result

    return []


# ---------------------------------------------------------------------------
# AKShare 真实探测 (免费开源，无需 key)
# ---------------------------------------------------------------------------

async def _probe_akshare(category: str) -> Dict[str, Any]:
    """通过 akshare 库真实探测数据。"""
    try:
        import akshare as ak  # noqa: F811
    except ImportError:
        logger.warning("akshare not installed, falling back to mock")
        return await _probe_mock(category, source_name="akshare")

    t0 = time.time()
    proxy = _get_proxy_from_context()
    try:
        with _proxy_context(proxy):
            if category == "kline":
                df = ak.stock_zh_a_daily(symbol="sz000001", adjust="qfq")
                if df is not None and len(df) > 0:
                    sample = df.tail(5).to_dict(orient="records")
                    return _ok_result(category, t0, {"rows": len(sample), "sample": sample[:2]})
                return _ok_result(category, t0, {"note": "数据为空"})

            elif category == "quote":
                import requests as _req
                sess = _req.Session()
                sess.trust_env = False
                sess.headers["Referer"] = "https://finance.sina.com.cn"
                resp = sess.get("https://hq.sinajs.cn/list=sz000001", timeout=10)
                resp.encoding = "gbk"
                text = resp.text.strip()
                parts = text.split('"')[1].split(",") if '"' in text else []
                if len(parts) >= 10:
                    sample = {
                        "名称": parts[0], "今开": parts[1], "昨收": parts[2],
                        "现价": parts[3], "最高": parts[4], "最低": parts[5],
                        "成交量(手)": parts[8], "成交额": parts[9],
                    }
                    return _ok_result(category, t0, {"rows": 1, "sample": [sample]})
                return _ok_result(category, t0, {"note": "新浪接口返回为空"})

            elif category == "fundamental":
                df = ak.stock_financial_abstract(symbol="000001")
                if df is not None and len(df) > 0:
                    latest_col = df.columns[2] if len(df.columns) > 2 else None
                    sample = []
                    for _, row in df.head(8).iterrows():
                        item = f"{row.iloc[0]}-{row.iloc[1]}"
                        val = str(row[latest_col]) if latest_col else ""
                        sample.append({"item": item, "value": val})
                    return _ok_result(category, t0, {"rows": len(sample), "sample": sample[:5]})
                return _ok_result(category, t0, {"note": "数据为空"})

            elif category == "technical":
                df = ak.stock_zh_a_daily(symbol="sz000001", adjust="qfq")
                if df is not None and len(df) >= 5:
                    closes = df["close"].tail(10).tolist()
                    ma5 = round(sum(closes[-5:]) / 5, 2) if len(closes) >= 5 else None
                    return _ok_result(category, t0, {"ma5": ma5, "recent_closes": closes[-5:]})
                return _ok_result(category, t0, {"note": "数据不足"})

            elif category == "stock_list":
                # 先快后慢：先试上证单所(10s)，失败再试全市场(12s)，总时间≤22s
                df = None
                try:
                    def _run_sh():
                        with _proxy_context(proxy):
                            return ak.stock_info_sh_name_code("主板A股")
                    df = await asyncio.wait_for(
                        asyncio.to_thread(_run_sh),
                        timeout=10,
                    )
                    if df is not None and len(df) > 0:
                        # 上证列名 证券代码/证券简称 → code/name，与全市场返回结构一致
                        if "证券代码" in df.columns and "证券简称" in df.columns:
                            df = df[["证券代码", "证券简称"]].rename(
                                columns={"证券代码": "code", "证券简称": "name"}
                            )
                        sample = df.head(5).to_dict(orient="records")
                        return _ok_result(category, t0, {"rows": len(df), "sample": sample[:3]})
                    # 上证返回空，不 return，进入备用
                except (asyncio.TimeoutError, Exception):
                    pass  # 进入备用：全市场
                try:
                    def _run_a():
                        with _proxy_context(proxy):
                            return ak.stock_info_a_code_name()
                    df = await asyncio.wait_for(
                        asyncio.to_thread(_run_a),
                        timeout=12,
                    )
                    if df is not None and len(df) > 0:
                        sample = df.head(5).to_dict(orient="records")
                        return _ok_result(category, t0, {"rows": len(df), "sample": sample[:3]})
                    return _ok_result(category, t0, {"note": "数据为空"})
                except asyncio.TimeoutError:
                    latency = int((time.time() - t0) * 1000)
                    logger.warning("akshare probe [stock_list] timeout")
                    return {"category": category, "success": False, "latency_ms": latency,
                            "sample_data": {}, "error": "探测超时"}
                except Exception as exc:
                    latency = int((time.time() - t0) * 1000)
                    logger.warning("akshare probe [stock_list] failed: %s", exc)
                    return {"category": category, "success": False, "latency_ms": latency,
                            "sample_data": {}, "error": str(exc)[:300]}

            elif category == "margin":
                from datetime import datetime as _dt
                _today = _dt.now().strftime("%Y%m%d")
                df = ak.stock_margin_detail_sse(date=_today)
                if df is not None and len(df) > 0:
                    sample = df.head(5).to_dict(orient="records")
                    return _ok_result(category, t0, {"rows": len(df), "sample": sample[:2]})
                return _ok_result(category, t0, {"note": "今日暂无融资融券数据"})

            elif category == "block_trade":
                df = ak.stock_dzjy_sctj()
                if df is not None and len(df) > 0:
                    sample = df.head(5).to_dict(orient="records")
                    return _ok_result(category, t0, {"rows": len(df), "sample": sample[:2]})
                return _ok_result(category, t0, {"note": "数据为空"})

            elif category == "capital_flow":
                df = ak.stock_individual_fund_flow(stock="000001", market="sz")
                if df is not None and len(df) > 0:
                    sample = df.tail(5).to_dict(orient="records")
                    return _ok_result(category, t0, {"rows": len(sample), "sample": sample[:2]})
                return _ok_result(category, t0, {"note": "数据为空"})

            elif category == "dividend":
                from datetime import datetime as _dt2
                _year = str(_dt2.now().year)
                df = ak.stock_fhps_em(date=_year)
                if df is not None and len(df) > 0:
                    sample = df.head(5).to_dict(orient="records")
                    return _ok_result(category, t0, {"rows": len(df), "sample": sample[:2]})
                return _ok_result(category, t0, {"note": "今年暂无分红数据"})

            elif category == "sector":
                df = ak.stock_board_industry_name_em()
                if df is not None and len(df) > 0:
                    sample = df.head(5).to_dict(orient="records")
                    return _ok_result(category, t0, {"rows": len(df), "sample": sample[:3]})
                return _ok_result(category, t0, {"note": "数据为空"})

            elif category == "hot_rank":
                df = ak.stock_hot_rank_em()
                sample = df.head(10).to_dict(orient="records") if df is not None else []
                return _ok_result(category, t0, {"rows": len(sample), "sample": sample[:3]})

            elif category == "institutional":
                df = ak.stock_hsgt_hold_stock_em(market="北向", indicator="今日排行")
                if df is not None and len(df) > 0:
                    sample = df.head(5).to_dict(orient="records")
                    return _ok_result(category, t0, {"rows": len(sample), "sample": sample[:2]})
                return _ok_result(category, t0, {"note": "数据为空"})

            elif category == "announcement":
                from datetime import datetime
                today = datetime.now().strftime("%Y%m%d")
                df = ak.stock_notice_report(symbol="全部", date=today)
                if df is not None and len(df) > 0:
                    sample = df.head(5).to_dict(orient="records")
                    return _ok_result(category, t0, {"rows": len(sample), "sample": sample[:2]})
                return _ok_result(category, t0, {"note": "今日暂无公告"})

            else:
                return _ok_result(category, t0, {"note": "未知分类"})

    except Exception as exc:
        latency = int((time.time() - t0) * 1000)
        logger.warning("akshare probe [%s] failed: %s", category, exc)
        return {"category": category, "success": False, "latency_ms": latency,
                "sample_data": {}, "error": str(exc)[:300]}


# ---------------------------------------------------------------------------
# 统一探测入口
# ---------------------------------------------------------------------------

async def probe_market_source(
    source: str,
    api_key: str = "",
    category: str = "all",
) -> List[Dict[str, Any]]:
    categories = [c["id"] for c in PROBE_CATEGORIES] if category == "all" else [category]
    results: List[Dict[str, Any]] = []
    for cat in categories:
        if source == "akshare":
            r = await _probe_akshare(cat)
        else:
            r = {"category": cat, "success": False, "latency_ms": 0,
                 "sample_data": {}, "error": f"数据源 {source} 未实现"}
        results.append(r)
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ok_result(category: str, t0: float, sample_data: dict, note: str = "") -> Dict[str, Any]:
    latency = int((time.time() - t0) * 1000)
    result: Dict[str, Any] = {
        "category": category,
        "success": True,
        "latency_ms": latency,
        "sample_data": sample_data,
        "error": "",
    }
    if note:
        result["sample_data"]["_note"] = note
    return result
