"""AKShare 全量/增量数据同步服务

对齐 https://akshare.akfamily.xyz/data/stock/stock.html 全部股票数据分类:

┌──────────────────┬──────────────────────────────────────────────────────┐
│ 分类 ID          │ AKShare 接口                                          │
├──────────────────┼──────────────────────────────────────────────────────┤
│ stock_list       │ stock_info_sh_name_code(主板A股) / stock_info_a_code_name │
│ kline            │ stock_zh_a_hist (日/周/月 K 线, 前复权/后复权/不复权)   │
│ financial        │ stock_financial_analysis_indicator + _abstract        │
│ margin           │ stock_margin_detail_szse / stock_margin_detail_sse    │
│ block_trade      │ stock_dzjy_mrmx (大宗交易每日明细)                       │
│ capital_flow     │ stock_individual_fund_flow (个股资金流向)               │
│ top_holder       │ stock_gdfx_free_top_10_em (十大流通股东)               │
│ dividend         │ stock_fhps_em (分红配股)                               │
│ sector           │ stock_board_industry_name_em + concept_name_em        │
│ sector_member    │ stock_board_industry_cons_em (板块成分股)               │
│ lhb              │ stock_lhb_detail_em (龙虎榜)                          │
│ northbound       │ stock_hsgt_hist_em (北向资金/沪股通/深股通)            │
│ limit_updown     │ stock_zt_pool_em / stock_zt_pool_dtgc_em             │
│ holder_count     │ stock_hold_num_cninfo (股东户数)                       │
└──────────────────┴──────────────────────────────────────────────────────┘

执行策略:
  - 支持全量 (full) 和增量 (incremental) 两种模式
  - 使用并发 (asyncio.Semaphore) 控制并行度
  - 每个 AKShare 调用通过 asyncio.to_thread 在线程池运行
  - 全局限速器防止被封 IP
  - 水位线 (watermark) 记录每分类最后同步日期
  - watermark 日期格式：各分类按实现约定，margin/limit_updown 用 YYYYMMDD，block_trade/northbound_hold 等用 YYYY-MM-DD；比较时与同分类存储格式一致即可（规格 6/9 章）。
"""

import asyncio
import contextvars
import json
import logging
import os
import random
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

# 当前 sync 任务使用的代理（按任务取还，由 run_sync 设置/清除）
_CURRENT_SYNC_PROXY: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("sync_proxy", default=None)
# 本任务是否请求了代理（用于任务日志区分「未开代理」与「代理池无可用」）
_SYNC_PROXY_REQUESTED: contextvars.ContextVar[bool] = contextvars.ContextVar("sync_proxy_requested", default=False)
# 动态并发时当前任务的代理池（Semaphore(N) + 按序分配），未启用时为 None；可为 SyncProxyPool 或 SyncProxyPoolWithReserve
_CURRENT_SYNC_PROXY_POOL: contextvars.ContextVar[Optional["SyncProxyPool"]] = contextvars.ContextVar(
    "sync_proxy_pool", default=None
)
# K 线 aggressive 时任务级东财/腾讯并发信号量（每域 N=len(active)），未启用时为 None 表示用模块级 _KLINE_SEM_EM/TX
_CURRENT_KLINE_SEM_EM: contextvars.ContextVar[Optional[asyncio.Semaphore]] = contextvars.ContextVar(
    "kline_sem_em", default=None
)
_CURRENT_KLINE_SEM_TX: contextvars.ContextVar[Optional[asyncio.Semaphore]] = contextvars.ContextVar(
    "kline_sem_tx", default=None
)
# K 线 aggressive 时任务级东财/腾讯并发数（=len(active)），用于任务日志「并发: 东财 N 腾讯 N」；未设置时日志用环境变量
_CURRENT_KLINE_CONCURRENT_N: contextvars.ContextVar[Optional[int]] = contextvars.ContextVar(
    "kline_concurrent_n", default=None
)
# 动态并发审计：(使用代理数 N, 实际并发数 M, 任务开始时池可用数 K)，仅动态并发时设置
_SYNC_PROXY_DYNAMIC_INFO: contextvars.ContextVar[Optional[tuple[int, int, int]]] = contextvars.ContextVar(
    "sync_proxy_dynamic_info", default=None
)
# K 线 aggressive 时：(在用 N, 备用 M, 池当时可用 K)，用于任务日志「动态并发(60%)」
_SYNC_PROXY_AGGRESSIVE_KLINE_INFO: contextvars.ContextVar[Optional[tuple[int, int, int]]] = contextvars.ContextVar(
    "sync_proxy_aggressive_kline_info", default=None
)
# 任务级 AKShare 信号量：启用代理池时替代全局 _SEM，大小为代理数 N
_CURRENT_SYNC_AK_SEM: contextvars.ContextVar[Optional[asyncio.Semaphore]] = contextvars.ContextVar(
    "sync_ak_sem", default=None
)

# 允许按代理池可用数动态并发的分类（必须/建议用代理，见《数据拉取-代理使用规则与策略》规则表）
DYNAMIC_CONCURRENCY_CATEGORIES = frozenset({
    "kline", "financial", "margin", "block_trade", "capital_flow", "top_holder",
    "dividend", "sector", "lhb", "northbound", "northbound_hold", "limit_updown",
    "holder_count", "peer_comparison", "news",
})

# 分类 → 代理池取用主域名（按 domain_results 过滤，仅返回该域 valid 的代理）
SYNC_CATEGORY_PRIMARY_DOMAIN = {
    "stock_list": "sse.com.cn",
    "kline": "eastmoney.com",
    "financial": "eastmoney.com",
    "margin": "sse.com.cn",
    "block_trade": "eastmoney.com",
    "capital_flow": "eastmoney.com",
    "top_holder": "eastmoney.com",
    "dividend": "eastmoney.com",
    "sector": "eastmoney.com",
    "lhb": "eastmoney.com",
    "northbound": "eastmoney.com",
    "northbound_hold": "eastmoney.com",
    "limit_updown": "eastmoney.com",
    "holder_count": "eastmoney.com",
    "peer_comparison": "eastmoney.com",
    "trade_calendar": "finance.sina.com.cn",
    "news": "eastmoney.com",
}


class SyncProxyPool:
    """任务内 N 个代理的池：acquire 返回 (proxy, release_cb)，按序轮转，限制并发数为 N。"""

    __slots__ = ("_proxy_list", "_sem", "_index", "_lock")

    def __init__(self, proxy_list: List[str]) -> None:
        n = len(proxy_list) or 1
        self._proxy_list = proxy_list if proxy_list else [""]
        self._sem = asyncio.Semaphore(n)
        self._index = 0
        self._lock = asyncio.Lock()

    @property
    def size(self) -> int:
        return len(self._proxy_list)

    async def acquire(self) -> tuple[str, Callable[..., None]]:
        await self._sem.acquire()
        async with self._lock:
            proxy = self._proxy_list[self._index % len(self._proxy_list)]
            self._index += 1
        def release(success: bool = True) -> None:
            self._sem.release()
        return (proxy, release)


class SyncProxyPoolWithReserve:
    """任务内「在用池 + 备用池」：acquire 与 SyncProxyPool 一致；release_cb(success=False) 时连续失败计数，
    达到 replace_after_failures 次则热刷新：先 get_proxy，无则从 reserve 取，替换该槽位（仅本任务内）。"""

    __slots__ = ("_active", "_reserve", "_consecutive_errors", "_lock", "_sem", "_index", "_tenant_id", "_domain", "_replace_after_failures")

    def __init__(
        self,
        active_list: List[str],
        reserve_list: List[str],
        *,
        tenant_id: Optional[str] = None,
        domain: Optional[str] = None,
        replace_after_failures: int = 1,
    ) -> None:
        self._active = list(active_list) if active_list else [""]
        self._reserve = list(reserve_list) if reserve_list else []
        self._consecutive_errors: Dict[str, int] = {}
        self._lock = asyncio.Lock()
        n = len(self._active) or 1
        self._sem = asyncio.Semaphore(n)
        self._index = 0
        self._tenant_id = tenant_id or ""
        self._domain = domain or ""
        self._replace_after_failures = max(1, replace_after_failures)

    @property
    def size(self) -> int:
        return len(self._active)

    async def acquire(self) -> tuple[str, Callable[..., None]]:
        await self._sem.acquire()
        async with self._lock:
            proxy = self._active[self._index % len(self._active)]
            self._index += 1
        loop = asyncio.get_event_loop()
        sem = self._sem

        def release(success: bool = True) -> None:
            if success:
                sem.release()
                return
            # 可能从线程池调用，提交到主循环执行
            asyncio.run_coroutine_threadsafe(self._do_release(proxy), loop)

        return (proxy, release)

    async def _do_release(self, proxy: str) -> None:
        try:
            self._consecutive_errors[proxy] = self._consecutive_errors.get(proxy, 0) + 1
            if self._consecutive_errors[proxy] >= self._replace_after_failures:
                new_proxy: Optional[str] = None
                if self._tenant_id:
                    try:
                        from src.services.data_service.proxy_pool_service import get_proxy
                        new_proxy = await get_proxy(self._tenant_id, domain=self._domain or None)
                    except Exception:
                        pass
                if new_proxy is None and self._reserve:
                    async with self._lock:
                        if self._reserve:
                            new_proxy = random.choice(self._reserve)
                            self._reserve.remove(new_proxy)
                if new_proxy is not None:
                    async with self._lock:
                        for i, p in enumerate(self._active):
                            if p == proxy:
                                self._active[i] = new_proxy
                                break
                        self._consecutive_errors[proxy] = 0
                        self._consecutive_errors[new_proxy] = 0
        except Exception:
            pass
        self._sem.release()


def _mask_proxy(proxy: str) -> str:
    """日志用脱敏：保留前两段或 host:port 结构，避免明文暴露完整 IP。"""
    if not proxy or ":" not in proxy:
        return "***"
    host, port = proxy.rsplit(":", 1)
    if not host:
        return f"***:{port}"
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[:2]) + ".*:" + port
    return "***:" + port


async def _append_proxy_info_task_start(task_id: int) -> None:
    """任务开始时写一条代理相关信息到任务日志（使用代理 / 动态并发 N/M/K / 代理池无可用直连）。仅当本任务涉及代理时写入。"""
    if not task_id:
        return
    requested = _SYNC_PROXY_REQUESTED.get(False)
    aggressive_info = _SYNC_PROXY_AGGRESSIVE_KLINE_INFO.get()
    if aggressive_info:
        N, M, K = aggressive_info
        await _append_task_log(task_id, "INFO", f"动态并发(60%): 在用 N={N}, 备用 M={M}, K={K}（该域可用数）")
        return
    dynamic_info = _SYNC_PROXY_DYNAMIC_INFO.get()
    if dynamic_info:
        N, M, K = dynamic_info
        await _append_task_log(task_id, "INFO", f"动态并发: 使用代理数 N={N}, 实际并发 M={M}, K={K}（该域可用数）")
        return
    proxy = _CURRENT_SYNC_PROXY.get()
    if proxy:
        await _append_task_log(task_id, "INFO", f"使用代理: {_mask_proxy(proxy)}")
    elif requested:
        await _append_task_log(task_id, "INFO", "代理池无可用，本次直连")

logger = logging.getLogger(__name__)

# 增量时「无新数据」统一返回文案（不写库、不更新 watermark）
SYNC_SKIPPED_MESSAGE = "本周期数据无变化，无需拉取"
SYNC_SKIPPED_MESSAGE_DAILY = "本日数据无变化，无需拉取"
SYNC_SKIPPED_MESSAGE_MARKET_CLOSED = "本日休市，无需拉取"

# 并发控制: 最多同时 3 个 AKShare 线程 (防封 IP)，其他同步类型用
_SEM = asyncio.Semaphore(3)

# K 线双源独立限速：东财/腾讯各一套，互不串扰，理论 2 倍吞吐；可配间隔与并发
_KLINE_EM_LOCK = threading.Lock()
_KLINE_EM_LAST: float = 0.0
_KLINE_TX_LOCK = threading.Lock()
_KLINE_TX_LAST: float = 0.0
_KLINE_SOURCE_INTERVAL = float(os.environ.get("KLINE_SOURCE_INTERVAL", "1.0"))  # 每源最小间隔(秒)，0.5 约 4/s
_KLINE_SEM_EM = asyncio.Semaphore(int(os.environ.get("KLINE_CONCURRENT_EM", "2")))  # 东财并发数
_KLINE_SEM_TX = asyncio.Semaphore(int(os.environ.get("KLINE_CONCURRENT_TX", "2")))  # 腾讯并发数


def _last_trading_date_str() -> str:
    """返回最近一个交易日 YYYYMMDD（按星期回退，周末用周五），供融资融券等按交易日拉数的接口使用。"""
    d = datetime.now()
    while d.weekday() >= 5:  # 5=周六 6=周日
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


async def get_last_trading_date_str(include_today: bool = True) -> str:
    """从表 exchange_trading_dates 查询「最近交易日」YYYYMMDD。
    include_today=True 时取 trade_date <= 当前日期的最后一条；
    include_today=False 时取倒数第二条（上一交易日）。
    表空或异常时回退到 _last_trading_date_str()（回退，规格目标为日历表）。
    约定（规格 2.5/8）：依赖交易日的分类在「当前自然日非交易日」时，用最近交易日拉取该日数据；
    增量时若「最近交易日 <= watermark」则返回「本日数据无变化」skipped。
    """
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import ExchangeTradingDate

        today = datetime.now().strftime("%Y-%m-%d")
        async for session in get_session():
            stmt = (
                select(ExchangeTradingDate.trade_date)
                .where(ExchangeTradingDate.trade_date <= today)
                .order_by(ExchangeTradingDate.trade_date.desc())
                .limit(2)
            )
            result = await session.execute(stmt)
            rows = [r[0] for r in result.fetchall()]
            if not rows:
                break
            if include_today and len(rows) >= 1:
                return str(rows[0]).replace("-", "")
            if not include_today and len(rows) >= 2:
                return str(rows[1]).replace("-", "")
            if rows:
                return str(rows[0]).replace("-", "")
            break
    except Exception:
        pass
    # 回退，规格目标为日历表
    return _last_trading_date_str()


# ---------------------------------------------------------------------------
# 数据分类定义
# ---------------------------------------------------------------------------

SYNC_CATEGORIES = [
    {"id": "stock_list",   "name": "A股列表",       "icon": "fa-list",                "desc": "沪深京A股全量股票代码与名称",      "default_interval": 86400},
    {"id": "kline",        "name": "K线行情",       "icon": "fa-chart-bar",           "desc": "日/周/月K线(不复权+前复权+后复权)", "default_interval": 3600},
    {"id": "financial",    "name": "财务指标",      "icon": "fa-file-invoice-dollar", "desc": "ROE/EPS/毛利率/资产负债率等",      "default_interval": 604800},
    {"id": "margin",       "name": "融资融券",      "icon": "fa-balance-scale",       "desc": "融资余额/融券余额/融资买入等",      "default_interval": 86400},
    {"id": "block_trade",  "name": "大宗交易",      "icon": "fa-handshake",           "desc": "大宗交易明细(买卖营业部/溢价率)",    "default_interval": 86400},
    {"id": "capital_flow", "name": "资金流向",      "icon": "fa-money-bill-wave",     "desc": "主力/大单/中单/小单净流入",         "default_interval": 3600},
    {"id": "top_holder",   "name": "十大股东",      "icon": "fa-users",               "desc": "十大流通股东及持仓变动",            "default_interval": 2592000},
    {"id": "dividend",     "name": "分红配股",      "icon": "fa-gift",                "desc": "历年送转派息方案",                  "default_interval": 604800},
    {"id": "sector",       "name": "行业/概念板块", "icon": "fa-sitemap",             "desc": "行业板块+概念板块列表及成分股",      "default_interval": 86400},
    {"id": "lhb",          "name": "龙虎榜",        "icon": "fa-trophy",              "desc": "龙虎榜上榜个股及营业部明细",         "default_interval": 86400},
    {"id": "northbound",   "name": "北向资金",      "icon": "fa-globe",               "desc": "沪股通/深股通每日净买入",            "default_interval": 3600},
    {"id": "northbound_hold", "name": "北向持股排行", "icon": "fa-university",        "desc": "北向持股个股截面(谁被买、买多少)，页面读库+Redis 加速", "default_interval": 3600},
    {"id": "limit_updown", "name": "涨跌停",        "icon": "fa-arrow-up",            "desc": "涨停池/跌停池/连板统计",             "default_interval": 1800},
    {"id": "holder_count", "name": "股东户数",      "icon": "fa-id-card",             "desc": "股东户数变动及户均持股",             "default_interval": 604800},
    {"id": "peer_comparison", "name": "同行比较", "icon": "fa-chart-line", "desc": "成长性/估值/杜邦/规模(行业内比较)", "default_interval": 604800},
    {"id": "trade_calendar", "name": "交易所日历", "icon": "fa-calendar-alt", "desc": "A股交易日历(排除周末与法定节假日)，供最近交易日等逻辑使用", "default_interval": 2592000},
    {"id": "news", "name": "资讯/公告", "icon": "fa-newspaper", "desc": "个股资讯与公告，供详情页资讯Tab展示", "default_interval": 14400},
]

CATEGORY_IDS = [c["id"] for c in SYNC_CATEGORIES]

# 任务记录相关：从 sync_task_record_service 导入并重新导出（供外部兼容）
from src.services.data_service.sync_task_record_service import (
    DATA_SYNC_CANCEL_CHANNEL,
    DATA_SYNC_QUEUE_KEY,
    TaskCancelledError,
    add_task_cancelled,
    cancel_all_running_sync_tasks,
    cancel_sync_task,
    clear_task_cancelled,
    delete_sync_tasks,
    enqueue_sync_task,
    get_sync_tasks_paged,
    get_task_logs,
    _append_task_log,
    _create_task,
    _create_task_or_use,
    _has_running_task,
    _is_task_cancelled,
    _raise_if_cancelled,
    _return_sync_skipped,
    _update_task,
)

# 数据拉取聚合日志 Redis list，保留最近 N 条（与计划 Q3 一致）
DATA_SYNC_LOG_HISTORY_KEY = "data_sync:log_history"
SYNC_LOG_HISTORY_MAX = 200


async def _append_sync_global_log(level: str, msg: str, *, task_id: Optional[int] = None, category: Optional[str] = None, **extra: Any) -> None:
    """追加一条数据拉取聚合日志到 Redis list，ts 使用北京时间（Q4）。"""
    try:
        from src.core.streams import get_redis_client
        from src.core.time_util import now_beijing
        ts = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
        entry = {"ts": ts, "level": level, "msg": msg, **extra}
        if task_id is not None:
            entry["task_id"] = task_id
        if category is not None:
            entry["category"] = category
        client = await get_redis_client()
        await client.rpush(DATA_SYNC_LOG_HISTORY_KEY, json.dumps(entry, ensure_ascii=False))
        await client.ltrim(DATA_SYNC_LOG_HISTORY_KEY, -SYNC_LOG_HISTORY_MAX, -1)
    except Exception as e:
        logger.debug("_append_sync_global_log failed: %s", e)


async def get_sync_log_entries(limit: int = 100) -> List[Dict[str, Any]]:
    """从 Redis 读取 data_sync:log_history 最近 limit 条，返回 log_entries 列表。"""
    try:
        from src.core.streams import get_redis_client
        client = await get_redis_client()
        raw = await client.lrange(DATA_SYNC_LOG_HISTORY_KEY, -limit, -1)
        out = []
        for s in (raw or []):
            try:
                out.append(json.loads(s) if isinstance(s, str) else json.loads(s.decode("utf-8")))
            except (json.JSONDecodeError, TypeError):
                continue
        return out
    except Exception as e:
        logger.debug("get_sync_log_entries failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# 辅助: AKShare 限速 + 线程调用
# ---------------------------------------------------------------------------

def _get_sync_batch_size() -> int:
    """启用代理池时返回池大小 N，用于 gather 批大小；无池时返回 1。"""
    pool = _CURRENT_SYNC_PROXY_POOL.get()
    if pool is None:
        return 1
    return getattr(pool, "size", 1)


def _ak_rate_limit():
    """复用 market_source_service 的限速器"""
    from src.services.data_service.market_source_service import _ak_rate_limit as _rl
    _rl()


def _kline_rate_limit_em():
    """K 线东财源独立限速，与腾讯源并行"""
    global _KLINE_EM_LAST
    with _KLINE_EM_LOCK:
        now = time.time()
        elapsed = now - _KLINE_EM_LAST
        if elapsed < _KLINE_SOURCE_INTERVAL:
            time.sleep(_KLINE_SOURCE_INTERVAL - elapsed)
        _KLINE_EM_LAST = time.time()


def _kline_rate_limit_tx():
    """K 线腾讯源独立限速，与东财源并行"""
    global _KLINE_TX_LAST
    with _KLINE_TX_LOCK:
        now = time.time()
        elapsed = now - _KLINE_TX_LAST
        if elapsed < _KLINE_SOURCE_INTERVAL:
            time.sleep(_KLINE_SOURCE_INTERVAL - elapsed)
        _KLINE_TX_LAST = time.time()


async def _ak_call(fn, *args, **kwargs):
    """在线程池中调用 AKShare 同步函数，带限速+信号量+重试+超时。若当前任务设置了代理（单代理或代理池）则使用代理。"""
    from src.services.data_service.akshare_call_service import run_ak_with_retry
    return await run_ak_with_retry(fn, *args, rate_limit_fn=_ak_rate_limit, **kwargs)


def _kline_sem_em():
    """K 线东财源信号量：aggressive 时用任务级，否则用模块级。"""
    s = _CURRENT_KLINE_SEM_EM.get()
    return s if s is not None else _KLINE_SEM_EM


def _kline_sem_tx():
    """K 线腾讯源信号量：aggressive 时用任务级，否则用模块级。"""
    s = _CURRENT_KLINE_SEM_TX.get()
    return s if s is not None else _KLINE_SEM_TX


async def _ak_call_kline_em(fn, *args, **kwargs):
    """K 线东财源专用：独立限速+信号量，与腾讯源并行，不占全局限速。带重试+超时+代理热刷新。"""
    from src.services.data_service.akshare_call_service import run_ak_with_retry
    return await run_ak_with_retry(fn, *args, sem=_kline_sem_em(), rate_limit_fn=_kline_rate_limit_em, **kwargs)


async def _ak_call_kline_tx(fn, *args, **kwargs):
    """K 线腾讯源专用：独立限速+信号量，与东财源并行。带重试+超时+代理热刷新。"""
    from src.services.data_service.akshare_call_service import run_ak_with_retry
    return await run_ak_with_retry(fn, *args, sem=_kline_sem_tx(), rate_limit_fn=_kline_rate_limit_tx, **kwargs)


def _symbol_to_tx(symbol: str) -> str:
    """将 6 位代码转为腾讯接口所需前缀格式: 上交所 6/5→sh，深交所 0/3→sz。仅支持上交所与深交所。"""
    s = (symbol or "").strip()
    if not s or len(s) < 6:
        return s
    if s.startswith(("sh", "sz")):
        return s
    if s[0] in "56":
        return "sh" + s
    if s[0] in "03":
        return "sz" + s
    return "sz" + s


def _symbol_to_em(symbol: str) -> str:
    """将 6 位代码转为东财同行比较接口格式: 0/3→SZ，5/6→SH；若已带 sh/sz 前缀则统一为大写 SH/SZ。"""
    s = (symbol or "").strip()
    if not s or len(s) < 6:
        return s
    if s.upper().startswith("SH"):
        return "SH" + (s[2:].lstrip() if len(s) > 2 else "")
    if s.upper().startswith("SZ"):
        return "SZ" + (s[2:].lstrip() if len(s) > 2 else "")
    if s[0] in "56":
        return "SH" + s
    if s[0] in "03":
        return "SZ" + s
    return "SZ" + s


def _kline_primary_source(symbol: str) -> str:
    """K 线双源主主：按交易所分流，仅上交所与深交所。返回 'em'(东财) 或 'tx'(腾讯)。
    东财主：上交所(5/6)；腾讯主：深交所(0/3)。失败时互备。"""
    s = (symbol or "").strip()
    if not s:
        return "em"
    if s.startswith("sh"):
        return "em"
    if s.startswith("sz"):
        return "tx"
    if len(s) >= 1:
        if s[0] in "56":
            return "em"   # 上交所
        if s[0] in "03":
            return "tx"   # 深交所
    return "em"


# ---------------------------------------------------------------------------
# 水位线管理
# ---------------------------------------------------------------------------

async def _get_watermark(category: str, sub_key: str = "") -> Optional[str]:
    """获取某分类最后同步日期"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import DataSyncWatermark
        async for session in get_session():
            stmt = select(DataSyncWatermark).where(
                DataSyncWatermark.category == category,
                DataSyncWatermark.sub_key == sub_key
            )
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            return row.last_sync_date if row else None
    except Exception:
        return None


async def _set_watermark(category: str, date_str: str, sub_key: str = ""):
    """更新某分类最后同步日期"""
    try:
        from sqlalchemy import text
        from src.core.db import get_session
        import os
        _dsn = os.environ.get("MYSQL_DSN", "")
        async for session in get_session():
            if "sqlite" in _dsn:
                sql = text(
                    "INSERT OR REPLACE INTO data_sync_watermarks (category, sub_key, last_sync_date, last_sync_at) "
                    "VALUES (:cat, :sk, :d, :now)"
                )
            else:
                sql = text(
                    "INSERT INTO data_sync_watermarks (category, sub_key, last_sync_date, last_sync_at) "
                    "VALUES (:cat, :sk, :d, :now) AS w "
                    "ON DUPLICATE KEY UPDATE last_sync_date=w.last_sync_date, last_sync_at=w.last_sync_at"
                )
            await session.execute(sql, {"cat": category, "sk": sub_key, "d": date_str, "now": datetime.utcnow()})
            await session.commit()
    except Exception as exc:
        logger.warning("Set watermark failed: %s", exc)


async def _get_watermark_fallback_from_table(category: str) -> Optional[str]:
    """当 watermark 表无记录时，从业务表推断「最后有数据日期」。用于 DB 有数据但 watermark 未写入的场景。"""
    if category == "news":
        try:
            from sqlalchemy import select, func
            from src.core.db import get_session
            from src.models.market_sync import StockNews
            async for session in get_session():
                stmt = select(func.max(StockNews.publish_time)).where(StockNews.publish_time != "")
                result = await session.execute(stmt)
                val = result.scalar()
                if val and str(val).strip():
                    s = str(val).strip()
                    return s[:10] if len(s) >= 10 else s  # "2024-01-15 10:00:00" -> "2024-01-15"
                break
        except Exception as exc:
            logger.debug("watermark fallback news: %s", exc)
    return None


async def _get_last_success_sync_at(category: str):
    """该分类「上次成功同步」的 UTC 时间（用于与当前时间做差判断是否超过 interval）。
    查询 data_sync_watermarks 中该 category 所有 sub_key 的 max(last_sync_at)；无行则返回 None。"""
    try:
        from sqlalchemy import select, func
        from src.core.db import get_session
        from src.models.market_sync import DataSyncWatermark
        async for session in get_session():
            stmt = select(func.max(DataSyncWatermark.last_sync_at)).where(
                DataSyncWatermark.category == category
            )
            result = await session.execute(stmt)
            val = result.scalar()
            return val
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 获取全部 A 股代码
# ---------------------------------------------------------------------------

async def _get_all_stock_codes() -> List[str]:
    """从数据库获取已存的 A 股代码列表; 如果为空则先拉取 stock_list"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_data import StockInfo
        async for session in get_session():
            result = await session.execute(select(StockInfo.code))
            codes = [r[0] for r in result.fetchall()]
            if codes:
                return codes
    except Exception:
        pass
    # 空列表, 先同步 stock_list
    await sync_stock_list("full")
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_data import StockInfo
        async for session in get_session():
            result = await session.execute(select(StockInfo.code))
            return [r[0] for r in result.fetchall()]
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════
# 各分类同步实现
# ═══════════════════════════════════════════════════════════════

async def sync_stock_list(sync_type: str = "full", **kwargs) -> Dict[str, Any]:
    """同步 A 股列表: 先 stock_info_sh_name_code(主板A股)，失败则 stock_info_a_code_name → stock_info 表"""
    import akshare as ak
    task_id = await _create_task_or_use("stock_list", sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)
    try:
        df = None
        try:
            df = await asyncio.wait_for(
                _ak_call(ak.stock_info_sh_name_code, "主板A股"),
                timeout=30,
            )
            if df is not None and not df.empty and "证券代码" in df.columns and "证券简称" in df.columns:
                df = df[["证券代码", "证券简称"]].rename(
                    columns={"证券代码": "code", "证券简称": "name"}
                )
            else:
                df = None
        except (asyncio.TimeoutError, Exception):
            df = None
        if df is None or df.empty:
            try:
                df = await asyncio.wait_for(
                    _ak_call(ak.stock_info_a_code_name),
                    timeout=60,
                )
                if df is not None and not df.empty:
                    if "code" not in df.columns or "name" not in df.columns:
                        df = None
            except (asyncio.TimeoutError, Exception):
                df = None
        if df is None or df.empty:
            await _append_task_log(task_id, "ERROR", "AKShare 返回空数据")
            await _update_task(task_id, status="failed", error_detail="AKShare 返回空数据",
                               finished_at=datetime.utcnow())
            return {"success": False, "error": "空数据"}

        if sync_type == "incremental":
            from src.core.db import get_session as _gs
            from src.models.market_data import StockInfo
            from sqlalchemy import select, func
            async for session in _gs():
                r = await session.execute(select(func.count()).select_from(StockInfo))
                db_count = r.scalar() or 0
                break
            if db_count == len(df):
                return await _return_sync_skipped(
                    task_id, SYNC_SKIPPED_MESSAGE,
                    category="stock_list", date_str=datetime.now().strftime("%Y-%m-%d")
                )

        from src.core.db import get_session
        from sqlalchemy import text
        import os

        count = 0
        async for session in get_session():
            for _, row in df.iterrows():
                code = str(row.get("code", "")).strip()
                name = str(row.get("name", "")).strip()
                if not code:
                    continue
                _dsn = os.environ.get("MYSQL_DSN", "")
                if "sqlite" in _dsn:
                    sql = text(
                        "INSERT OR REPLACE INTO stock_info (code, name, market) "
                        "VALUES (:code, :name, 'A')"
                    )
                    await session.execute(sql, {"code": code, "name": name})
                else:
                    sql = text(
                        "INSERT INTO stock_info (code, name, market, updated_at) "
                        "VALUES (:code, :name, 'A', :now) AS s "
                        "ON DUPLICATE KEY UPDATE name=s.name, updated_at=:now"
                    )
                    await session.execute(sql, {"code": code, "name": name, "now": datetime.utcnow()})
                count += 1
            await session.commit()

        await _set_watermark("stock_list", datetime.now().strftime("%Y-%m-%d"))
        await _append_task_log(task_id, "INFO", f"任务结束: 成功 total={len(df)} saved={count}")
        await _update_task(task_id, status="success", total_count=len(df),
                           success_count=count, finished_at=datetime.utcnow())
        return {"success": True, "total": len(df), "saved": count}

    except Exception as exc:
        logger.error("sync_stock_list failed: %s", exc)
        await _append_task_log(task_id, "ERROR", str(exc)[:500])
        await _update_task(task_id, status="failed", error_detail=str(exc)[:500],
                           finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}


async def sync_kline(sync_type: str = "full", symbols: List[str] = None,
                     period: str = "daily", adjust: str = "", resume: bool = False, **kwargs) -> Dict[str, Any]:
    """同步 K 线数据: stock_zh_a_hist → kline_storage。
    resume=True 时从 ClickHouse 查询已存在 symbol，仅同步缺失的股票（断点续传）。
    """
    import akshare as ak
    task_id = await _create_task_or_use("kline", "resume" if resume else sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)
    _kline_start_time = time.time()

    if not symbols:
        symbols = await _get_all_stock_codes()
    if not symbols:
        await _append_task_log(task_id, "ERROR", "无股票代码")
        await _update_task(task_id, status="failed", error_detail="无股票代码",
                           finished_at=datetime.utcnow())
        return {"success": False, "error": "无股票代码, 请先同步 stock_list"}

    # 断点续传：仅同步 ClickHouse 中尚未有数据的股票
    if resume:
        from src.services.data_service.kline_storage import get_kline_existing_symbols
        existing = await get_kline_existing_symbols(period=period)
        existing_set = set(existing)
        before_count = len(symbols)
        symbols = [s for s in symbols if s not in existing_set]
        skipped = before_count - len(symbols)
        await _append_task_log(
            task_id, "INFO",
            f"断点续传: ClickHouse 已有 {len(existing_set)} 只，跳过 {skipped} 只，待同步 {len(symbols)} 只"
        )
        if not symbols:
            await _update_task(task_id, status="success", total_count=0, success_count=0,
                               error_count=0, finished_at=datetime.utcnow())
            return {"success": True, "total": 0, "ok": 0, "err": 0, "task_id": task_id}

    # 确定时间范围：end_date 使用最近交易日，与数据源语义一致，休市日不请求“今天”
    if sync_type == "incremental":
        wm = await _get_watermark("kline", f"{period}_{adjust}")
        start_date = wm or "20200101"
    else:
        start_date = "20200101"
    end_date = await get_last_trading_date_str(include_today=True)

    if sync_type == "incremental" and start_date and start_date >= end_date:
        today_ymd = datetime.now().strftime("%Y%m%d")
        skip_message = (
            SYNC_SKIPPED_MESSAGE_MARKET_CLOSED
            if today_ymd > end_date
            else SYNC_SKIPPED_MESSAGE_DAILY
        )
        return await _return_sync_skipped(
            task_id, skip_message,
            category="kline", date_str=end_date, sub_key=f"{period}_{adjust}"
        )

    # 日 K 增量：闭市后同步以保证当日 K 线完整（默认仅提示，可选强制校验）
    if period == "daily" and sync_type == "incremental":
        from datetime import time as dt_time

        close_cutoff = dt_time(15, 10)
        now = datetime.now()
        if now.time() < close_cutoff:
            if os.environ.get("KLINE_DAILY_REQUIRE_AFTER_CLOSE", "").lower() == "true":
                return await _return_sync_skipped(
                    task_id, "建议闭市后（15:10）再同步日K，以保证当日K线完整",
                    category="kline", date_str=end_date, sub_key=f"{period}_{adjust}"
                )
            logger.info("日K增量未到闭市后，建议15:10后再同步以保证当日K线完整")

    from src.services.data_service.kline_storage import save_kline_to_ch
    success_count = 0
    error_count = 0
    errors = []
    failed_symbols: List[str] = []

    total = len(symbols)
    await _update_task(task_id, total_count=total)
    adjust_label = adjust if adjust else "不复权"
    await _append_task_log(task_id, "INFO", f"拉取数据: 日K {adjust_label} 共 {total} 只")
    _kline_n = _CURRENT_KLINE_CONCURRENT_N.get()
    if _kline_n is not None:
        _em = _tx = str(_kline_n)
    else:
        _em = os.environ.get("KLINE_CONCURRENT_EM", "2")
        _tx = os.environ.get("KLINE_CONCURRENT_TX", "2")
    await _append_task_log(task_id, "INFO", f"并发: 东财 {_em} 腾讯 {_tx}, batch 50")

    # 双源主主：按交易所分流，互为主备。仅上交所与深交所：东财=上交所，腾讯=深交所；任一方失败则用另一方
    def _df_to_bars_em(df, date_key="日期", open_key="开盘", high_key="最高", low_key="最低",
                       close_key="收盘", vol_key="成交量", turn_key="成交额"):
        bars = []
        for _, row in df.iterrows():
            bars.append({
                "date": str(row.get(date_key, "")),
                "open": float(row.get(open_key, 0)),
                "high": float(row.get(high_key, 0)),
                "low": float(row.get(low_key, 0)),
                "close": float(row.get(close_key, 0)),
                "volume": float(row.get(vol_key, 0)),
                "turnover": float(row.get(turn_key, 0)) if turn_key else 0.0,
            })
        return bars

    async def _pull_one(symbol: str):
        nonlocal success_count, error_count
        bars = []
        primary = _kline_primary_source(symbol)
        try:
            # 主源优先，失败或空则切备源；东财/腾讯各用独立限速，双源并行约 2 倍吞吐
            if primary == "em":
                df = await _ak_call_kline_em(
                    ak.stock_zh_a_hist,
                    symbol=symbol, period=period,
                    start_date=start_date, end_date=end_date, adjust=adjust
                )
                if df is not None and not df.empty:
                    bars = _df_to_bars_em(df)
                if not bars:
                    tx_symbol = _symbol_to_tx(symbol)
                    df_tx = await _ak_call_kline_tx(
                        ak.stock_zh_a_hist_tx,
                        symbol=tx_symbol, start_date=start_date, end_date=end_date, adjust=adjust
                    )
                    if df_tx is not None and not df_tx.empty:
                        bars = _df_to_bars_em(
                            df_tx, date_key="date", open_key="open", high_key="high", low_key="low",
                            close_key="close", vol_key="amount", turn_key=None
                        )
            else:
                tx_symbol = _symbol_to_tx(symbol)
                df_tx = await _ak_call_kline_tx(
                    ak.stock_zh_a_hist_tx,
                    symbol=tx_symbol, start_date=start_date, end_date=end_date, adjust=adjust
                )
                if df_tx is not None and not df_tx.empty:
                    bars = _df_to_bars_em(
                        df_tx, date_key="date", open_key="open", high_key="high", low_key="low",
                        close_key="close", vol_key="amount", turn_key=None
                    )
                if not bars:
                    df = await _ak_call_kline_em(
                        ak.stock_zh_a_hist,
                        symbol=symbol, period=period,
                        start_date=start_date, end_date=end_date, adjust=adjust
                    )
                    if df is not None and not df.empty:
                        bars = _df_to_bars_em(df)
            if bars:
                await save_kline_to_ch(symbol, period, bars)
                success_count += 1
        except Exception as exc:
            error_count += 1
            failed_symbols.append(symbol)
            err_msg = f"{symbol}: {str(exc)[:100]}"
            if len(errors) < 10:
                errors.append(err_msg)
            await _append_task_log(task_id, "ERROR", err_msg)

    # 分批处理 (每批 50 只, 避免内存爆炸)
    batch_size = 50
    for i in range(0, len(symbols), batch_size):
        await _raise_if_cancelled(task_id)
        batch = symbols[i:i + batch_size]
        tasks = [_pull_one(s) for s in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
        # 每批更新进度
        await _update_task(task_id, success_count=success_count, error_count=error_count)
        done = min(i + batch_size, total)
        await _append_task_log(task_id, "INFO", f"已处理 {done}/{total} 成功 {success_count} 失败 {error_count}")
        logger.info("K-line sync progress: %d/%d (ok=%d, err=%d)",
                     done, total, success_count, error_count)

    await _set_watermark("kline", end_date.replace("-", ""), f"{period}_{adjust}")
    _pool = _CURRENT_SYNC_PROXY_POOL.get()
    _proxy = _CURRENT_SYNC_PROXY.get()
    if _pool:
        _elapsed = time.time() - _kline_start_time
        _throughput = (total / (_elapsed / 60.0)) if _elapsed > 0 else 0
        await _append_task_log(task_id, "INFO", f"动态并发代理池 本任务处理 {total} 只, 耗时 {_elapsed:.0f} 秒, 约 {_throughput:.0f} 只/分钟")
    elif _proxy:
        _elapsed = time.time() - _kline_start_time
        _throughput = (total / (_elapsed / 60.0)) if _elapsed > 0 else 0
        await _append_task_log(task_id, "INFO", f"代理 {_mask_proxy(_proxy)} 本任务处理 {total} 只, 耗时 {_elapsed:.0f} 秒, 约 {_throughput:.0f} 只/分钟")
    await _append_task_log(task_id, "INFO", f"任务结束: 成功 {success_count} 失败 {error_count}" + (f", failed_symbols={len(failed_symbols)}" if failed_symbols else ""))
    await _update_task(task_id, status="success", success_count=success_count,
                       error_count=error_count, error_detail="\n".join(errors) if errors else None,
                       failed_symbols=json.dumps(failed_symbols) if failed_symbols else None,
                       finished_at=datetime.utcnow())
    return {"success": True, "total": total, "ok": success_count, "err": error_count, "task_id": task_id}


async def _get_financial_existing_symbols() -> List[str]:
    """查询 stock_financial 表中已有数据的股票代码，用于续传时仅同步缺失股票。"""
    from sqlalchemy import select, distinct
    from src.core.db import get_session
    from src.models.market_sync import StockFinancial
    async for session in get_session():
        stmt = select(distinct(StockFinancial.symbol))
        result = await session.execute(stmt)
        return [row[0] for row in result.fetchall()]
    return []


async def sync_financial(sync_type: str = "full", symbols: List[str] = None, resume: bool = False, **kwargs) -> Dict[str, Any]:
    """同步财务指标: stock_financial_analysis_indicator → stock_financial 表。
    resume=True 时仅同步 stock_financial 中尚无数据的股票（断点续传），用于补全不全数据。"""
    import akshare as ak
    task_id = await _create_task_or_use("financial", "resume" if resume else sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    if not symbols:
        symbols = await _get_all_stock_codes()
    if not symbols:
        await _append_task_log(task_id, "ERROR", "无股票代码")
        await _update_task(task_id, status="failed", error_detail="无股票代码", finished_at=datetime.utcnow())
        return {"success": False, "error": "无股票代码, 请先同步 stock_list"}

    # 续传：仅同步库中尚无财务数据的股票
    if resume:
        existing = await _get_financial_existing_symbols()
        existing_set = set(existing)
        before_count = len(symbols)
        symbols = [s for s in symbols if s not in existing_set]
        skipped = before_count - len(symbols)
        await _append_task_log(
            task_id, "INFO",
            f"断点续传: 已有财务数据 {len(existing_set)} 只，跳过 {skipped} 只，待同步 {len(symbols)} 只"
        )
        if not symbols:
            await _update_task(task_id, status="success", total_count=0, success_count=0,
                               error_count=0, finished_at=datetime.utcnow())
            return {"success": True, "total": 0, "ok": 0, "err": 0, "task_id": task_id}

    from src.core.db import get_session
    from src.models.market_sync import StockFinancial
    import os

    success_count = 0
    error_count = 0
    failed_symbols: List[str] = []
    total = len(symbols)
    await _update_task(task_id, total_count=total)

    wm = await _get_watermark("financial", "") if sync_type == "incremental" else None
    new_row_count = 0
    max_report_date = wm or ""

    async def _pull_financial_one(symbol: str) -> tuple[str, Optional[Any], str]:
        df = await _ak_call(ak.stock_financial_analysis_indicator, symbol=symbol)
        if df is None or df.empty:
            return (symbol, None, "")
        mr = ""
        for _, row in df.iterrows():
            report_date = str(row.iloc[0]) if len(row) > 0 else ""
            if report_date and (sync_type == "full" or (wm is not None and report_date > wm)):
                if report_date > mr:
                    mr = report_date
        return (symbol, df, mr)

    batch_size = _get_sync_batch_size()
    for i in range(0, total, batch_size):
        await _raise_if_cancelled(task_id)
        batch = symbols[i:i + batch_size]
        results = await asyncio.gather(*[_pull_financial_one(s) for s in batch], return_exceptions=True)
        for symbol, df, mr in (r for r in results if not isinstance(r, Exception)):
            if df is None or df.empty:
                continue
            if mr and mr > max_report_date:
                max_report_date = mr
            try:
                rows_df = df if sync_type == "full" else df
                async for session in get_session():
                    for _, row in rows_df.iterrows():
                        report_date = str(row.iloc[0]) if len(row) > 0 else ""
                        if not report_date:
                            continue
                        if sync_type == "incremental" and wm is not None and report_date <= wm:
                            continue
                        fin = StockFinancial(
                            symbol=symbol,
                            report_date=report_date,
                            roe=_safe_float(row, "净资产收益率(%)"),
                            gross_margin=_safe_float(row, "销售毛利率(%)"),
                            net_margin=_safe_float(row, "销售净利率(%)"),
                            eps=_safe_float(row, "基本每股收益(元)"),
                            debt_ratio=_safe_float(row, "资产负债比率(%)"),
                            current_ratio=_safe_float(row, "流动比率"),
                            raw_data=row.to_json(force_ascii=False) if hasattr(row, 'to_json') else None,
                        )
                        session.add(fin)
                        if sync_type == "incremental":
                            new_row_count += 1
                    await session.commit()
                success_count += 1
            except Exception as exc:
                error_count += 1
                failed_symbols.append(symbol)
                logger.debug("Financial sync %s failed: %s", symbol, exc)
        for j, r in enumerate(results):
            if isinstance(r, Exception):
                if batch[j] not in failed_symbols:
                    failed_symbols.append(batch[j])
                logger.debug("Financial sync %s failed: %s", batch[j], r)

        await _update_task(task_id, success_count=success_count, error_count=error_count,
                           failed_symbols=json.dumps(failed_symbols) if failed_symbols else None)
        await _append_task_log(task_id, "INFO", f"已处理 {min(i + batch_size, total)}/{total} 成功 {success_count} 失败 {error_count}")

    if sync_type == "incremental" and new_row_count == 0:
        return await _return_sync_skipped(
            task_id, SYNC_SKIPPED_MESSAGE,
            category="financial", date_str=datetime.now().strftime("%Y-%m-%d")
        )

    # 规格 9.2/9.3：仅当整次任务无失败时更新 watermark，部分成功不推进
    if error_count == 0:
        await _set_watermark("financial", max_report_date if (sync_type == "incremental" and new_row_count > 0) else datetime.now().strftime("%Y-%m-%d"))
    await _append_task_log(task_id, "INFO", f"任务结束: 成功 {success_count} 失败 {error_count}" + (f", failed_symbols={len(failed_symbols)}" if failed_symbols else ""))
    await _update_task(task_id, status="success", success_count=success_count,
                       error_count=error_count, failed_symbols=json.dumps(failed_symbols) if failed_symbols else None,
                       finished_at=datetime.utcnow())
    return {"success": True, "total": total, "ok": success_count, "err": error_count, "task_id": task_id}


async def sync_sector(sync_type: str = "full", **kwargs) -> Dict[str, Any]:
    """同步行业+概念板块列表及成分股。使用 UPSERT 避免增量/重复执行时 1062 重复键。
    先写 stock_sectors，再按板块拉取成分股写入 stock_sector_members（行业 industry_cons_em，概念 concept_cons_em）。"""
    import akshare as ak
    from sqlalchemy import bindparam, select, text, func

    task_id = await _create_task_or_use("sector", sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    from src.core.db import get_session
    from src.models.market_sync import StockSector, StockSectorMember

    _dsn = os.environ.get("MYSQL_DSN", "")
    use_mysql_upsert = "sqlite" not in _dsn.lower()

    ind_df = await _ak_call(ak.stock_board_industry_name_em)
    con_df = await _ak_call(ak.stock_board_concept_name_em)
    def _sector_row_count(df) -> int:
        if df is None or df.empty:
            return 0
        return sum(1 for _, r in df.iterrows() if r.get("板块代码") and r.get("板块名称"))

    expected_sectors = _sector_row_count(ind_df) + _sector_row_count(con_df)
    if sync_type == "incremental" and expected_sectors > 0:
        async for session in get_session():
            r = await session.execute(select(func.count()).select_from(StockSector))
            if (r.scalar() or 0) == expected_sectors:
                return await _return_sync_skipped(
                    task_id, SYNC_SKIPPED_MESSAGE,
                    category="sector", date_str=datetime.now().strftime("%Y-%m-%d")
                )
            break

    # 待拉成分股的板块列表 (sector_type, sector_code, sector_name)
    sectors_to_fetch: List[tuple] = []
    if ind_df is not None and not ind_df.empty:
        for _, r in ind_df.iterrows():
            if r.get("板块代码") and r.get("板块名称"):
                sectors_to_fetch.append(("industry", str(r.get("板块代码", "")), str(r.get("板块名称", ""))))
    if con_df is not None and not con_df.empty:
        for _, r in con_df.iterrows():
            if r.get("板块代码") and r.get("板块名称"):
                sectors_to_fetch.append(("concept", str(r.get("板块代码", "")), str(r.get("板块名称", ""))))

    count = 0
    try:
        async for session in get_session():
            now = datetime.utcnow()
            if use_mysql_upsert:
                from sqlalchemy.dialects.mysql import insert as mysql_insert
                if ind_df is not None and not ind_df.empty:
                    rows = [
                        {"sector_type": "industry", "sector_code": str(r.get("板块代码", "")), "sector_name": str(r.get("板块名称", "")), "updated_at": now}
                        for _, r in ind_df.iterrows() if r.get("板块代码") and r.get("板块名称")
                    ]
                    if rows:
                        stmt = mysql_insert(StockSector).values(rows)
                        stmt = stmt.on_duplicate_key_update(sector_name=stmt.inserted.sector_name, updated_at=stmt.inserted.updated_at)
                        await session.execute(stmt)
                        count += len(rows)
                if con_df is not None and not con_df.empty:
                    rows = [
                        {"sector_type": "concept", "sector_code": str(r.get("板块代码", "")), "sector_name": str(r.get("板块名称", "")), "updated_at": now}
                        for _, r in con_df.iterrows() if r.get("板块代码") and r.get("板块名称")
                    ]
                    if rows:
                        stmt = mysql_insert(StockSector).values(rows)
                        stmt = stmt.on_duplicate_key_update(sector_name=stmt.inserted.sector_name, updated_at=stmt.inserted.updated_at)
                        await session.execute(stmt)
                        count += len(rows)
            else:
                if ind_df is not None and not ind_df.empty:
                    for _, row in ind_df.iterrows():
                        code = str(row.get("板块代码", ""))
                        name = str(row.get("板块名称", ""))
                        if code and name:
                            session.add(StockSector(sector_type="industry", sector_code=code, sector_name=name))
                            count += 1
                if con_df is not None and not con_df.empty:
                    for _, row in con_df.iterrows():
                        code = str(row.get("板块代码", ""))
                        name = str(row.get("板块名称", ""))
                        if code and name:
                            session.add(StockSector(sector_type="concept", sector_code=code, sector_name=name))
                            count += 1
            await session.commit()
            break  # get_session() 只用一个 session

        # 成分股：按板块拉取并写入 stock_sector_members
        member_count = 0
        if sectors_to_fetch:
            sector_codes = [s[1] for s in sectors_to_fetch]
            async for session in get_session():
                if sector_codes:
                    _chunk_size = 100
                    for _i in range(0, len(sector_codes), _chunk_size):
                        chunk = sector_codes[_i : _i + _chunk_size]
                        stmt_del = text("DELETE FROM stock_sector_members WHERE sector_code IN :codes").bindparams(bindparam("codes", expanding=True))
                        await session.execute(stmt_del, {"codes": chunk})
                        await session.commit()
                break

            async def _pull_sector_one(stype: str, code: str, name: str) -> tuple[str, str, str, int]:
                if stype == "industry":
                    df_cons = await _ak_call(ak.stock_board_industry_cons_em, symbol=name)
                else:
                    df_cons = await _ak_call(ak.stock_board_concept_cons_em, symbol=name)
                if df_cons is None or df_cons.empty:
                    return (stype, code, name, 0)
                now_m = datetime.utcnow()
                written = 0
                async for session in get_session():
                    if use_mysql_upsert:
                        from sqlalchemy.dialects.mysql import insert as mysql_insert
                        batch = []
                        for _, row in df_cons.iterrows():
                            sym = str(row.get("代码", "")).strip()
                            if not sym:
                                continue
                            sym_name = row.get("名称")
                            sym_name = (str(sym_name)[:64] if sym_name is not None and str(sym_name) != "nan" else None)
                            batch.append({"sector_code": code, "symbol": sym, "symbol_name": sym_name, "updated_at": now_m})
                        if batch:
                            stmt = mysql_insert(StockSectorMember).values(batch)
                            stmt = stmt.on_duplicate_key_update(symbol_name=stmt.inserted.symbol_name, updated_at=stmt.inserted.updated_at)
                            await session.execute(stmt)
                            written = len(batch)
                    else:
                        for _, row in df_cons.iterrows():
                            sym = str(row.get("代码", "")).strip()
                            if not sym:
                                continue
                            sym_name = row.get("名称")
                            sym_name = (str(sym_name)[:64] if sym_name is not None and str(sym_name) != "nan" else None)
                            session.merge(StockSectorMember(sector_code=code, symbol=sym, symbol_name=sym_name))
                            written += 1
                    await session.commit()
                    break
                return (stype, code, name, written)

            batch_size = _get_sync_batch_size()
            for i in range(0, len(sectors_to_fetch), batch_size):
                if i % 40 == 0:
                    await _raise_if_cancelled(task_id)
                batch = sectors_to_fetch[i : i + batch_size]
                results = await asyncio.gather(
                    *[_pull_sector_one(stype, code, name) for stype, code, name in batch],
                    return_exceptions=True,
                )
                for r in results:
                    if isinstance(r, Exception):
                        logger.warning("sector members failed: %s", r)
                        continue
                    _, _, _, written = r
                    member_count += written
                if (i + len(batch)) % 50 == 0 or i + len(batch) == len(sectors_to_fetch):
                    await _append_task_log(task_id, "INFO", f"成分股进度: {i + len(batch)}/{len(sectors_to_fetch)} 板块, 已写入 {member_count} 条")

        total_records = count + member_count
        await _set_watermark("sector", datetime.now().strftime("%Y-%m-%d"))
        await _append_task_log(task_id, "INFO", f"任务结束: 成功 sectors={count} members={member_count}")
        await _update_task(task_id, status="success", total_count=total_records,
                           success_count=total_records, finished_at=datetime.utcnow())
        return {"success": True, "total": total_records}

    except Exception as exc:
        logger.error("sync_sector failed: %s", exc)
        await _append_task_log(task_id, "ERROR", str(exc)[:500])
        await _update_task(task_id, status="failed", error_detail=str(exc)[:500],
                           finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}


async def sync_lhb(sync_type: str = "full", **kwargs) -> Dict[str, Any]:
    """同步龙虎榜: stock_lhb_detail_em"""
    import akshare as ak
    task_id = await _create_task_or_use("lhb", sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    from src.core.db import get_session
    from src.models.market_sync import StockLHB

    try:
        # 获取最近交易日龙虎榜
        if sync_type == "incremental":
            wm = await _get_watermark("lhb", "")
            start = wm or (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
        else:
            wm = None
            start = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")

        try:
            df = await _ak_call(ak.stock_lhb_detail_em, start_date=start, end_date=end)
        except (TypeError, KeyError) as _e:
            # akshare stock_lhb_detail_em 在东方财富返回 result=null 时对 data_json["result"]["pages"] 下标会抛 TypeError
            if "NoneType" in str(_e) and "not subscriptable" in str(_e):
                df = None
                logger.info("sync_lhb: API returned null result, treating as no data: %s", _e)
            else:
                raise
        if df is None or df.empty:
            await _append_task_log(task_id, "INFO", "任务结束: 无数据")
            await _update_task(task_id, status="success", total_count=0, finished_at=datetime.utcnow())
            return {"success": True, "total": 0}

        # akshare 接口列名可能为「上榜日」「龙虎榜成交额」，与历史「上榜日期」「成交额」兼容
        if "上榜日期" not in df.columns and "上榜日" in df.columns:
            df["上榜日期"] = df["上榜日"].astype(str)
        if "成交额" not in df.columns and "龙虎榜成交额" in df.columns:
            df["成交额"] = df["龙虎榜成交额"]

        if sync_type == "incremental" and wm:
            df["_dt"] = df["上榜日期"].astype(str).str.replace("-", "")
            df = df[df["_dt"] > wm].drop(columns=["_dt"], errors="ignore")
            if df.empty:
                return await _return_sync_skipped(
                    task_id, SYNC_SKIPPED_MESSAGE_DAILY,
                    category="lhb", date_str=end
                )

        count = 0
        async for session in get_session():
            for _, row in df.iterrows():
                lhb = StockLHB(
                    symbol=str(row.get("代码", "")),
                    symbol_name=str(row.get("名称", "")),
                    trade_date=str(row.get("上榜日期", "")),
                    reason=str(row.get("解读", row.get("上榜原因", ""))),
                    close_price=_safe_float(row, "收盘价"),
                    change_pct=_safe_float(row, "涨跌幅"),
                    net_buy=_safe_float(row, "龙虎榜净买额"),
                    buy_amount=_safe_float(row, "龙虎榜买入额"),
                    sell_amount=_safe_float(row, "龙虎榜卖出额"),
                    turnover=_safe_float(row, "成交额"),
                )
                session.add(lhb)
                count += 1
            await session.commit()

        await _set_watermark("lhb", end)
        await _append_task_log(task_id, "INFO", f"任务结束: 成功 count={count}")
        await _update_task(task_id, status="success", total_count=count,
                           success_count=count, finished_at=datetime.utcnow())
        return {"success": True, "total": count}

    except Exception as exc:
        logger.error("sync_lhb failed: %s", exc)
        await _append_task_log(task_id, "ERROR", str(exc)[:500])
        await _update_task(task_id, status="failed", error_detail=str(exc)[:500],
                           finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}


# 北向资金同步代码版本：若后端未重启，会仍用旧代码导致 stock_hsgt_north_net_flow_in_em 报错
NORTHBOUND_SYNC_VERSION = "stock_hsgt_hist_em_v1"


async def sync_northbound(sync_type: str = "full", **kwargs) -> Dict[str, Any]:
    """同步北向资金: akshare stock_hsgt_hist_em（北向资金/沪股通/深股通）"""
    import akshare as ak
    task_id = await _create_task_or_use("northbound", sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", f"任务开始: 北向资金 (backend={NORTHBOUND_SYNC_VERSION})")
    await _append_proxy_info_task_start(task_id)
    logger.info("sync_northbound: %s", NORTHBOUND_SYNC_VERSION)

    from src.core.db import get_session
    from src.models.market_sync import NorthboundFlow

    try:
        # 北向资金日度数据（akshare: stock_hsgt_hist_em，勿用已废弃的 stock_hsgt_north_net_flow_in_em）
        fn = getattr(ak, "stock_hsgt_hist_em", None)
        if fn is None:
            raise AttributeError("akshare 缺少 stock_hsgt_hist_em，请升级: pip install -U akshare")
        df = await _ak_call(fn, symbol="北向资金")
        if df is None or df.empty:
            await _append_task_log(task_id, "INFO", "任务结束: 无数据")
            await _update_task(task_id, status="success", total_count=0, finished_at=datetime.utcnow())
            return {"success": True, "total": 0}

        _date_col = "日期"
        _wm = await _get_watermark("northbound", "") if sync_type == "incremental" else None

        # 增量：只保留 trade_date > watermark 的行，避免重复插入导致 1062
        if sync_type == "incremental" and _wm and _date_col in df.columns:
            df["_dt"] = df[_date_col].astype(str).str[:10]
            df = df[df["_dt"] > _wm].drop(columns=["_dt"], errors="ignore")
            if df.empty:
                return await _return_sync_skipped(
                    task_id, SYNC_SKIPPED_MESSAGE_DAILY,
                    category="northbound", date_str=datetime.now().strftime("%Y-%m-%d")
                )

        # 可选：拉取沪股通、深股通分项用于 sh_net_buy / sz_net_buy
        sh_by_date: Dict[str, float] = {}
        sz_by_date: Dict[str, float] = {}
        try:
            df_sh = await _ak_call(fn, symbol="沪股通")
            if df_sh is not None and not df_sh.empty and "日期" in df_sh.columns:
                col = "当日成交净买额" if "当日成交净买额" in df_sh.columns else df_sh.columns[1]
                for _, row in df_sh.iterrows():
                    d = str(row.get("日期", ""))[:10]
                    if d:
                        sh_by_date[d] = _safe_float(row, col)
            df_sz = await _ak_call(fn, symbol="深股通")
            if df_sz is not None and not df_sz.empty and "日期" in df_sz.columns:
                col = "当日成交净买额" if "当日成交净买额" in df_sz.columns else df_sz.columns[1]
                for _, row in df_sz.iterrows():
                    d = str(row.get("日期", ""))[:10]
                    if d:
                        sz_by_date[d] = _safe_float(row, col)
        except Exception:
            pass

        # 列名：stock_hsgt_hist_em 返回「当日成交净买额」（单位亿元）
        total_col = "当日成交净买额" if "当日成交净买额" in df.columns else "当日净流入"
        _dsn = os.environ.get("MYSQL_DSN", "")
        use_mysql_upsert = "sqlite" not in _dsn.lower()
        now_nb = datetime.utcnow()
        rows = []
        for _, row in df.iterrows():
            trade_date = str(row.get("日期", str(row.iloc[0]) if len(row) > 0 else ""))[:10]
            if not trade_date:
                continue
            rows.append({
                "trade_date": trade_date,
                "direction": "north",
                "total_net_buy": _safe_float(row, total_col),
                "sh_net_buy": sh_by_date.get(trade_date) if sh_by_date else _safe_float(row, "沪股通净流入"),
                "sz_net_buy": sz_by_date.get(trade_date) if sz_by_date else _safe_float(row, "深股通净流入"),
                "updated_at": now_nb,
            })
        count = len(rows)
        async for session in get_session():
            if use_mysql_upsert and rows:
                from sqlalchemy.dialects.mysql import insert as mysql_insert
                stmt = mysql_insert(NorthboundFlow).values(rows)
                stmt = stmt.on_duplicate_key_update(
                    total_net_buy=stmt.inserted.total_net_buy,
                    sh_net_buy=stmt.inserted.sh_net_buy,
                    sz_net_buy=stmt.inserted.sz_net_buy,
                    updated_at=stmt.inserted.updated_at,
                )
                await session.execute(stmt)
            else:
                for r in rows:
                    session.add(NorthboundFlow(**r))
            await session.commit()

        await _set_watermark("northbound", datetime.now().strftime("%Y-%m-%d"))
        await _append_task_log(task_id, "INFO", f"任务结束: 成功 count={count}")
        await _update_task(task_id, status="success", total_count=count,
                           success_count=count, finished_at=datetime.utcnow())
        return {"success": True, "total": count}

    except Exception as exc:
        logger.error("sync_northbound failed: %s", exc)
        await _append_task_log(task_id, "ERROR", str(exc)[:500])
        await _update_task(task_id, status="failed", error_detail=str(exc)[:500],
                           finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}


async def sync_northbound_hold(sync_type: str = "full", **kwargs) -> Dict[str, Any]:
    """同步北向持股排行（个股截面）: stock_hsgt_hold_stock_em → northbound_hold_stock 表，供页面读库+Redis 加速。按日保留历史，仅写入/更新当日截面。"""
    import akshare as ak
    from sqlalchemy import delete

    task_id = await _create_task_or_use("northbound_hold", sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    from src.core.db import get_session
    from src.models.market_sync import NorthboundHoldStock

    _dsn = os.environ.get("MYSQL_DSN", "")
    use_mysql_upsert = "sqlite" not in _dsn.lower()
    now_nb = datetime.utcnow()

    # 默认同步 (北向, 今日排行)；可扩展 5日/10日。强依赖交易日，使用最近交易日。
    to_sync = [("北向", "今日排行")]
    try:
        trade_date_ymd = await get_last_trading_date_str(include_today=True)
        trade_date = trade_date_ymd[:4] + "-" + trade_date_ymd[4:6] + "-" + trade_date_ymd[6:]
        if sync_type == "incremental":
            wm = await _get_watermark("northbound_hold", "")
            if wm is not None and trade_date <= wm:
                return await _return_sync_skipped(
                    task_id, SYNC_SKIPPED_MESSAGE_DAILY,
                    category="northbound_hold", date_str=trade_date
                )
        total = 0
        for market, indicator in to_sync:
            df = await _ak_call(ak.stock_hsgt_hold_stock_em, market=market, indicator=indicator)
            if df is None or df.empty:
                await _append_task_log(task_id, "INFO", f"{market} {indicator}: 无数据")
                continue
            rows = []
            for _, row in df.iterrows():
                rows.append({
                    "trade_date": trade_date,
                    "market": market,
                    "indicator": indicator,
                    "code": str(row.get("代码", "")),
                    "name": str(row.get("名称", "")),
                    "close": _safe_float(row, "今日收盘价"),
                    "change_pct": _safe_float(row, "今日涨跌幅"),
                    "hold_shares": _safe_float(row, "持股股数"),
                    "hold_value": _safe_float(row, "持股市值"),
                    "float_ratio": _safe_float(row, "持股数量占A股百分比"),
                    "increase_shares": _safe_float(row, "增持股数"),
                    "increase_value": _safe_float(row, "增持市值"),
                    "sector": str(row.get("所属板块", "")),
                    "updated_at": now_nb,
                })
            total += len(rows)
            async for session in get_session():
                if use_mysql_upsert and rows:
                    from sqlalchemy.dialects.mysql import insert as mysql_insert
                    stmt = mysql_insert(NorthboundHoldStock).values(rows)
                    stmt = stmt.on_duplicate_key_update(
                        name=stmt.inserted.name,
                        close=stmt.inserted.close,
                        change_pct=stmt.inserted.change_pct,
                        hold_shares=stmt.inserted.hold_shares,
                        hold_value=stmt.inserted.hold_value,
                        float_ratio=stmt.inserted.float_ratio,
                        increase_shares=stmt.inserted.increase_shares,
                        increase_value=stmt.inserted.increase_value,
                        sector=stmt.inserted.sector,
                        updated_at=stmt.inserted.updated_at,
                    )
                    await session.execute(stmt)
                else:
                    await session.execute(
                        delete(NorthboundHoldStock).where(
                            NorthboundHoldStock.trade_date == trade_date,
                            NorthboundHoldStock.market == market,
                            NorthboundHoldStock.indicator == indicator,
                        )
                    )
                    for r in rows:
                        session.add(NorthboundHoldStock(**r))
                await session.commit()
            await _append_task_log(task_id, "INFO", f"{market} {indicator}: 写入 {len(rows)} 条")
        await _set_watermark("northbound_hold", trade_date)
        await _update_task(task_id, status="success", total_count=total, success_count=total,
                           finished_at=datetime.utcnow())
        return {"success": True, "total": total}
    except Exception as exc:
        logger.error("sync_northbound_hold failed: %s", exc)
        await _append_task_log(task_id, "ERROR", str(exc)[:500])
        await _update_task(task_id, status="failed", error_detail=str(exc)[:500],
                           finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}


async def sync_limit_updown(sync_type: str = "full", **kwargs) -> Dict[str, Any]:
    """同步涨跌停池: stock_zt_pool_em。强依赖交易日，使用最近交易日作为 trade_date。"""
    import akshare as ak
    task_id = await _create_task_or_use("limit_updown", sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    from src.core.db import get_session
    from src.models.market_sync import StockLimitUpDown

    try:
        trade_date = await get_last_trading_date_str(include_today=True)
        if sync_type == "incremental":
            wm = await _get_watermark("limit_updown", "")
            if wm is not None and trade_date <= wm:
                return await _return_sync_skipped(
                    task_id, SYNC_SKIPPED_MESSAGE_DAILY,
                    category="limit_updown", date_str=trade_date
                )
        df = await _ak_call(ak.stock_zt_pool_em, date=trade_date)

        count = 0
        if df is not None and not df.empty:
            trade_date_iso = trade_date[:4] + "-" + trade_date[4:6] + "-" + trade_date[6:]
            async for session in get_session():
                for _, row in df.iterrows():
                    rec = StockLimitUpDown(
                        symbol=str(row.get("代码", "")),
                        symbol_name=str(row.get("名称", "")),
                        trade_date=trade_date_iso,
                        limit_type="up",
                        close_price=_safe_float(row, "最新价"),
                        change_pct=_safe_float(row, "涨跌幅"),
                        first_limit_time=str(row.get("首次封板时间", "")),
                        last_limit_time=str(row.get("最后封板时间", "")),
                        open_count=_safe_int(row, "炸板次数"),
                        continuous_days=_safe_int(row, "连板数"),
                    )
                    session.add(rec)
                    count += 1
                await session.commit()

        await _set_watermark("limit_updown", trade_date)
        await _append_task_log(task_id, "INFO", f"任务结束: 成功 count={count}")
        await _update_task(task_id, status="success", total_count=count,
                           success_count=count, finished_at=datetime.utcnow())
        return {"success": True, "total": count}

    except Exception as exc:
        logger.error("sync_limit_updown failed: %s", exc)
        await _append_task_log(task_id, "ERROR", str(exc)[:500])
        await _update_task(task_id, status="failed", error_detail=str(exc)[:500],
                           finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}


async def _get_capital_flow_existing_symbols(watermark: Optional[str]) -> List[str]:
    """查询 stock_capital_flow 中已有 trade_date > watermark 的 symbol，用于续传时跳过。watermark 为 None 时不按日期过滤（即全部已有）。"""
    try:
        from sqlalchemy import select, distinct
        from src.core.db import get_session
        from src.models.market_sync import StockCapitalFlow
        async for session in get_session():
            stmt = select(distinct(StockCapitalFlow.symbol))
            if watermark:
                stmt = stmt.where(StockCapitalFlow.trade_date > watermark)
            result = await session.execute(stmt)
            return [row[0] for row in result.fetchall()]
    except Exception as exc:
        logger.warning("_get_capital_flow_existing_symbols failed: %s", exc)
    return []


async def sync_capital_flow(sync_type: str = "full", symbols: List[str] = None, resume: bool = False, **kwargs) -> Dict[str, Any]:
    """同步个股资金流向: stock_individual_fund_flow。
    规格 3.6/6.2：增量时先做任务级先判断（最近交易日 vs watermark），
    若已同步到最新则直接返回、不发起任何按 symbol 的请求。
    resume=True 时仅同步库中尚无 trade_date > watermark 记录的 symbol。
    """
    import akshare as ak
    task_id = await _create_task_or_use("capital_flow", "resume" if resume else sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    if not symbols:
        symbols = await _get_all_stock_codes()

    from src.core.db import get_session
    from src.models.market_sync import StockCapitalFlow

    wm = await _get_watermark("capital_flow", "") if (sync_type == "incremental" or resume) else None
    if resume:
        existing = await _get_capital_flow_existing_symbols(wm)
        existing_set = set(existing)
        before_count = len(symbols)
        symbols = [s for s in symbols if s not in existing_set]
        skipped = before_count - len(symbols)
        await _append_task_log(
            task_id, "INFO",
            f"断点续传: 已有 trade_date>watermark 数据 {len(existing_set)} 只，跳过 {skipped} 只，待同步 {len(symbols)} 只"
        )
        if not symbols:
            await _update_task(task_id, status="success", total_count=0, success_count=0,
                               error_count=0, finished_at=datetime.utcnow())
            return {"success": True, "total": 0, "ok": 0, "err": 0}

    success_count = 0
    error_count = 0
    total = len(symbols)
    await _update_task(task_id, total_count=total)

    new_row_count = 0
    new_row_count = 0
    today = datetime.now().strftime("%Y-%m-%d")
    last_td_iso = None
    if resume or sync_type == "incremental":
        last_trading_date = await get_last_trading_date_str(include_today=True)
        last_td_iso = f"{last_trading_date[:4]}-{last_trading_date[4:6]}-{last_trading_date[6:8]}"

    # 规格 3.6/6.2：增量任务级先判断。在遍历任何 symbol 之前，若 最近交易日 <= watermark 则直接返回，不请求任何接口。
    if sync_type == "incremental":
        if wm is not None and last_td_iso and last_td_iso <= wm:
            today_ymd = datetime.now().strftime("%Y%m%d")
            skip_message = (
                SYNC_SKIPPED_MESSAGE_MARKET_CLOSED
                if today_ymd > last_trading_date
                else SYNC_SKIPPED_MESSAGE_DAILY
            )
            return await _return_sync_skipped(
                task_id, skip_message,
                category="capital_flow", date_str=last_td_iso
            )

    async def _pull_capital_flow_one(symbol: str) -> tuple[str, Optional[Any]]:
        df = await _ak_call(ak.stock_individual_fund_flow, stock=symbol, market="sh" if symbol.startswith("6") else "sz")
        return (symbol, df)

    batch_size = _get_sync_batch_size()
    for i in range(0, total, batch_size):
        await _raise_if_cancelled(task_id)
        batch = symbols[i:i + batch_size]
        results = await asyncio.gather(*[_pull_capital_flow_one(s) for s in batch], return_exceptions=True)
        for symbol, df in (r for r in results if not isinstance(r, Exception)):
            if df is None or df.empty:
                continue
            try:
                async for session in get_session():
                    for _, row in df.tail(30).iterrows():  # 最近30天
                        trade_date = str(row.get("日期", str(row.iloc[0]) if len(row) > 0 else ""))[:10]
                        if not trade_date:
                            continue
                        if (sync_type == "incremental" or resume) and wm is not None and trade_date <= wm:
                            continue
                        cf = StockCapitalFlow(
                            symbol=symbol,
                            trade_date=trade_date,
                            main_net_inflow=_safe_float(row, "主力净流入-净额"),
                            small_net_inflow=_safe_float(row, "小单净流入-净额"),
                            medium_net_inflow=_safe_float(row, "中单净流入-净额"),
                            large_net_inflow=_safe_float(row, "大单净流入-净额"),
                            super_large_net_inflow=_safe_float(row, "超大单净流入-净额"),
                        )
                        session.add(cf)
                        if sync_type == "incremental" or resume:
                            new_row_count += 1
                    await session.commit()
                success_count += 1
            except Exception as exc:
                error_count += 1
                logger.debug("Capital flow sync %s failed: %s", symbol, exc)
        for j, r in enumerate(results):
            if isinstance(r, Exception):
                error_count += 1
                logger.debug("Capital flow sync %s failed: %s", batch[j], r)

        await _update_task(task_id, success_count=success_count, error_count=error_count)
        await _append_task_log(task_id, "INFO", f"已处理 {min(i + batch_size, total)}/{total} 成功 {success_count} 失败 {error_count}")

    if (sync_type == "incremental" or resume) and new_row_count == 0:
        return await _return_sync_skipped(
            task_id, SYNC_SKIPPED_MESSAGE_DAILY,
            category="capital_flow", date_str=last_td_iso if last_td_iso else today
        )

    # 规格 9.2/9.3：仅当整次任务无失败时更新 watermark，部分成功不推进。增量/续传用最近交易日（YYYY-MM-DD），全量用自然日。
    if error_count == 0:
        await _set_watermark("capital_flow", last_td_iso if ((sync_type == "incremental" or resume) and last_td_iso) else today)
    await _append_task_log(task_id, "INFO", f"任务结束: 成功 {success_count} 失败 {error_count}")
    await _update_task(task_id, status="success", success_count=success_count,
                       error_count=error_count, finished_at=datetime.utcnow())
    return {"success": True, "total": total, "ok": success_count, "err": error_count}


async def sync_trade_calendar(sync_type: str = "full", **kwargs) -> Dict[str, Any]:
    """同步交易所交易日历: tool_trade_date_hist_sina() → exchange_trading_dates。全量/增量均为拉全量覆盖。"""
    import akshare as ak
    from sqlalchemy import text

    task_id = await _create_task_or_use("trade_calendar", sync_type, kwargs.pop("task_id", None))
    if not task_id:
        return {"success": False, "error": "创建任务记录失败"}
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    from src.core.db import get_session
    from src.models.market_sync import ExchangeTradingDate

    try:
        if sync_type == "incremental":
            wm = await _get_watermark("trade_calendar", "")
            today = datetime.now().strftime("%Y-%m-%d")
            if wm is not None and wm >= today:
                return await _return_sync_skipped(
                    task_id, SYNC_SKIPPED_MESSAGE_DAILY,
                    category="trade_calendar", date_str=today
                )

        df = await _ak_call(ak.tool_trade_date_hist_sina)
        if df is None or df.empty:
            await _append_task_log(task_id, "INFO", "任务结束: 无数据")
            await _update_task(task_id, status="success", total_count=0, finished_at=datetime.utcnow())
            return {"success": True, "total": 0}

        col = "trade_date" if "trade_date" in df.columns else df.columns[0]
        dates = df[col].astype(str).str.strip()
        dates = [str(x) for x in dates[dates.str.match(r"^\d{4}-\d{2}-\d{2}$")].unique().tolist()]

        async for session in get_session():
            await session.execute(text("DELETE FROM exchange_trading_dates"))
            for d in dates:
                row = ExchangeTradingDate(trade_date=str(d))
                session.add(row)
            await session.commit()

        await _set_watermark("trade_calendar", datetime.now().strftime("%Y-%m-%d"))
        await _append_task_log(task_id, "INFO", f"任务结束: 成功 count={len(dates)}")
        await _update_task(task_id, status="success", total_count=len(dates), success_count=len(dates), finished_at=datetime.utcnow())
        return {"success": True, "total": len(dates)}
    except Exception as exc:
        logger.error("sync_trade_calendar failed: %s", exc)
        await _append_task_log(task_id, "ERROR", str(exc)[:500])
        await _update_task(task_id, status="failed", error_detail=str(exc)[:500], finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}


async def _get_margin_existing_symbols(trade_date_iso: str) -> List[str]:
    """查询 stock_margin_trading 中已有 trade_date 的 symbol，用于续传时跳过。"""
    try:
        from sqlalchemy import select, distinct
        from src.core.db import get_session
        from src.models.market_sync import StockMarginTrading
        async for session in get_session():
            stmt = select(distinct(StockMarginTrading.symbol)).where(
                StockMarginTrading.trade_date == trade_date_iso
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.fetchall()]
    except Exception as exc:
        logger.warning("_get_margin_existing_symbols failed: %s", exc)
    return []


async def sync_margin(sync_type: str = "full", resume: bool = False, **kwargs) -> Dict[str, Any]:
    """同步融资融券。接口按交易日取数，非交易日会返回空/异常(Length mismatch)，故用最近交易日。
    resume=True 时仅插入当日库中尚无记录的 symbol，不覆盖已有。"""
    import akshare as ak
    task_id = await _create_task_or_use("margin", "resume" if resume else sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    from src.core.db import get_session
    from src.models.market_sync import StockMarginTrading

    try:
        # 必须用交易日，否则 akshare 内部可能报 Length mismatch (Expected axis has 0 elements)
        trade_date = await get_last_trading_date_str(include_today=True)
        trade_date_iso = trade_date[:4] + "-" + trade_date[4:6] + "-" + trade_date[6:]
        if not resume and sync_type == "incremental":
            wm = await _get_watermark("margin", "")
            if wm is not None and trade_date <= wm:
                return await _return_sync_skipped(
                    task_id, SYNC_SKIPPED_MESSAGE_DAILY,
                    category="margin", date_str=trade_date
                )

        existing_set: set = set()
        if resume:
            existing = await _get_margin_existing_symbols(trade_date_iso)
            existing_set = set(existing)
            await _append_task_log(
                task_id, "INFO",
                f"断点续传: 当日已有 {len(existing_set)} 只，仅同步未存在标的"
            )

        df = await _ak_call(ak.stock_margin_detail_sse, date=trade_date)

        count = 0
        n_skip = 0
        if df is not None and not df.empty:
            async for session in get_session():
                for _, row in df.iterrows():
                    symbol = str(row.get("标的证券代码", row.get("证券代码", "")))
                    if not symbol:
                        continue
                    if resume and symbol in existing_set:
                        n_skip += 1
                        continue
                    mt = StockMarginTrading(
                        symbol=symbol,
                        trade_date=trade_date_iso,
                        rz_balance=_safe_float(row, "融资余额"),
                        rz_buy=_safe_float(row, "融资买入额"),
                        rz_repay=_safe_float(row, "融资偿还额"),
                        rq_balance=_safe_float(row, "融券余量"),  # 上交所接口列为 融券余量
                        rq_sell=_safe_float(row, "融券卖出量"),
                        rq_repay=_safe_float(row, "融券偿还量"),
                    )
                    session.add(mt)
                    count += 1
                await session.commit()

        if resume:
            await _append_task_log(
                task_id, "INFO",
                f"断点续传: 已有 {len(existing_set)} 只，跳过 {n_skip} 只，待同步 {count} 只"
            )
        await _set_watermark("margin", trade_date)
        await _append_task_log(task_id, "INFO", f"任务结束: 成功 count={count}")
        await _update_task(task_id, status="success", total_count=count,
                           success_count=count, finished_at=datetime.utcnow())
        return {"success": True, "total": count}

    except Exception as exc:
        logger.error("sync_margin failed: %s", exc)
        await _append_task_log(task_id, "ERROR", str(exc)[:500])
        await _update_task(task_id, status="failed", error_detail=str(exc)[:500],
                           finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}


async def _get_block_trade_existing_symbols(dates: List[str]) -> List[str]:
    """查询 stock_block_trade 中在给定日期区间内已有记录的 symbol，用于续传时跳过。"""
    try:
        from sqlalchemy import select, distinct
        from src.core.db import get_session
        from src.models.market_sync import StockBlockTrade
        if not dates:
            return []
        async for session in get_session():
            stmt = select(distinct(StockBlockTrade.symbol)).where(
                StockBlockTrade.trade_date.in_(dates)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.fetchall()]
    except Exception as exc:
        logger.warning("_get_block_trade_existing_symbols failed: %s", exc)
    return []


async def sync_block_trade(sync_type: str = "full", resume: bool = False, **kwargs) -> Dict[str, Any]:
    """同步大宗交易。接口 stock_dzjy_mrmx(symbol='A股', start_date, end_date) YYYYMMDD；
    增量=最近交易日一天，全量=最近 N 天；按日期区间先删后插；watermark 为 YYYY-MM-DD。
    resume=True 时不删表，仅对区间内尚无任何记录的 symbol 插入新行。"""
    import akshare as ak
    from datetime import date as date_type
    from sqlalchemy import bindparam, text

    task_id = await _create_task_or_use("block_trade", "resume" if resume else sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    from src.core.db import get_session
    from src.models.market_sync import StockBlockTrade

    try:
        last_td = await get_last_trading_date_str(include_today=True)
        last_td_iso = last_td[:4] + "-" + last_td[4:6] + "-" + last_td[6:]
        if not resume and sync_type == "incremental":
            wm = await _get_watermark("block_trade", "")
            if wm is not None and last_td_iso <= wm:
                return await _return_sync_skipped(
                    task_id, SYNC_SKIPPED_MESSAGE_DAILY,
                    category="block_trade", date_str=last_td_iso
                )
        if resume or sync_type != "incremental":
            # 续传或全量：日期范围与全量一致（最近 30 天）
            end_date_ymd = last_td
            end_d = date_type(int(last_td[:4]), int(last_td[4:6]), int(last_td[6:8]))
            start_d = end_d - timedelta(days=30)
            start_date_ymd = start_d.strftime("%Y%m%d")
            dates_iso = []
            d = date_type(int(start_date_ymd[:4]), int(start_date_ymd[4:6]), int(start_date_ymd[6:8]))
            while d <= end_d:
                dates_iso.append(d.strftime("%Y-%m-%d"))
                d += timedelta(days=1)
        else:
            start_date_ymd = end_date_ymd = last_td
            dates_iso = [last_td_iso]

        existing_set: set = set()
        if resume:
            existing = await _get_block_trade_existing_symbols(dates_iso)
            existing_set = set(existing)
            await _append_task_log(
                task_id, "INFO",
                f"断点续传: 区间内已有 {len(existing_set)} 只，仅同步未存在标的"
            )

        df = await _ak_call(
            ak.stock_dzjy_mrmx,
            symbol="A股",
            start_date=start_date_ymd,
            end_date=end_date_ymd,
        )

        count = 0
        n_skip = 0
        if df is not None and not df.empty:
            async for session in get_session():
                if not resume and dates_iso:
                    stmt = text(
                        "DELETE FROM stock_block_trade WHERE trade_date IN :dates"
                    ).bindparams(bindparam("dates", expanding=True))
                    await session.execute(stmt, {"dates": dates_iso})
                for _, row in df.iterrows():
                    symbol = str(row.get("证券代码", ""))
                    if resume and symbol in existing_set:
                        n_skip += 1
                        continue
                    raw = str(row.get("交易日期", ""))
                    trade_date_iso = (
                        raw[:10]
                        if len(raw) >= 10
                        else (raw[:4] + "-" + raw[4:6] + "-" + raw[6:8] if len(raw) >= 8 else raw)
                    )
                    _bv = row.get("买方营业部")
                    _sv = row.get("卖方营业部")
                    bt = StockBlockTrade(
                        symbol=symbol,
                        trade_date=trade_date_iso,
                        price=_safe_float(row, "成交价"),
                        volume=_safe_float(row, "成交量(股)") or _safe_float(row, "成交量"),
                        turnover=_safe_float(row, "成交额(元)") or _safe_float(row, "成交额"),
                        buyer=(str(_bv)[:128] if _bv is not None and str(_bv) != "nan" else None),
                        seller=(str(_sv)[:128] if _sv is not None and str(_sv) != "nan" else None),
                        premium=_safe_float(row, "折溢率"),
                    )
                    session.add(bt)
                    count += 1
                await session.commit()

        if resume:
            await _append_task_log(
                task_id, "INFO",
                f"断点续传: 已有 {len(existing_set)} 只，跳过 {n_skip} 只，待同步 {count} 只"
            )
        await _set_watermark("block_trade", last_td_iso)
        await _append_task_log(task_id, "INFO", f"任务结束: 成功 count={count}")
        await _update_task(task_id, status="success", total_count=count,
                           success_count=count, finished_at=datetime.utcnow())
        return {"success": True, "total": count}

    except Exception as exc:
        logger.error("sync_block_trade failed: %s", exc)
        await _append_task_log(task_id, "ERROR", str(exc)[:500])
        await _update_task(task_id, status="failed", error_detail=str(exc)[:500],
                           finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}


def _latest_dividend_report_date() -> str:
    """返回最近可用的分红报告日期 (XXXX0630 或 XXXX1231)，供 stock_fhps_em 使用。"""
    now = datetime.now()
    y, m = now.year, now.month
    if m >= 7:
        return f"{y}0630"
    return f"{y - 1}1231"


def _previous_dividend_report_date(current: str) -> str:
    """给定当前报告期 current (XXXX0630 或 XXXX1231)，返回上一报告期。"""
    y = int(current[:4])
    if current.endswith("0630"):
        return f"{y - 1}1231"
    return f"{y}0630"


async def sync_dividend(sync_type: str = "full", symbols: List[str] = None, **kwargs) -> Dict[str, Any]:
    """同步分红配股。接口 stock_fhps_em 的 date 必须为 XXXX0630 或 XXXX1231，不能仅传年份。
    全量时拉取当前报告期 + 上一报告期，分别写入。"""
    import akshare as ak
    task_id = await _create_task_or_use("dividend", sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    from src.core.db import get_session
    from src.models.market_sync import StockDividend

    try:
        date_param = _latest_dividend_report_date()
        if sync_type == "incremental":
            wm = await _get_watermark("dividend", "")
            if wm is not None and date_param <= wm:
                return await _return_sync_skipped(
                    task_id, SYNC_SKIPPED_MESSAGE,
                    category="dividend", date_str=date_param
                )
        if sync_type == "full":
            dates_to_fetch = [date_param, _previous_dividend_report_date(date_param)]
        else:
            dates_to_fetch = [date_param]

        def _safe_date_str(val):
            """将可能为 datetime/NaT/None 的单元格转为 YYYY-MM-DD，NaT 不支持 strftime 会报错故用 try。"""
            if val is None:
                return ""
            try:
                return val.strftime("%Y-%m-%d")
            except Exception:
                return str(val)[:10] if val else ""

        count = 0
        for one_date in dates_to_fetch:
            df = await _ak_call(ak.stock_fhps_em, date=one_date)
            if df is None or df.empty:
                continue
            async for session in get_session():
                for _, row in df.iterrows():
                    report_date = _safe_date_str(row.get("最新公告日期") or row.get("预案公告日"))
                    ex_date = _safe_date_str(row.get("除权除息日"))
                    record_date = _safe_date_str(row.get("股权登记日"))
                    dv = StockDividend(
                        symbol=str(row.get("代码", "")),
                        report_date=report_date or f"{one_date[:4]}-{one_date[4:6]}-{one_date[6:8]}",
                        ex_date=ex_date or None,
                        record_date=record_date or None,
                        bonus_ratio=_safe_float(row, "送转股份-送转比例"),
                        convert_ratio=_safe_float(row, "送转股份-转股比例"),
                        dividend_per_share=_safe_float(row, "现金分红-现金分红比例"),
                    )
                    session.add(dv)
                    count += 1
                await session.commit()

        await _set_watermark("dividend", date_param)
        await _append_task_log(task_id, "INFO", f"任务结束: 成功 count={count}")
        await _update_task(task_id, status="success", total_count=count,
                           success_count=count, finished_at=datetime.utcnow())
        return {"success": True, "total": count}

    except Exception as exc:
        logger.error("sync_dividend failed: %s", exc)
        await _append_task_log(task_id, "ERROR", str(exc)[:500])
        await _update_task(task_id, status="failed", error_detail=str(exc)[:500],
                           finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}


def _top_holder_em_code(symbol: str) -> str:
    """6 位代码转东财十大流通股东接口 code：小写 sh/sz + 6 位。"""
    s = (symbol or "").strip()
    if not s or len(s) < 6:
        return s
    if s.upper().startswith("SH"):
        return ("sh" + (s[2:].lstrip() if len(s) > 2 else "")).lower()
    if s.upper().startswith("SZ"):
        return ("sz" + (s[2:].lstrip() if len(s) > 2 else "")).lower()
    if s[0] in "56":
        return ("sh" + s).lower()
    if s[0] in "03":
        return ("sz" + s).lower()
    return ("sz" + s).lower()


def _last_n_quarter_end_dates(n: int = 4) -> List[str]:
    """最近 n 个季末日期，返回 YYYY-MM-DD 列表（与东财接口 date 参数一致）。"""
    now = datetime.now()
    y, m = now.year, now.month
    out = []
    for _ in range(n):
        if m >= 10:
            out.append(f"{y}-09-30")
            m, y = 6, y
        elif m >= 7:
            out.append(f"{y}-06-30")
            m, y = 3, y
        elif m >= 4:
            out.append(f"{y}-03-31")
            m, y = 12, y - 1
        else:
            out.append(f"{y - 1}-12-31")
            m, y = 9, y - 1
    return out


def _fetch_top_holder_em_sync(code: str, date_ymd: str) -> List[dict]:
    """同步请求东财十大流通股东 PageSDLTGD；返回 sdltgd 列表，接口返回键为英文。"""
    import requests
    url = "https://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/PageSDLTGD"
    r = requests.get(url, params={"code": code, "date": date_ymd}, timeout=15)
    j = r.json()
    if not isinstance(j, dict) or "sdltgd" not in j:
        if j.get("message"):
            raise RuntimeError(j.get("message", "接口未返回 sdltgd"))
        return []
    return list(j["sdltgd"]) if j["sdltgd"] else []


async def _get_top_holder_existing_symbols() -> List[str]:
    """查询 stock_top_holders 中已有最近 4 季报告期数据的 symbol，用于续传时跳过。"""
    try:
        from sqlalchemy import select, distinct
        from src.core.db import get_session
        from src.models.market_sync import StockTopHolder
        quarter_dates = _last_n_quarter_end_dates(4)
        async for session in get_session():
            stmt = select(distinct(StockTopHolder.symbol)).where(
                StockTopHolder.report_date.in_(quarter_dates)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.fetchall()]
    except Exception as exc:
        logger.warning("_get_top_holder_existing_symbols failed: %s", exc)
    return []


async def sync_top_holder(sync_type: str = "full", symbols: List[str] = None, resume: bool = False, **kwargs) -> Dict[str, Any]:
    """同步十大流通股东。直连东财 PageSDLTGD，接口返回英文字段，与 stock_top_holders 表一致。
    resume=True 时仅同步库中尚无最近 4 季报告期数据的 symbol。
    """
    task_id = await _create_task_or_use("top_holder", "resume" if resume else sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    if not symbols:
        symbols = await _get_all_stock_codes()
        if not symbols:
            await _append_task_log(task_id, "ERROR", "无股票代码，请先同步 A股列表")
            await _update_task(task_id, status="failed", error_detail="无股票代码", finished_at=datetime.utcnow())
            return {"success": False, "error": "无股票代码, 请先同步 stock_list"}
        if not resume:
            symbols = symbols[:100]  # 十大股东数据量大, 默认只拉前100只（续传时拉全量缺失）

    from sqlalchemy import text
    from src.core.db import get_session
    from src.models.market_sync import StockTopHolder

    quarter_dates = _last_n_quarter_end_dates(4)
    wm = await _get_watermark("top_holder", "") if sync_type == "incremental" else None
    if resume:
        existing = await _get_top_holder_existing_symbols()
        existing_set = set(existing)
        before_count = len(symbols)
        symbols = [s for s in symbols if s not in existing_set]
        skipped = before_count - len(symbols)
        await _append_task_log(
            task_id, "INFO",
            f"断点续传: 已有最近4季数据 {len(existing_set)} 只，跳过 {skipped} 只，待同步 {len(symbols)} 只"
        )
        if not symbols:
            await _update_task(task_id, status="success", total_count=0, success_count=0,
                               error_count=0, finished_at=datetime.utcnow())
            return {"success": True, "total": 0, "ok": 0, "err": 0}
    if sync_type == "incremental" and wm:
        quarter_dates = [d for d in quarter_dates if d > wm]
        if not quarter_dates:
            return await _return_sync_skipped(
                task_id, SYNC_SKIPPED_MESSAGE,
                category="top_holder", date_str=datetime.now().strftime("%Y-%m-%d")
            )

    success_count = 0
    error_count = 0
    new_row_count = 0
    max_report_date = wm or ""

    items: List[tuple[str, str]] = []
    for symbol in symbols:
        em_code = _top_holder_em_code(symbol)
        if not em_code or len(em_code) < 8:
            error_count += 1
            continue
        for date_ymd in quarter_dates:
            items.append((symbol, em_code, date_ymd))

    async def _pull_top_holder_one(symbol: str, em_code: str, date_ymd: str) -> tuple[str, str, Optional[list]]:
        rows = await _ak_call(_fetch_top_holder_em_sync, em_code, date_ymd)
        return (symbol, date_ymd, rows)

    batch_size = _get_sync_batch_size()
    for i in range(0, len(items), batch_size):
        if i % 40 == 0:
            await _raise_if_cancelled(task_id)
        batch = items[i : i + batch_size]
        results = await asyncio.gather(
            *[_pull_top_holder_one(s, em, d) for s, em, d in batch],
            return_exceptions=True,
        )
        for symbol, date_ymd, rows in (r for r in results if not isinstance(r, Exception)):
            if not rows:
                continue
            try:
                report_date = date_ymd
                async for session in get_session():
                    await session.execute(
                        text("DELETE FROM stock_top_holders WHERE symbol = :s AND report_date = :d"),
                        {"s": symbol, "d": report_date},
                    )
                    for rec in rows:
                        rank = rec.get("HOLDER_RANK")
                        if rank is not None and isinstance(rank, (int, float)):
                            rank = int(rank)
                        hold_num = rec.get("HOLD_NUM")
                        if hold_num is not None and isinstance(hold_num, (int, float)):
                            hold_num = float(hold_num)
                        hold_ratio = rec.get("FREE_HOLDNUM_RATIO")
                        if hold_ratio is not None and isinstance(hold_ratio, (int, float)):
                            hold_ratio = float(hold_ratio)
                        change_ratio = rec.get("CHANGE_RATIO")
                        if change_ratio is not None and isinstance(change_ratio, (int, float)):
                            change_ratio = float(change_ratio)
                        else:
                            change_ratio = None
                        th = StockTopHolder(
                            symbol=symbol,
                            report_date=report_date,
                            holder_type=str(rec.get("HOLDER_TYPE") or "top10_free")[:64],
                            rank=rank,
                            holder_name=str(rec.get("HOLDER_NAME") or "")[:256],
                            hold_count=hold_num,
                            hold_ratio=hold_ratio,
                            change_type=str(rec.get("HOLD_NUM_CHANGE") or "")[:20] or None,
                            change_count=None,
                            change_ratio=change_ratio,
                        )
                        session.add(th)
                    await session.commit()
                success_count += 1
                new_row_count += len(rows)
                if report_date > max_report_date:
                    max_report_date = report_date
            except Exception as exc:
                error_count += 1
                await _append_task_log(task_id, "ERROR", f"{symbol} {date_ymd} 写入: {str(exc)[:150]}")
        for j, r in enumerate(results):
            if isinstance(r, Exception):
                error_count += 1
                s, _, d = batch[j]
                await _append_task_log(task_id, "ERROR", f"{s} {d}: {str(r)[:150]}")

    if sync_type == "incremental" and new_row_count == 0:
        return await _return_sync_skipped(
            task_id, SYNC_SKIPPED_MESSAGE,
            category="top_holder", date_str=datetime.now().strftime("%Y-%m-%d")
        )

    if error_count == 0:
        await _set_watermark("top_holder", max_report_date if (sync_type == "incremental" and new_row_count > 0) else datetime.now().strftime("%Y-%m-%d"))
    await _append_task_log(task_id, "INFO", f"任务结束: 成功 {success_count} 失败 {error_count}")
    await _update_task(task_id, status="success", total_count=len(symbols),
                       success_count=success_count, error_count=error_count,
                       finished_at=datetime.utcnow())
    return {"success": True, "total": len(symbols), "ok": success_count, "err": error_count}


def _latest_quarter_end_date() -> str:
    """返回最近可用的季末日期 (XXXX0331/0630/0930/1231)，供 stock_hold_num_cninfo 使用。"""
    now = datetime.now()
    y, m = now.year, now.month
    if m >= 10:
        return f"{y}0930"
    if m >= 7:
        return f"{y}0630"
    if m >= 4:
        return f"{y}0331"
    return f"{y - 1}0930"


def _latest_quarter_end_date_iso() -> str:
    """返回最近季末日期的 YYYY-MM-DD 格式，供 stock_holder_count 表 end_date 查询。"""
    s = _latest_quarter_end_date()
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


async def _get_holder_count_existing_symbols() -> List[str]:
    """查询 stock_holder_count 中已有最近一季报告期数据的 symbol，用于续传时跳过。"""
    try:
        from sqlalchemy import select, distinct
        from src.core.db import get_session
        from src.models.market_sync import StockHolderCount
        latest_iso = _latest_quarter_end_date_iso()
        latest_compact = _latest_quarter_end_date()  # YYYYMMDD，接口可能写入此种格式
        async for session in get_session():
            stmt = select(distinct(StockHolderCount.symbol)).where(
                StockHolderCount.end_date.in_([latest_iso, latest_compact])
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.fetchall()]
    except Exception as exc:
        logger.warning("_get_holder_count_existing_symbols failed: %s", exc)
    return []


async def sync_holder_count(sync_type: str = "full", symbols: List[str] = None, resume: bool = False, **kwargs) -> Dict[str, Any]:
    """同步股东户数。接口 stock_hold_num_cninfo 仅支持季末日期 (0331/0630/0930/1231)。
    resume=True 时仅同步库中尚无最近一季报告期数据的 symbol。
    """
    import akshare as ak
    task_id = await _create_task_or_use("holder_count", "resume" if resume else sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    from src.core.db import get_session
    from src.models.market_sync import StockHolderCount

    try:
        existing_set = set()
        # 必须使用季末日期，否则巨潮接口返回无 records 导致 'records' 异常
        date_param = _latest_quarter_end_date()
        if resume:
            existing = await _get_holder_count_existing_symbols()
            existing_set = set(existing)
            await _append_task_log(
                task_id, "INFO",
                f"断点续传: 已有最近一季数据 {len(existing_set)} 只，待同步库中缺失的 symbol"
            )
        if sync_type == "incremental" and not resume:
            wm = await _get_watermark("holder_count", "")
            if wm is not None and date_param <= wm:
                return await _return_sync_skipped(
                    task_id, SYNC_SKIPPED_MESSAGE,
                    category="holder_count", date_str=date_param
                )
        df = await _ak_call(ak.stock_hold_num_cninfo, date=date_param)

        count = 0
        if df is not None and not df.empty:
            # 接口列名: 证券代码, 证券简称, 变动日期, 本期股东人数, 上期股东人数, 股东人数增幅, 本期人均持股数量, ...
            async for session in get_session():
                for _, row in df.iterrows():
                    symbol = str(row.get("证券代码", "")).strip()
                    if resume and symbol in existing_set:
                        continue
                    end_d = str(row.get("变动日期", ""))[:10]
                    hc = StockHolderCount(
                        symbol=symbol,
                        end_date=end_d,
                        holder_count=_safe_int(row, "本期股东人数"),
                        holder_count_change=_safe_float(row, "股东人数增幅"),
                        avg_hold_amount=_safe_float(row, "本期人均持股数量"),  # 万股，接口无户均持股金额
                    )
                    session.add(hc)
                    count += 1
                await session.commit()

        # 仅当成功完成时更新 watermark（与增量/续传规则一致）
        await _set_watermark("holder_count", date_param)
        await _append_task_log(task_id, "INFO", f"任务结束: 成功 count={count}")
        await _update_task(task_id, status="success", total_count=count,
                           success_count=count, finished_at=datetime.utcnow())
        return {"success": True, "total": count}

    except Exception as exc:
        logger.error("sync_holder_count failed: %s", exc)
        await _append_task_log(task_id, "ERROR", str(exc)[:500])
        await _update_task(task_id, status="failed", error_detail=str(exc)[:500],
                           finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}


async def sync_peer_comparison(sync_type: str = "full", symbols: List[str] = None, resume: bool = False, **kwargs) -> Dict[str, Any]:
    """同步同行比较：成长性/估值/杜邦/规模(东财按 symbol 请求)。resume 时仅同步当日 4 条不全的股票。"""
    import akshare as ak
    import os
    from sqlalchemy import text, select, func
    from src.core.db import get_session
    from src.models.market_sync import StockPeerComparison

    task_id = await _create_task_or_use("peer_comparison", "resume" if resume else sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    if not symbols:
        symbols = await _get_all_stock_codes()
    if not symbols:
        await _append_task_log(task_id, "ERROR", "无股票代码")
        await _update_task(task_id, status="failed", error_detail="无股票代码", finished_at=datetime.utcnow())
        return {"success": False, "error": "无股票代码, 请先同步 stock_list"}

    # 仅上交所与深交所（与 K 线策略一致，东财同行比较不支持北交所等）
    symbols = [
        s for s in symbols
        if (len(s) >= 6 and s[0] in "0356") or (len(s) >= 2 and (s.upper().startswith("SH") or s.upper().startswith("SZ")))
    ]
    # 规格 3.15/2.5：基准日用最近交易日，非自然日；休市日不写入休市日当天为 as_of_date。
    last_trading_date = await get_last_trading_date_str(include_today=True)  # YYYYMMDD
    target_date_iso = f"{last_trading_date[:4]}-{last_trading_date[4:6]}-{last_trading_date[6:8]}"
    today = target_date_iso  # 全量/增量/续传写入时 as_of_date 均用最近交易日

    # 规格 3.15/6.2：增量任务级先判断。若 最近交易日 <= watermark 则直接返回，不请求任何接口。
    if sync_type == "incremental" and not resume:
        wm = await _get_watermark("peer_comparison", "")
        if wm is not None and target_date_iso <= wm:
            today_ymd = datetime.now().strftime("%Y%m%d")
            skip_message = (
                SYNC_SKIPPED_MESSAGE_MARKET_CLOSED
                if today_ymd > last_trading_date
                else SYNC_SKIPPED_MESSAGE_DAILY
            )
            return await _return_sync_skipped(
                task_id, skip_message,
                category="peer_comparison", date_str=target_date_iso
            )

    if resume:
        # 续传以「表中最新 as_of_date」为基准日，避免全量跨天后“当日”与库中写入日不一致导致已有数据被误判为 0
        target_date = today
        try:
            async for session in get_session():
                max_date_stmt = select(func.max(StockPeerComparison.as_of_date)).select_from(StockPeerComparison)
                max_res = await session.execute(max_date_stmt)
                max_date_val = max_res.scalar() or None
                if max_date_val:
                    target_date = max_date_val if isinstance(max_date_val, str) else str(max_date_val)[:10]
                stmt = (
                    select(StockPeerComparison.symbol)
                    .where(StockPeerComparison.as_of_date == target_date)
                    .group_by(StockPeerComparison.symbol)
                    .having(func.count(StockPeerComparison.id) == 4)
                )
                result = await session.execute(stmt)
                complete_symbols = {r[0] for r in result.fetchall()}
                break
        except Exception:
            complete_symbols = set()
        symbols = [s for s in symbols if s not in complete_symbols]
        await _append_task_log(
            task_id, "INFO",
            f"续传: 基准日 {target_date} 已有 4 类数据 {len(complete_symbols)} 只，待同步 {len(symbols)} 只"
        )
        if not symbols:
            await _set_watermark("peer_comparison", target_date)
            await _update_task(task_id, status="success", total_count=0, success_count=0, error_count=0, finished_at=datetime.utcnow())
            return {"success": True, "total": 0, "ok": 0, "err": 0, "task_id": task_id}
        today = target_date  # 续传写入时使用同一基准日，与已有数据一致

    total = len(symbols)
    await _update_task(task_id, total_count=total)
    success_count = 0
    error_count = 0
    failed_symbols: List[str] = []
    _dsn = os.environ.get("MYSQL_DSN", "")

    def _df_to_json(df) -> str:
        if df is None or df.empty:
            return ""
        return json.dumps(df.to_dict(orient="records"), ensure_ascii=False)

    async def _pull_one(sym: str):
        em_symbol = _symbol_to_em(sym)
        rows = []
        for api_fn, sub_type in [
            (ak.stock_zh_growth_comparison_em, "growth"),
            (ak.stock_zh_valuation_comparison_em, "valuation"),
            (ak.stock_zh_dupont_comparison_em, "dupont"),
            (ak.stock_zh_scale_comparison_em, "scale"),
        ]:
            try:
                df = await _ak_call(api_fn, symbol=em_symbol)
                if df is not None and not df.empty:
                    rows.append((sub_type, _df_to_json(df)))
            except Exception as e:
                raise RuntimeError(f"{sub_type}: {e}") from e
        return (sym, rows)

    batch_size = 25
    for i in range(0, total, batch_size):
        await _raise_if_cancelled(task_id)
        batch = symbols[i : i + batch_size]
        results = await asyncio.gather(*[_pull_one(s) for s in batch], return_exceptions=True)
        now = datetime.utcnow()
        for sym, res in zip(batch, results):
            if isinstance(res, Exception):
                error_count += 1
                failed_symbols.append(sym)
                await _append_task_log(task_id, "ERROR", f"{sym}: {str(res)[:200]}")
                continue
            _, rows = res
            success_count += 1
            try:
                async for session in get_session():
                    for sub_type, raw_data in rows:
                        if "sqlite" in _dsn:
                            sql = text(
                                "INSERT INTO stock_peer_comparison (symbol, sub_type, as_of_date, raw_data, updated_at) "
                                "VALUES (:symbol, :sub_type, :as_of_date, :raw_data, :now) "
                                "ON CONFLICT(symbol, sub_type, as_of_date) DO UPDATE SET raw_data=excluded.raw_data, updated_at=excluded.updated_at"
                            )
                        else:
                            sql = text(
                                "INSERT INTO stock_peer_comparison (symbol, sub_type, as_of_date, raw_data, updated_at) "
                                "VALUES (:symbol, :sub_type, :as_of_date, :raw_data, :now) AS new "
                                "ON DUPLICATE KEY UPDATE raw_data=new.raw_data, updated_at=new.updated_at"
                            )
                        await session.execute(sql, {"symbol": sym, "sub_type": sub_type, "as_of_date": today, "raw_data": raw_data, "now": now})
                    await session.commit()
                    break
            except Exception as exc:
                logger.warning("peer_comparison upsert %s failed: %s", sym, exc)
                error_count += 1
                success_count -= 1
                failed_symbols.append(sym)
                await _append_task_log(task_id, "ERROR", f"{sym}: 写入失败 {str(exc)[:150]}")

        await _update_task(task_id, success_count=success_count, error_count=error_count)
        if failed_symbols:
            await _update_task(task_id, failed_symbols=json.dumps(failed_symbols))
        done = min(i + batch_size, total)
        await _append_task_log(task_id, "INFO", f"已处理 {done}/{total} 成功 {success_count} 失败 {error_count}")

    await _set_watermark("peer_comparison", today)
    await _update_task(
        task_id,
        status="success",
        success_count=success_count,
        error_count=error_count,
        failed_symbols=json.dumps(failed_symbols) if failed_symbols else None,
        finished_at=datetime.utcnow(),
    )
    await _append_task_log(task_id, "INFO", f"任务结束: 成功 {success_count} 失败 {error_count}")
    return {"success": True, "total": total, "ok": success_count, "err": error_count, "task_id": task_id}


async def _get_news_existing_symbols() -> List[str]:
    """查询 stock_news 表中已有数据的股票代码，用于续传时跳过。"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import StockNews
        async for session in get_session():
            stmt = select(StockNews.symbol).distinct()
            result = await session.execute(stmt)
            return [r[0] for r in result.fetchall()]
    except Exception:
        return []


async def sync_news(sync_type: str = "full", resume: bool = False, **kwargs) -> Dict[str, Any]:
    """同步个股资讯/公告: stock_news_em → stock_news 表。支持全量/增量/续传。"""
    import akshare as ak
    from sqlalchemy import select
    from src.core.db import get_session
    from src.models.market_sync import StockNews

    task_id = await _create_task_or_use("news", sync_type, kwargs.pop("task_id", None))
    await _append_task_log(task_id, "INFO", "任务开始")
    await _append_proxy_info_task_start(task_id)

    symbols = await _get_all_stock_codes()
    if not symbols:
        await _append_task_log(task_id, "ERROR", "无股票代码")
        await _update_task(task_id, status="failed", error_detail="无股票代码，请先同步 stock_list", finished_at=datetime.utcnow())
        return {"success": False, "error": "无股票代码，请先同步 stock_list"}

    # 续传：仅同步 stock_news 中尚未有数据的股票
    if resume:
        existing = await _get_news_existing_symbols()
        existing_set = set(existing)
        symbols = [s for s in symbols if s not in existing_set]
        if not symbols:
            await _append_task_log(task_id, "INFO", "续传: 所有股票已有数据，无需同步")
            await _update_task(task_id, status="success", total_count=0, success_count=0, error_count=0, finished_at=datetime.utcnow())
            return {"success": True, "total": 0, "ok": 0, "err": 0, "task_id": task_id}

    # 增量：今日已同步则跳过
    wm: Optional[str] = None
    today = datetime.now().strftime("%Y-%m-%d")
    if sync_type == "incremental":
        wm = await _get_watermark("news", "")
        if wm is None:
            wm = await _get_watermark_fallback_from_table("news")
        if wm is not None and wm == today:
            return await _return_sync_skipped(
                task_id, SYNC_SKIPPED_MESSAGE_DAILY,
                category="news", date_str=today
            )

    total = len(symbols)
    await _update_task(task_id, total_count=total)
    success_count = 0
    error_count = 0
    max_publish_date: Optional[str] = None  # 增量时记录最大 publish_time 日期
    first_error_logged = False

    async def _pull_news_one(code: str) -> tuple[str, Optional[list], Optional[str]]:
        df = await _ak_call(ak.stock_news_em, symbol=code)
        if df is None or df.empty:
            return (code, None, None)
        rows = []
        for _, row in df.head(30).iterrows():
            title = str(row.get("新闻标题", ""))[:512]
            content = str(row.get("新闻内容", ""))[:2000] if row.get("新闻内容") else None
            publish_time = str(row.get("发布时间", ""))[:32]
            source = str(row.get("文章来源", ""))[:128]
            url = str(row.get("新闻链接", ""))[:512]
            if not url:
                continue
            if sync_type == "incremental" and wm and publish_time:
                pt_date = publish_time[:10] if len(publish_time) >= 10 else publish_time
                if pt_date <= wm:
                    continue
            rows.append({
                "symbol": code,
                "title": title or "",
                "content": content,
                "publish_time": publish_time or "",
                "source": source,
                "url": url or "",
            })
        if not rows:
            return (code, None, None)
        pt_max = None
        for r in rows:
            pt = r.get("publish_time", "") or ""
            pt_date = pt[:10] if len(pt) >= 10 else pt
            if pt_date and (pt_max is None or pt_date > pt_max):
                pt_max = pt_date
        return (code, rows, pt_max)

    batch_size = _get_sync_batch_size()
    for i in range(0, total, batch_size):
        if i % 50 == 0:
            await _raise_if_cancelled(task_id)
        batch = symbols[i : i + batch_size]
        results = await asyncio.gather(*[_pull_news_one(c) for c in batch], return_exceptions=True)
        for code, rows, pt_max in (r for r in results if not isinstance(r, Exception)):
            if rows is None:
                continue
            if sync_type == "incremental" and pt_max and (max_publish_date is None or pt_max > max_publish_date):
                max_publish_date = pt_max
            try:
                async for session in get_session():
                    for r in rows:
                        stmt = select(StockNews).where(StockNews.symbol == code, StockNews.url == r["url"])
                        result = await session.execute(stmt)
                        existing = result.scalar_one_or_none()
                        if existing:
                            existing.title = r["title"]
                            existing.content = r["content"]
                            existing.publish_time = r["publish_time"]
                            existing.source = r["source"]
                        else:
                            session.add(StockNews(**r))
                    await session.commit()
                    success_count += 1
                    break
            except Exception as exc:
                error_count += 1
                if error_count <= 3:
                    logger.warning("sync_news %s write failed: %s", code, exc)
        for j, r in enumerate(results):
            if isinstance(r, Exception):
                code = batch[j]
                error_count += 1
                if error_count <= 3:
                    logger.warning("sync_news %s failed: %s", code, r)
                if not first_error_logged:
                    first_error_logged = True
                    _e = str(r).lower()
                    if any(x in _e for x in ("could not resolve host", "eastmoney", "connection timed out", "timed out", "curl")):
                        await _append_task_log(task_id, "WARN", "若网络无法访问东方财富，请在资讯/公告卡片中开启「使用代理」后重试")
        if (i + len(batch)) % 50 == 0 or i + len(batch) == total:
            await _update_task(task_id, success_count=success_count, error_count=error_count)
            await _append_task_log(task_id, "INFO", f"进度 {min(i + len(batch), total)}/{total} 成功 {success_count} 失败 {error_count}")

    # watermark: 全量/续传用 today；增量有写入用 max_publish_date，无写入用 today
    if sync_type == "incremental" and max_publish_date:
        await _set_watermark("news", max_publish_date)
    else:
        await _set_watermark("news", today)
    await _append_task_log(task_id, "INFO", f"任务结束: 成功 {success_count} 失败 {error_count}")
    await _update_task(
        task_id,
        status="success",
        success_count=success_count,
        error_count=error_count,
        finished_at=datetime.utcnow(),
    )
    return {"success": True, "total": total, "ok": success_count, "err": error_count, "task_id": task_id}


# ═══════════════════════════════════════════════════════════════
# 统一调度入口
# ═══════════════════════════════════════════════════════════════

_SYNC_HANDLERS = {
    "stock_list": sync_stock_list,
    "kline": sync_kline,
    "financial": sync_financial,
    "trade_calendar": sync_trade_calendar,
    "margin": sync_margin,
    "block_trade": sync_block_trade,
    "capital_flow": sync_capital_flow,
    "top_holder": sync_top_holder,
    "dividend": sync_dividend,
    "sector": sync_sector,
    "lhb": sync_lhb,
    "northbound": sync_northbound,
    "northbound_hold": sync_northbound_hold,
    "limit_updown": sync_limit_updown,
    "holder_count": sync_holder_count,
    "peer_comparison": sync_peer_comparison,
    "news": sync_news,
}


async def kline_retry_failed(task_id: int) -> Dict[str, Any]:
    """仅对指定 K 线任务的 failed_symbols 重试同步，新建一条任务记录；写入幂等。"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTask
        async for session in get_session():
            stmt = select(DataSyncTask).where(DataSyncTask.id == task_id)
            result = await session.execute(stmt)
            task = result.scalar_one_or_none()
            if not task:
                return {"success": False, "error": "任务不存在"}
            if task.category != "kline":
                return {"success": False, "error": "仅支持 K 线任务"}
            if not task.failed_symbols or not task.failed_symbols.strip():
                return {"success": False, "error": "无失败代码列表"}
            symbols = json.loads(task.failed_symbols)
            if not isinstance(symbols, list) or not symbols:
                return {"success": False, "error": "失败代码列表为空"}
            break
        # 新建任务，仅同步失败 symbol
        result = await sync_kline(sync_type="full", symbols=symbols)
        if not result.get("success"):
            return {"success": False, "error": result.get("error", "同步失败")}
        new_task_id = result.get("task_id")
        return {"success": True, "task_id": new_task_id, "message": f"已创建新任务重试 {len(symbols)} 个代码"}
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"failed_symbols 格式错误: {e}"}
    except Exception as exc:
        logger.warning("kline_retry_failed failed: %s", exc)
        return {"success": False, "error": str(exc)[:200]}


async def financial_retry_failed(task_id: int) -> Dict[str, Any]:
    """仅对指定财务指标任务的 failed_symbols 重试同步，新建一条任务记录；写入幂等。"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import DataSyncTask
        async for session in get_session():
            stmt = select(DataSyncTask).where(DataSyncTask.id == task_id)
            result = await session.execute(stmt)
            task = result.scalar_one_or_none()
            if not task:
                return {"success": False, "error": "任务不存在"}
            if task.category != "financial":
                return {"success": False, "error": "仅支持财务指标任务"}
            if not task.failed_symbols or not task.failed_symbols.strip():
                return {"success": False, "error": "无失败代码列表"}
            symbols = json.loads(task.failed_symbols)
            if not isinstance(symbols, list) or not symbols:
                return {"success": False, "error": "失败代码列表为空"}
            break
        result = await sync_financial(sync_type="full", symbols=symbols)
        if not result.get("success"):
            return {"success": False, "error": result.get("error", "同步失败")}
        new_task_id = result.get("task_id")
        return {"success": True, "task_id": new_task_id, "message": f"已创建新任务重试 {len(symbols)} 个代码"}
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"failed_symbols 格式错误: {e}"}
    except Exception as exc:
        logger.warning("financial_retry_failed failed: %s", exc)
        return {"success": False, "error": str(exc)[:200]}


async def run_sync(
    category: str, sync_type: str = "full", tenant_id: Optional[str] = None,
    task_id: Optional[int] = None, **kwargs
) -> Dict[str, Any]:
    """执行单分类同步。K 线支持 sync_type=resume 表示断点续传（仅同步 ClickHouse 中缺失的股票）。
    若 tenant_id 非空且该分类配置了使用代理，则取代理（单代理或按池动态 N 个）并在任务内注入 _ak_call；任务结束或异常时逐条归还。"""
    handler = _SYNC_HANDLERS.get(category)
    if not handler:
        return {"success": False, "error": f"未知分类: {category}"}
    if task_id:
        if await _is_task_cancelled(task_id):
            await _append_task_log(task_id, "INFO", "任务已取消（Worker 领到前已取消）")
            return {"success": False, "cancelled": True, "error": "任务已取消"}
        await _update_task(task_id, status="running")
        await _append_task_log(task_id, "INFO", "Worker 已接收任务，开始执行")
        await _append_sync_global_log("INFO", "任务开始", task_id=task_id, category=category)
    proxy: Optional[str] = None
    token = None
    pool_token = None
    ak_sem_token = None
    dynamic_info_token = None
    proxy_list: List[str] = []
    token_requested = None

    used_pool_with_reserve = False
    kline_sem_em_token = None
    kline_sem_tx_token = None
    kline_concurrent_n_token = None
    aggressive_info_token = None
    if tenant_id:
        from src.services.data_service.proxy_pool_service import (
            get_proxy,
            get_proxy_pool_available_count,
            get_proxies,
            get_proxies_top_pct,
            remove_proxy,
        )
        use_proxy_map = await _load_sync_use_proxy()
        use_proxy_flag = use_proxy_map.get(category, False)
        token_requested = _SYNC_PROXY_REQUESTED.set(use_proxy_flag)
        proxy_domain = SYNC_CATEGORY_PRIMARY_DOMAIN.get(category) if use_proxy_flag else None
        if use_proxy_flag:
            from src.services.data_service.akshare_call_service import _load_sync_ak_retry_config
            _, _, _, replace_after_failures = await _load_sync_ak_retry_config()
            if category == "kline":
                aggressive = await _load_sync_proxy_kline_aggressive()
                if aggressive:
                    active, reserve = await get_proxies_top_pct(tenant_id, pct=0.6, domain=proxy_domain)
                    if len(active) >= 2:
                        pool = SyncProxyPoolWithReserve(
                            active, reserve,
                            tenant_id=tenant_id, domain=proxy_domain or "",
                            replace_after_failures=replace_after_failures,
                        )
                        pool_token = _CURRENT_SYNC_PROXY_POOL.set(pool)
                        used_pool_with_reserve = True
                        count_total = len(active) + len(reserve)
                        aggressive_info_token = _SYNC_PROXY_AGGRESSIVE_KLINE_INFO.set(
                            (len(active), len(reserve), count_total)
                        )
                        proxy_list = active + reserve
                        sem_n = len(active)
                        kline_sem_em_token = _CURRENT_KLINE_SEM_EM.set(asyncio.Semaphore(sem_n))
                        kline_sem_tx_token = _CURRENT_KLINE_SEM_TX.set(asyncio.Semaphore(sem_n))
                        kline_concurrent_n_token = _CURRENT_KLINE_CONCURRENT_N.set(sem_n)
                        logger.info(
                            "run_sync kline aggressive: 在用 N=%s, 备用 M=%s, 池当时可用 K=%s",
                            len(active), len(reserve), count_total,
                        )
            if not proxy_list and category in DYNAMIC_CONCURRENCY_CATEGORIES:
                K = await get_proxy_pool_available_count(tenant_id, domain=proxy_domain)
                max_c, min_p = await _load_sync_proxy_dynamic_config()
                if K >= min_p and K > 0:
                    active, reserve = await get_proxies_top_pct(tenant_id, pct=0.6, domain=proxy_domain)
                    if len(active) >= min_p:
                        active_capped = active[:max_c]
                        reserve_extended = reserve + active[max_c:]
                        pool = SyncProxyPoolWithReserve(
                            active_capped, reserve_extended,
                            tenant_id=tenant_id, domain=proxy_domain or "",
                            replace_after_failures=replace_after_failures,
                        )
                        pool_token = _CURRENT_SYNC_PROXY_POOL.set(pool)
                        used_pool_with_reserve = True
                        ak_sem_token = _CURRENT_SYNC_AK_SEM.set(asyncio.Semaphore(len(active_capped)))
                        aggressive_info_token = _SYNC_PROXY_AGGRESSIVE_KLINE_INFO.set(
                            (len(active_capped), len(reserve_extended), K)
                        )
                        proxy_list = active_capped + reserve_extended
                        logger.info(
                            "run_sync dynamic 60%%: 在用 N=%s, 备用 M=%s, K=%s",
                            len(active_capped), len(reserve_extended), K,
                        )
                if not proxy_list:
                    proxy = await get_proxy(tenant_id, domain=proxy_domain)
                    proxy_list = [proxy] if proxy else []
                    if proxy:
                        token = _CURRENT_SYNC_PROXY.set(proxy)
            elif not proxy_list:
                proxy = await get_proxy(tenant_id, domain=proxy_domain)
                proxy_list = [proxy] if proxy else []
                if proxy:
                    token = _CURRENT_SYNC_PROXY.set(proxy)

    try:
        if task_id is not None:
            kwargs["task_id"] = task_id
        _RESUME_CATEGORIES = frozenset(
            {"kline", "financial", "peer_comparison", "news", "capital_flow", "top_holder", "holder_count", "margin", "block_trade"}
        )
        if sync_type == "resume":
            if category not in _RESUME_CATEGORIES:
                if task_id:
                    await _append_task_log(task_id, "ERROR", f"该分类不支持续传: {category}")
                    await _update_task(task_id, status="failed", error_detail=f"该分类不支持续传: {category}", finished_at=datetime.utcnow())
                return {"success": False, "error": f"该分类不支持续传: {category}"}
        if category == "margin" and sync_type == "resume":
            return await sync_margin(sync_type="full", resume=True, **kwargs)
        if category == "block_trade" and sync_type == "resume":
            return await sync_block_trade(sync_type="full", resume=True, **kwargs)
        if category == "kline" and sync_type == "resume":
            return await sync_kline(sync_type="full", resume=True, **kwargs)
        if category == "financial" and sync_type == "resume":
            return await sync_financial(sync_type="full", resume=True, **kwargs)
        if category == "peer_comparison" and sync_type == "resume":
            return await sync_peer_comparison(sync_type="full", resume=True, **kwargs)
        if category == "news" and sync_type == "resume":
            return await sync_news(sync_type="full", resume=True, **kwargs)
        if category == "capital_flow" and sync_type == "resume":
            return await sync_capital_flow(sync_type="full", resume=True, **kwargs)
        if category == "top_holder" and sync_type == "resume":
            return await sync_top_holder(sync_type="full", resume=True, **kwargs)
        if category == "holder_count" and sync_type == "resume":
            return await sync_holder_count(sync_type="full", resume=True, **kwargs)
        result = await handler(sync_type=sync_type, **kwargs)
        if task_id is not None:
            await _append_sync_global_log("INFO", "任务结束", task_id=task_id, category=category)
        return result
    except TaskCancelledError:
        if task_id:
            await _append_task_log(task_id, "INFO", "任务已取消")
            await _append_sync_global_log("INFO", "任务已取消", task_id=task_id, category=category)
        return {"success": False, "cancelled": True, "error": "任务已取消"}
    except Exception as exc:
        logger.error("run_sync(%s) failed: %s", category, exc)
        if task_id:
            await _append_task_log(task_id, "ERROR", f"run_sync 异常: {str(exc)[:300]}")
            await _append_sync_global_log("ERROR", f"run_sync 异常: {str(exc)[:300]}", task_id=task_id, category=category)
            await _update_task(task_id, status="failed", error_detail=str(exc)[:500], finished_at=datetime.utcnow())
        return {"success": False, "error": str(exc)[:200]}
    finally:
        if token_requested is not None:
            _SYNC_PROXY_REQUESTED.reset(token_requested)
        if dynamic_info_token is not None:
            _SYNC_PROXY_DYNAMIC_INFO.reset(dynamic_info_token)
        if aggressive_info_token is not None:
            _SYNC_PROXY_AGGRESSIVE_KLINE_INFO.reset(aggressive_info_token)
        if kline_sem_em_token is not None:
            _CURRENT_KLINE_SEM_EM.reset(kline_sem_em_token)
        if kline_sem_tx_token is not None:
            _CURRENT_KLINE_SEM_TX.reset(kline_sem_tx_token)
        if kline_concurrent_n_token is not None:
            _CURRENT_KLINE_CONCURRENT_N.reset(kline_concurrent_n_token)
        if pool_token is not None:
            _CURRENT_SYNC_PROXY_POOL.reset(pool_token)
        if ak_sem_token is not None:
            _CURRENT_SYNC_AK_SEM.reset(ak_sem_token)
        if token is not None:
            _CURRENT_SYNC_PROXY.reset(token)
        if proxy_list and tenant_id and not used_pool_with_reserve:
            for p in proxy_list:
                try:
                    from src.services.data_service.proxy_pool_service import remove_proxy
                    await remove_proxy(tenant_id, p)
                except Exception as e:
                    logger.warning("run_sync finally remove_proxy failed for %s: %s", _mask_proxy(p), e)


async def run_sync_all(sync_type: str = "full", tenant_id: Optional[str] = None) -> Dict[str, Any]:
    """执行全部分类同步 (按依赖顺序)。tenant_id 会传给每次 run_sync 用于按分类使用代理。"""
    # 1. stock_list 必须先跑 (后续分类依赖股票代码)
    results = {}
    results["stock_list"] = await run_sync("stock_list", sync_type, tenant_id=tenant_id)

    # 2. 不依赖个股的分类可以并发
    independent = ["trade_calendar", "sector", "lhb", "northbound", "northbound_hold", "limit_updown", "margin", "block_trade", "dividend", "holder_count"]
    tasks = [run_sync(cat, sync_type, tenant_id=tenant_id) for cat in independent]
    ind_results = await asyncio.gather(*tasks, return_exceptions=True)
    for cat, res in zip(independent, ind_results):
        results[cat] = res if not isinstance(res, Exception) else {"success": False, "error": str(res)}

    # 3. 依赖个股代码的分类 (耗时长, 串行)
    for cat in ["kline", "financial", "capital_flow", "top_holder", "peer_comparison"]:
        results[cat] = await run_sync(cat, sync_type, tenant_id=tenant_id)

    return results


async def get_sync_status() -> Dict[str, Any]:
    """获取所有分类的同步状态 (含频率配置、是否使用代理、定时增量开关)"""
    custom_intervals = await _load_custom_intervals()
    use_proxy_map = await _load_sync_use_proxy()
    schedule_enabled = await _load_sync_schedule_enabled()

    status = {}
    for cat in SYNC_CATEGORIES:
        cat_id = cat["id"]
        wm = await _get_watermark(cat_id)
        if wm is None:
            wm = await _get_watermark_fallback_from_table(cat_id)
        default_interval = cat.get("default_interval", 86400)
        interval = custom_intervals.get(cat_id, default_interval)
        status[cat_id] = {
            **cat,
            "last_sync_date": wm,
            "synced": wm is not None,
            "default_interval": default_interval,
            "interval": interval,
            "use_proxy": use_proxy_map.get(cat_id, False),
        }
    status["schedule_enabled"] = schedule_enabled
    status["worker_concurrency"] = await _load_sync_worker_concurrency()
    try:
        from src.services.data_service.akshare_call_service import _load_sync_ak_retry_config
        retry_count, backoff_base, timeout_seconds, replace_after = await _load_sync_ak_retry_config()
        status["sync_ak_retry_count"] = retry_count
        status["sync_ak_timeout_seconds"] = timeout_seconds
        status["sync_proxy_replace_after_failures"] = replace_after
    except Exception:
        status["sync_ak_retry_count"] = 3
        status["sync_ak_timeout_seconds"] = 60
        status["sync_proxy_replace_after_failures"] = 1
    try:
        status["log_entries"] = await get_sync_log_entries(100)
    except Exception:
        status["log_entries"] = []
    return status


async def _load_sync_worker_concurrency() -> int:
    """Worker 最大并发数。配置中心 sync_worker_concurrency，默认 3，限制 1~8。"""
    try:
        from src.services.config_center_service import get_config
        result = await get_config("public", "default", "sync_worker_concurrency")
        if result and result.get("value") is not None:
            val = result.get("value", result)
            if isinstance(val, (int, float)):
                n = int(val)
                return max(1, min(8, n))
            if isinstance(val, str):
                n = int(val.strip()) if val.strip() else 3
                return max(1, min(8, n))
    except Exception as exc:
        logger.debug("Load sync_worker_concurrency failed: %s", exc)
    return 3


async def set_sync_worker_concurrency(value: int) -> bool:
    """保存 Worker 最大并发数到配置中心。value 须在 1~8。"""
    if not (1 <= value <= 8):
        return False
    try:
        from src.services.config_center_service import set_config
        await set_config("public", "default", "sync_worker_concurrency",
                         json.dumps(value), description="数据拉取 Worker 最大并发数")
        return True
    except Exception as exc:
        logger.warning("Save sync_worker_concurrency failed: %s", exc)
        return False


async def _load_custom_intervals() -> Dict[str, int]:
    """从配置中心加载用户自定义同步频率"""
    try:
        from src.services.config_center_service import get_config
        result = await get_config("public", "default", "sync_intervals")
        if result:
            val = result.get("value", result) if isinstance(result, dict) else result
            if isinstance(val, str):
                return json.loads(val)
            if isinstance(val, dict):
                return {k: int(v) for k, v in val.items()}
    except Exception as exc:
        logger.debug("Load custom sync intervals failed: %s", exc)
    return {}


async def set_sync_intervals(intervals: Dict[str, int]) -> bool:
    """保存用户自定义同步频率到配置中心"""
    try:
        from src.services.config_center_service import set_config
        await set_config("public", "default", "sync_intervals",
                         json.dumps(intervals, ensure_ascii=False),
                         description="数据同步自定义频率 (秒)")
        return True
    except Exception as exc:
        logger.warning("Save sync intervals failed: %s", exc)
        return False


async def _load_sync_schedule_enabled() -> bool:
    """是否启用定时增量。默认 True；配置 sync_schedule_enabled 为 false 时关闭。"""
    try:
        from src.services.config_center_service import get_config
        result = await get_config("public", "default", "sync_schedule_enabled")
        if result and result.get("value") is not None:
            val = result.get("value", result)
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                return val.strip().lower() not in ("false", "0", "no", "off")
    except Exception:
        pass
    return True


async def set_sync_schedule_enabled(enabled: bool) -> bool:
    """保存定时增量开关到配置中心"""
    try:
        from src.services.config_center_service import set_config
        await set_config("public", "default", "sync_schedule_enabled",
                         json.dumps(enabled), description="数据拉取定时增量是否启用")
        return True
    except Exception as exc:
        logger.warning("Save sync_schedule_enabled failed: %s", exc)
        return False


async def run_sync_incremental_schedule() -> None:
    """定时增量检查：按 sync_intervals 与上次成功同步时间判断是否到期，到期且该分类无 running 任务时触发 run_sync(category, incremental)。"""
    if not await _load_sync_schedule_enabled():
        return
    custom_intervals = await _load_custom_intervals()
    # 遍历顺序：trade_calendar 优先，其余按 SYNC_CATEGORIES
    order = [c for c in SYNC_CATEGORIES if c["id"] == "trade_calendar"]
    order += [c for c in SYNC_CATEGORIES if c["id"] != "trade_calendar"]
    now = datetime.now(timezone.utc)
    triggered: List[str] = []
    for cat in order:
        cat_id = cat["id"]
        try:
            default_interval = cat.get("default_interval", 86400)
            interval_seconds = custom_intervals.get(cat_id, default_interval)
            if interval_seconds <= 0:
                continue
            last_at = await _get_last_success_sync_at(cat_id)
            if last_at is not None:
                # 使 last_at 可与时区 aware 的 now 做差（DB 可能返回 naive）
                if last_at.tzinfo is None:
                    last_at = last_at.replace(tzinfo=timezone.utc)
                if (now - last_at).total_seconds() < interval_seconds:
                    continue
            if await _has_running_task(cat_id):
                continue
            task_id = await _create_task(cat_id, "incremental")
            if task_id:
                payload = {"task_id": task_id, "category": cat_id, "sync_type": "incremental", "tenant_id": None}
                if await enqueue_sync_task(payload):
                    triggered.append(cat_id)
                    await _append_sync_global_log("INFO", f"定时增量触发: {cat_id}")
                else:
                    await _update_task(task_id, status="failed", error_detail="入队失败")
        except Exception as exc:
            logger.warning("run_sync_incremental_schedule category=%s: %s", cat_id, exc)
    if triggered:
        logger.info("run_sync_incremental_schedule triggered: %s", triggered)


async def _load_sync_proxy_dynamic_config() -> tuple[int, int]:
    """(max_concurrency, min_pool_for_dynamic)。配置中心 sync_proxy_dynamic 优先，否则环境变量。"""
    try:
        from src.services.config_center_service import get_config
        result = await get_config("public", "default", "sync_proxy_dynamic")
        if result and result.get("value"):
            val = result["value"]
            if isinstance(val, str):
                val = json.loads(val) if val.strip() else {}
            if isinstance(val, dict):
                max_c = val.get("max_concurrency")
                min_p = val.get("min_pool_for_dynamic")
                if max_c is not None and min_p is not None:
                    return (int(max_c), int(min_p))
    except Exception as exc:
        logger.debug("Load sync_proxy_dynamic failed: %s", exc)
    max_c = int(os.environ.get("SYNC_PROXY_MAX_CONCURRENCY", "50"))
    min_p = int(os.environ.get("SYNC_PROXY_MIN_POOL_FOR_DYNAMIC", "2"))
    return (max(max_c, 1), max(min_p, 0))


async def _load_sync_proxy_kline_aggressive() -> bool:
    """是否对 K 线启用「前 60% 按延迟 + 双源 2N 并发 + 连续 3 次失败从备用池替换」。配置中心 sync_proxy_kline_aggressive 或环境变量。"""
    try:
        from src.services.config_center_service import get_config
        result = await get_config("public", "default", "sync_proxy_kline_aggressive")
        if result and result.get("value") is not None:
            val = result["value"]
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                return val.strip().lower() in ("true", "1", "yes")
    except Exception as exc:
        logger.debug("Load sync_proxy_kline_aggressive failed: %s", exc)
    return os.environ.get("SYNC_PROXY_KLINE_AGGRESSIVE", "").strip().lower() in ("true", "1", "yes")


async def _load_sync_use_proxy() -> Dict[str, bool]:
    """从配置中心加载各分类是否使用代理。key=category id, value=bool。"""
    try:
        from src.services.config_center_service import get_config
        result = await get_config("public", "default", "sync_use_proxy")
        if result:
            val = result.get("value", result) if isinstance(result, dict) else result
            if isinstance(val, str):
                data = json.loads(val)
                return {k: bool(v) for k, v in (data if isinstance(data, dict) else {}).items()}
            if isinstance(val, dict):
                return {k: bool(v) for k, v in val.items()}
    except Exception as exc:
        logger.debug("Load sync_use_proxy failed: %s", exc)
    return {}


async def set_sync_use_proxy(use_proxy_map: Dict[str, bool]) -> bool:
    """保存各分类「使用代理」开关到配置中心。"""
    try:
        from src.services.config_center_service import set_config
        await set_config("public", "default", "sync_use_proxy",
                         json.dumps(use_proxy_map, ensure_ascii=False),
                         description="数据拉取各分类是否使用代理")
        return True
    except Exception as exc:
        logger.warning("Save sync_use_proxy failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _safe_float(row, col: str) -> Optional[float]:
    """安全提取 float"""
    try:
        v = row.get(col)
        if v is None or str(v).strip() in ("", "nan", "NaN", "--", "-"):
            return None
        return float(v)
    except (ValueError, TypeError):
        return None


def _safe_int(row, col: str) -> Optional[int]:
    """安全提取 int"""
    try:
        v = row.get(col)
        if v is None or str(v).strip() in ("", "nan", "NaN", "--", "-"):
            return None
        return int(float(v))
    except (ValueError, TypeError):
        return None
