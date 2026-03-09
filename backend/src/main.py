import os
from pathlib import Path
from dotenv import load_dotenv

# 自动加载 backend/.env 配置
# 在 pytest 下不覆盖 MYSQL_DSN，避免测试连到本地库并执行 reset_auth_state 清空 users 表
_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    _in_pytest = "PYTEST_CURRENT_TEST" in os.environ
    load_dotenv(_env_file, override=not _in_pytest)

# 进程时区设为北京时间，保证日志与 datetime.now() 为北京时
os.environ["TZ"] = "Asia/Shanghai"
try:
    import time
    time.tzset()
except AttributeError:
    pass  # Windows 无 tzset，依赖系统时区或 TZ 由启动环境设置

import asyncio
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from src.core.db import get_engine
from src.core.metrics import setup_metrics
from src.core.middleware import TenantContextMiddleware

logger = logging.getLogger(__name__)

# 导入所有模型以确保 create_all 建表
import src.models  # noqa: F401
from src.models.base import Base

from src.routers.account import router as account_router
from src.routers.admin import router as admin_router
from src.routers.ai_analysis import router as ai_analysis_router
from src.routers.ai_config import router as ai_config_router
from src.routers.ai_config_audit import router as ai_config_audit_router
from src.routers.ai_config_backup import router as ai_config_backup_router
from src.routers.alert import router as alert_router
from src.routers.auth import router as auth_router
from src.routers.backtest import router as backtest_router
from src.routers.backup import router as backup_router
from src.routers.community import router as community_router
from src.routers.config_center import router as config_center_router
from src.routers.consent import router as consent_router
from src.routers.dashboard import router as dashboard_router
from src.routers.data import router as data_router
from src.routers.data_rights import router as data_rights_router
from src.routers.export_audit import router as export_audit_router
from src.routers.export_report import router as export_report_router
from src.routers.factor import router as factor_router
from src.routers.import_knowledge import router as import_knowledge_router
from src.routers.import_strategy import router as import_strategy_router
from src.routers.logs import router as logs_router
from src.routers.market import router as market_router
from src.routers.message import router as message_router
from src.routers.object_storage import router as object_storage_router
from src.routers.ops import router as ops_router
from src.routers.pricing import router as pricing_router
from src.routers.replay import router as replay_router
from src.routers.social import router as social_router
from src.routers.strategy import router as strategy_router
from src.routers.strategy_templates import router as strategy_templates_router
from src.routers.system import router as system_router
from src.routers.trade import router as trade_router
from src.routers.user_factor import router as user_factor_router
from src.routers.data_sync import router as data_sync_router
from src.routers.ws import router as ws_router

DESCRIPTION = """
# 散户低频策略交易平台 API

面向散户的量化策略交易一站式平台，提供策略管理、回测、模拟/实盘交易、AI 辅助分析、社区互动等功能。

## 快速开始

1. 调用 `POST /auth/register` 注册账户
2. 调用 `POST /auth/login` 获取 JWT Token
3. 在右上角 **Authorize** 按钮中填入 `Bearer <token>`
4. 即可调用所有需要认证的 API

## 认证方式

所有需要登录的接口在 Header 中添加：
```
Authorization: Bearer eyJhbG...
```

---
"""

TAG_METADATA = [
    {"name": "🔐 认证", "description": "用户注册、登录、个人信息"},
    {"name": "📊 策略", "description": "策略 CRUD、回测管理"},
    {"name": "💹 交易", "description": "同步/异步下单、持仓查询、交易模式"},
    {"name": "📈 行情", "description": "实时行情、热门排行、深度数据、行情告警"},
    {"name": "📉 数据", "description": "市场历史数据、仪表盘概览"},
    {"name": "🏘️ 社区", "description": "发帖、互动、排行榜、社区消息"},
    {"name": "👥 社交", "description": "关注/取消关注、粉丝列表、关系查询"},
    {"name": "✉️ 私信", "description": "一对一私信、消息标记已读"},
    {"name": "🚨 告警", "description": "告警创建、列表、解决"},
    {"name": "💾 备份", "description": "全量/增量备份、恢复"},
    {"name": "🤖 AI 配置", "description": "AI 模型参数配置、审计日志、备份恢复"},
    {"name": "⚙️ 配置中心", "description": "集中配置管理（命名空间/键值/版本）"},
    {"name": "🔧 运维", "description": "系统健康、运维状态、审计日志、Prometheus 指标"},
    {"name": "🔒 合规", "description": "用户授权管理、数据权利请求"},
    {"name": "💰 定价", "description": "套餐方案、用户权益查询"},
    {"name": "📐 因子", "description": "公共因子库、用户自定义因子"},
    {"name": "📦 导入导出", "description": "策略/知识导入（文件上传）、导出审计"},
    {"name": "🎬 回放", "description": "交易回放报告、分析、导出"},
    {"name": "🗄️ 对象存储", "description": "文件上传/下载/删除"},
    {"name": "👑 管理员", "description": "租户管理、权限授予"},
]

app = FastAPI(
    title="散户低频策略交易平台 API",
    description=DESCRIPTION,
    version="1.0.0",
    openapi_tags=TAG_METADATA,
    docs_url="/docs",
    redoc_url="/redoc",
    swagger_ui_parameters={
        "docExpansion": "list",
        "defaultModelsExpandDepth": 0,
        "filter": True,
        "tryItOutEnabled": True,
        "persistAuthorization": True,
        "displayRequestDuration": True,
    },
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup() -> None:
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # 后台预热行情缓存
    asyncio.create_task(_warmup_market_cache())
    # 代理池定时流水线（刷新状态 → 测试 → 删除无效）
    asyncio.create_task(_proxy_pool_schedule_loop())
    # 数据拉取定时增量：每 60 秒按 sync_intervals 与上次成功同步时间触发增量
    asyncio.create_task(_data_sync_incremental_schedule_loop())
    # 备份策略：定时全量 + 超期清理（按 GET/PUT backup-policy 配置）
    asyncio.create_task(_backup_schedule_loop())
    # 行情定时预热：交易日 9:14–9:16 执行 hot/sectors/ranking
    asyncio.create_task(_market_cache_schedule_warmup())


async def _warmup_market_cache() -> None:
    """后台预热: ClickHouse 建表 + 股票列表 + 大盘指数 (不阻塞启动)。"""
    # ClickHouse K线表 (静默降级 — 不可用时 fallback MySQL)
    try:
        from src.services.data_service.kline_storage import ensure_kline_table
        ok = await ensure_kline_table()
        logger.info("Market warmup: ClickHouse kline table %s", "ready" if ok else "unavailable (MySQL fallback)")
    except Exception as e:
        logger.warning("Market warmup ClickHouse failed: %s", e)

    from src.services.data_service.hot_rank_service import (
        get_stock_list,
        get_indices,
        get_hot_rank,
        get_sectors,
        get_ranking,
    )
    try:
        await get_stock_list()  # 加载全量A股列表 -> 内存+Redis+MySQL
        logger.info("Market warmup: stock list loaded")
    except Exception as e:
        logger.warning("Market warmup stock list failed: %s", e)
    try:
        await get_indices()  # 加载大盘指数 -> 内存+Redis
        logger.info("Market warmup: indices loaded")
    except Exception as e:
        logger.warning("Market warmup indices failed: %s", e)
    try:
        await get_hot_rank()
        logger.info("Market warmup: hot rank loaded")
    except Exception as e:
        logger.warning("Market warmup hot rank failed: %s", e)
    try:
        await get_sectors("all")
        logger.info("Market warmup: sectors loaded")
    except Exception as e:
        logger.warning("Market warmup sectors failed: %s", e)
    try:
        await get_ranking("change_pct", "desc", 30)
        logger.info("Market warmup: ranking loaded")
    except Exception as e:
        logger.warning("Market warmup ranking failed: %s", e)


async def _market_cache_schedule_warmup() -> None:
    """每 60 秒检查：交易日 9:05–15:30 内且距上次定时运行≥间隔时，执行 hot/sectors/ranking 预热。"""
    import time
    from datetime import datetime

    _SCHEDULE_LAST_RUN_TTL = 86400 * 7  # 7 天，仅用于 schedule_last_run_ts 键

    while True:
        try:
            await asyncio.sleep(60)
            now = datetime.now()
            # 时间窗口：9:05–15:30（闭区间），开市=集合竞价 9:15
            minute_of_day = now.hour * 60 + now.minute
            if minute_of_day < 9 * 60 + 5 or minute_of_day > 15 * 60 + 30:
                continue
            try:
                from src.services.config_center_service import list_tenant_ids_for_key, get_config
                tenant_ids = await list_tenant_ids_for_key("default", "market_warmup_schedule_enabled")
                if not tenant_ids:
                    continue
                cfg_enabled = await get_config(tenant_ids[0], "default", "market_warmup_schedule_enabled")
                if cfg_enabled and str(cfg_enabled.get("value", "")).lower() in ("false", "0"):
                    continue
                interval_raw = 600
                try:
                    cfg_interval = await get_config(tenant_ids[0], "default", "market_warmup_interval_seconds")
                    if cfg_interval is not None and cfg_interval.get("value") is not None:
                        interval_raw = int(cfg_interval.get("value"))
                except (TypeError, ValueError):
                    pass
                interval_seconds = max(300, min(3600, interval_raw))
            except Exception:
                continue
            try:
                from src.services.data_service.data_sync_service import get_last_trading_date_str
                today_ymd = now.strftime("%Y%m%d")
                last_td = await get_last_trading_date_str(include_today=True)
                if last_td != today_ymd:
                    continue
            except Exception:
                continue
            try:
                from src.services.cache_policy_service import get_cached, set_cached
                last_run_s = await get_cached("market:warmup:schedule_last_run_ts")
                last_run_ts = int(last_run_s) if last_run_s else 0
                now_ts = int(time.time())
                if now_ts - last_run_ts < interval_seconds:
                    continue
            except Exception:
                continue
            try:
                from src.services.market_warmup_service import check_warmup_running, run_market_warmup, set_warmup_running
                if await check_warmup_running():
                    continue
                allowed = {"indices", "hot", "sectors", "ranking", "minute_top20"}
                cfg_items = await get_config(tenant_ids[0], "default", "market_warmup_schedule_items")
                value = cfg_items.get("value") if cfg_items else None
                if isinstance(value, list) and len(value) > 0:
                    items = [x for x in value if x in allowed]
                    if not items:
                        items = ["indices", "hot", "sectors", "ranking"]
                else:
                    items = ["indices", "hot", "sectors", "ranking"]
                await set_warmup_running(True)
                await run_market_warmup(items, tenant_id=tenant_ids[0], trigger="schedule")
                logger.info("market_cache schedule warmup: %s done", ",".join(items))
                await set_cached(
                    "market:warmup:schedule_last_run_ts",
                    str(int(time.time())),
                    ttl=_SCHEDULE_LAST_RUN_TTL,
                )
            except Exception as e:
                logger.warning("market_cache schedule warmup failed: %s", e)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("market_cache schedule warmup loop: %s", e)


async def _proxy_pool_schedule_loop() -> None:
    """每 60 秒检查：第一条线定时更新（拉取并入池），第二条线快速更新代理池（全池并发复测并即时剔除无效）。"""
    import time
    while True:
        try:
            await asyncio.sleep(60)
            from src.services.config_center_service import list_tenant_ids_for_key
            from src.services.data_service.proxy_pool_service import (
                _refresh_running,
                _update_schedule_last_run,
                append_schedule_log_entries,
                get_proxy_pool_config,
                run_fast_pool_update,
                run_refresh_pipeline,
            )
            from src.utils.log_entries import log_entry
            tenant_ids = await list_tenant_ids_for_key("default", "proxy_pool")
            now_ts = int(time.time())
            for tid in tenant_ids:
                try:
                    cfg = await get_proxy_pool_config(tid)
                    # 第一条线：定时更新（仅拉取→校验→入池）
                    has_source = bool((cfg.get("url") or "").strip())
                    if not has_source:
                        fu = cfg.get("proxy_file_urls")
                        if isinstance(fu, list):
                            has_source = any((u or "").strip() for u in fu)
                        else:
                            has_source = bool(str(fu or "").strip())
                    if cfg.get("schedule_enabled") and has_source:
                        interval = max(60, int(cfg.get("schedule_interval_seconds") or 3600))
                        last_run = int(cfg.get("schedule_last_run_at") or 0)
                        if now_ts - last_run >= interval:
                            _refresh_running.add(tid)
                            try:
                                result = await run_refresh_pipeline(tid)
                                if not result.get("skipped"):
                                    await _update_schedule_last_run(tid, now_ts)
                                    logger.info("proxy_pool 定时更新 tenant=%s added=%s", tid, result.get("added"))
                                    try:
                                        entries = result.get("log_entries")
                                        if entries:
                                            await append_schedule_log_entries(tid, entries)
                                        else:
                                            await append_schedule_log_entries(
                                                tid,
                                                [log_entry("INFO", f"[schedule] 定时更新 done added={result.get('added')}")],
                                            )
                                    except Exception as ex:
                                        logger.warning("proxy_pool append_schedule_log_entries after refresh: %s", ex)
                            finally:
                                _refresh_running.discard(tid)
                    # 第二条线：快速更新代理池（全池并发复测，即时剔除无效）
                    if cfg.get("fast_schedule_enabled"):
                        fast_interval = max(60, int(cfg.get("fast_schedule_interval_seconds") or 600))
                        fast_last = int(cfg.get("fast_schedule_last_run_at") or 0)
                        if now_ts - fast_last >= fast_interval and tid not in _refresh_running:
                            result = await run_fast_pool_update(tid)
                            if not result.get("skipped"):
                                logger.info("proxy_pool 快速更新 tenant=%s tested=%s deleted=%s", tid, result.get("tested"), result.get("deleted"))
                                try:
                                    entries = result.get("log_entries")
                                    if entries:
                                        await append_schedule_log_entries(tid, entries)
                                    else:
                                        await append_schedule_log_entries(
                                            tid,
                                            [
                                                log_entry(
                                                    "INFO",
                                                    f"[fast] 快速更新 done tested={result.get('tested')} deleted={result.get('deleted')}",
                                                )
                                            ],
                                        )
                                except Exception as ex:
                                    logger.warning("proxy_pool append_schedule_log_entries after fast: %s", ex)
                except Exception as e:
                    logger.warning("proxy_pool schedule loop tenant=%s failed: %s", tid, e)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("proxy_pool schedule loop: %s", e)


async def _data_sync_incremental_schedule_loop() -> None:
    """每 60 秒检查各分类是否到期，到期则触发增量同步。"""
    while True:
        try:
            await asyncio.sleep(60)
            from src.services.data_service.data_sync_service import run_sync_incremental_schedule
            await run_sync_incremental_schedule()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("data_sync incremental schedule loop: %s", e)


async def _backup_schedule_loop() -> None:
    """每 10 分钟：按 backup-policy 执行定时全量/增量备份 + 超期清理。"""
    import time
    from datetime import datetime
    from src.services.backup_policy_service import (
        BACKUP_POLICY_KEY,
        BACKUP_POLICY_NAMESPACE,
        get_backup_policy_api,
        get_last_scheduled_run,
        set_last_scheduled_run,
        get_last_incremental_run,
        set_last_incremental_run,
    )
    from src.services.backup_service import create_backup_task, cleanup_expired_backups
    from src.services.config_center_service import list_tenant_ids_for_key
    from src.core.db import get_session

    while True:
        try:
            await asyncio.sleep(600)  # 10 分钟
            tenant_ids = await list_tenant_ids_for_key(BACKUP_POLICY_NAMESPACE, BACKUP_POLICY_KEY)
            if not tenant_ids:
                continue
            now_ts = time.time()
            full_interval_seconds_default = 86400  # 1 天
            async for session in get_session():
                for tid in tenant_ids:
                    try:
                        policy = await get_backup_policy_api(tid)
                        if not policy.get("enabled", True):
                            continue
                        full_interval_days = max(1, policy.get("full_interval_days", 1))
                        full_interval_seconds = full_interval_days * 86400
                        last_full_run = await get_last_scheduled_run(tid)
                        due_full = last_full_run is None or (now_ts - last_full_run) >= full_interval_seconds
                        if due_full:
                            name = "定时全量-" + datetime.utcnow().strftime("%Y%m%d-%H%M")
                            await create_backup_task(
                                session, tid, name, type="full",
                                content=["mysql", "ai_config", "system_config"],
                                destination="local",
                            )
                            await set_last_scheduled_run(tid, now_ts)
                            logger.info("backup schedule: triggered full backup tenant=%s name=%s", tid, name)
                        else:
                            incremental_enabled = policy.get("incremental_enabled", False)
                            if incremental_enabled:
                                last_incr_run = await get_last_incremental_run(tid)
                                if last_incr_run is None or (now_ts - last_incr_run) >= full_interval_seconds_default:
                                    name = "定时增量-" + datetime.utcnow().strftime("%Y%m%d-%H%M")
                                    await create_backup_task(
                                        session, tid, name, type="incremental",
                                        content=["mysql", "ai_config", "system_config"],
                                        destination="local",
                                    )
                                    await set_last_incremental_run(tid, now_ts)
                                    logger.info("backup schedule: triggered incremental backup tenant=%s name=%s", tid, name)
                    except Exception as e:
                        logger.warning("backup schedule tenant=%s failed: %s", tid, e)
                break
            # 超期清理
            async for session in get_session():
                for tid in tenant_ids:
                    try:
                        policy = await get_backup_policy_api(tid)
                        retention_days = max(1, policy.get("retention_days", 90))
                        deleted = await cleanup_expired_backups(session, tid, retention_days)
                        if deleted:
                            logger.info("backup cleanup tenant=%s deleted=%s", tid, deleted)
                    except Exception as e:
                        logger.warning("backup cleanup tenant=%s failed: %s", tid, e)
                break
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("backup schedule loop: %s", e)


app.add_middleware(TenantContextMiddleware)
setup_metrics(app)

# --- 核心路由 ---
app.include_router(system_router)
app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(strategy_router)
app.include_router(strategy_templates_router)
app.include_router(backtest_router)
app.include_router(trade_router)
app.include_router(account_router)
app.include_router(ws_router)

# --- 社区 ---
app.include_router(community_router)
app.include_router(social_router)
app.include_router(message_router)

# --- 行情/数据 ---
app.include_router(market_router)
app.include_router(data_router)
app.include_router(data_sync_router)
app.include_router(replay_router)
app.include_router(factor_router)
app.include_router(user_factor_router)

# --- 运维/管理 ---
app.include_router(alert_router)
app.include_router(ops_router)
app.include_router(logs_router)
app.include_router(backup_router)
app.include_router(admin_router)
app.include_router(object_storage_router)
app.include_router(config_center_router)

# --- AI 分析 / AI 配置 (audit/backup 路由必须在 ai_config 之前以避免 /{key} 遮蔽) ---
# 同时挂载 /api 前缀，兼容前端 API 基址为 http://host:8000/api 时的请求
app.include_router(ai_analysis_router)
app.include_router(ai_analysis_router, prefix="/api")
app.include_router(ai_config_audit_router)
app.include_router(ai_config_backup_router)
app.include_router(ai_config_router)

# --- 合规/权限 ---
app.include_router(consent_router)
app.include_router(data_rights_router)
app.include_router(pricing_router)

# --- 导入/导出 ---
app.include_router(import_strategy_router)
app.include_router(import_knowledge_router)
app.include_router(export_report_router)
app.include_router(export_audit_router)

# ──────────────────────────────────────────────────
# Swagger 中文文档 & 测试样例 (自动 patch 到路由上)
# ──────────────────────────────────────────────────
_ROUTE_DOC: dict = {
    # 认证
    "POST /auth/register": {"tags": ["🔐 认证"], "summary": "注册新用户",
        "description": "使用手机号+密码注册。成功后返回用户信息。",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"phone": "13800138000", "password": "MyPass@123", "tenant_id": "t1"}}}}}},
    "POST /auth/login": {"tags": ["🔐 认证"], "summary": "用户登录",
        "description": "使用手机号+密码登录，返回 JWT Token。请将 Token 填入右上角 **Authorize** → `Bearer <token>`",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"phone": "13800138000", "password": "MyPass@123", "tenant_id": "t1"}}}}}},
    "GET /auth/profile": {"tags": ["🔐 认证"], "summary": "获取当前用户信息",
        "description": "需要 Header 携带 `Authorization: Bearer <token>`"},

    # 策略
    "POST /strategy": {"tags": ["📊 策略"], "summary": "创建交易策略",
        "description": "创建策略 → 写入 `strategies` + `strategy_versions` 表",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"name": "双均线策略", "logic_code": "def run(ctx):\\n    if ctx.ma5 > ctx.ma20: ctx.buy()", "params_json": {"fast": 5, "slow": 20}}}}}}},
    "GET /strategy": {"tags": ["📊 策略"], "summary": "获取策略列表", "description": "返回当前用户所有策略"},
    "POST /strategy/{strategy_id}/backtest": {"tags": ["📊 策略"], "summary": "发起回测",
        "description": "回测任务 → 写入 `backtest_tasks` 表",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"start_date": "2025-01-01", "end_date": "2025-12-31", "initial_capital": 100000}}}}}},

    # 交易
    "GET /trade/modes": {"tags": ["💹 交易"], "summary": "交易模式列表", "description": "返回可用交易模式（模拟/实盘）"},
    "POST /trade/order": {"tags": ["💹 交易"], "summary": "同步下单",
        "description": "同步提交订单 → 写入 `orders` 表。direction: BUY/SELL, env: sim/real",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"symbol": "600519.SH", "direction": "BUY", "price": 1800.50, "volume": 100, "env": "sim"}}}}}},
    "POST /trade/order/async": {"tags": ["💹 交易"], "summary": "异步下单",
        "description": "异步下单立即返回 → 写入 `orders` 表，结果通过 WebSocket 推送",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"symbol": "000001.SZ", "direction": "SELL", "price": 15.20, "volume": 200, "env": "sim"}}}}}},
    "GET /account/positions": {"tags": ["💹 交易"], "summary": "查询持仓", "description": "获取当前持仓（股票、数量、成本、浮盈）"},

    # 行情
    "GET /market/hot": {"tags": ["📈 行情"], "summary": "热门行情排行", "description": "当日涨幅/成交量排行榜，无需认证"},
    "GET /market/depth": {"tags": ["📈 行情"], "summary": "五档深度行情", "description": "需传 `symbol` 参数，如 `600519.SH`"},
    "GET /market/quote": {"tags": ["📈 行情"], "summary": "即时报价", "description": "获取最新价格、涨跌幅"},
    "POST /market/alert": {"tags": ["📈 行情"], "summary": "创建行情告警",
        "description": "设置价格条件告警",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"symbol": "600519.SH", "condition": "price > 2000", "threshold": 2000.0, "level": "warning"}}}}}},

    # 数据
    "GET /data/market": {"tags": ["📉 数据"], "summary": "查询历史行情", "description": "参数: symbol, start_date, end_date"},
    "GET /dashboard/overview": {"tags": ["📉 数据"], "summary": "仪表盘概览", "description": "策略数、持仓汇总、今日盈亏"},

    # 社区
    "POST /community/post": {"tags": ["🏘️ 社区"], "summary": "发布帖子",
        "description": "发帖 → 写入 `community_posts` 表",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"title": "分享我的双均线策略", "content": "MA5/MA20策略月收益8%", "category": "strategy_share"}}}}}},
    "GET /community/rankings": {"tags": ["🏘️ 社区"], "summary": "社区排行榜", "description": "按收益率/贡献度排序"},
    "POST /community/interaction": {"tags": ["🏘️ 社区"], "summary": "帖子互动(点赞/评论)",
        "description": "→ 写入 `community_interactions` 表",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"post_id": "帖子ID", "interaction_type": "like", "content": ""}}}}}},
    "POST /community/message": {"tags": ["🏘️ 社区"], "summary": "社区消息",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"receiver_id": "目标用户ID", "content": "想讨论策略优化"}}}}}},

    # 社交
    "POST /social/follow/{user_id}": {"tags": ["👥 社交"], "summary": "关注用户", "description": "→ 写入 `community_relations` 表"},
    "DELETE /social/follow/{user_id}": {"tags": ["👥 社交"], "summary": "取消关注"},
    "GET /social/following": {"tags": ["👥 社交"], "summary": "我的关注列表"},
    "GET /social/followers": {"tags": ["👥 社交"], "summary": "我的粉丝列表"},
    "GET /social/relation/{user_id}": {"tags": ["👥 社交"], "summary": "查询关系", "description": "关注/互关/陌生人"},

    # 私信
    "POST /messages": {"tags": ["✉️ 私信"], "summary": "发送私信",
        "description": "→ 写入 `community_messages` 表",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"receiver_id": "目标用户ID", "content": "你好，对你的策略很感兴趣"}}}}}},
    "GET /messages": {"tags": ["✉️ 私信"], "summary": "私信列表", "description": "参数: peer_id=对方用户ID"},
    "PUT /messages/{message_id}/read": {"tags": ["✉️ 私信"], "summary": "标记已读"},

    # 告警
    "POST /alerts": {"tags": ["🚨 告警"], "summary": "创建告警",
        "description": "→ 写入 `alerts` 表。level: info/warning/error/critical",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"title": "茅台突破2000", "message": "600519股价突破2000元", "level": "warning", "source": "price_monitor"}}}}}},
    "GET /alerts": {"tags": ["🚨 告警"], "summary": "告警列表", "description": "可按 status 筛选 (active/resolved)"},
    "PUT /alerts/{alert_id}/resolve": {"tags": ["🚨 告警"], "summary": "解决告警"},

    # 备份
    "POST /backups": {"tags": ["💾 备份"], "summary": "创建备份",
        "description": "→ 写入 `backups` 表",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"name": "每日全量备份", "type": "full"}}}}}},
    "GET /backups": {"tags": ["💾 备份"], "summary": "备份列表"},
    "POST /backups/{backup_id}/restore": {"tags": ["💾 备份"], "summary": "恢复备份"},

    # AI 配置
    "POST /ai-config/": {"tags": ["🤖 AI 配置"], "summary": "设置 AI 配置",
        "description": "→ 写入 `ai_configs` 表",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"key": "default_model", "value": {"model": "gpt-4o-mini", "temperature": 0.7}, "description": "默认模型配置"}}}}}},
    "GET /ai-config/": {"tags": ["🤖 AI 配置"], "summary": "AI 配置列表"},
    "GET /ai-config/{key}": {"tags": ["🤖 AI 配置"], "summary": "获取指定配置"},
    "GET /ai-config/audit": {"tags": ["🤖 AI 配置"], "summary": "配置审计日志"},
    "GET /ai-config/permission-check": {"tags": ["🤖 AI 配置"], "summary": "权限检查", "description": "参数: config_type, action"},
    "GET /ai-config/backup/download": {"tags": ["🤖 AI 配置"], "summary": "下载 AI 配置备份"},
    "POST /ai-config/backup/restore": {"tags": ["🤖 AI 配置"], "summary": "恢复 AI 配置(上传JSON文件)"},
    "POST /ai/strategy/parse": {"tags": ["🤖 AI 配置"], "summary": "AI 策略解析",
        "description": "自然语言 → 结构化策略代码",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"prompt": "当RSI超过70时卖出，低于30时买入"}}}}}},

    # 配置中心
    "POST /config-center": {"tags": ["⚙️ 配置中心"], "summary": "写入配置",
        "description": "→ 写入 `config_entries` 表",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"namespace": "trading", "key": "max_position_ratio", "value": "0.3", "description": "单票最大仓位比例"}}}}}},
    "GET /config-center": {"tags": ["⚙️ 配置中心"], "summary": "配置列表"},
    "GET /config-center/{namespace}/{key}": {"tags": ["⚙️ 配置中心"], "summary": "获取指定配置", "description": "例: /config-center/trading/max_position_ratio"},
    "DELETE /config-center/{namespace}/{key}": {"tags": ["⚙️ 配置中心"], "summary": "删除配置"},

    # 运维
    "GET /system/health": {"tags": ["🔧 运维"], "summary": "系统健康检查", "description": "无需认证，可用于探活"},
    "GET /ops/status": {"tags": ["🔧 运维"], "summary": "运维状态", "description": "版本、运行时长、连接池"},
    "GET /logs/audit": {"tags": ["🔧 运维"], "summary": "审计日志"},
    "GET /metrics": {"tags": ["🔧 运维"], "summary": "Prometheus 指标", "description": "监控系统自动采集"},

    # 合规
    "GET /consents/": {"tags": ["🔒 合规"], "summary": "授权列表"},
    "POST /consents/": {"tags": ["🔒 合规"], "summary": "授予数据授权",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"scope": "market_data", "consent_id": "c-001"}}}}}},
    "DELETE /consents/{consent_id}": {"tags": ["🔒 合规"], "summary": "撤销授权"},
    "POST /data-rights/request": {"tags": ["🔒 合规"], "summary": "数据权利请求(GDPR)",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"request_type": "export", "reason": "导出交易记录", "data_categories": ["trade_history"]}}}}}},

    # 定价
    "GET /pricing/plans": {"tags": ["💰 定价"], "summary": "套餐方案", "description": "基础/专业/VIP，无需认证"},
    "GET /pricing/entitlements": {"tags": ["💰 定价"], "summary": "用户权益"},

    # 因子
    "GET /factors/public": {"tags": ["📐 因子"], "summary": "公共因子库", "description": "系统内置因子，无需认证"},
    "POST /user-factors/": {"tags": ["📐 因子"], "summary": "创建自定义因子",
        "description": "→ 写入 `user_factors` 表",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"name": "20日动量", "code": "mom_20d", "description": "20日价格动量"}}}}}},
    "GET /user-factors/": {"tags": ["📐 因子"], "summary": "我的因子列表"},

    # 导入/导出
    "POST /import/strategy": {"tags": ["📦 导入导出"], "summary": "导入策略(CSV/XLSX)", "description": "上传文件批量导入"},
    "POST /import/knowledge": {"tags": ["📦 导入导出"], "summary": "导入知识库(JSON)"},
    "GET /export/audit": {"tags": ["📦 导入导出"], "summary": "导出审计记录"},
    "GET /export/report/{report_id}": {"tags": ["📦 导入导出"], "summary": "下载导出报告"},

    # 回放
    "POST /replay/report": {"tags": ["🎬 回放"], "summary": "创建回放报告",
        "description": "→ 写入 `replay_reports` 表",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"strategy_id": "策略ID", "start_date": "2025-01-01", "end_date": "2025-06-30"}}}}}},
    "POST /replay/analysis": {"tags": ["🎬 回放"], "summary": "回放分析", "description": "参数: trade_id"},
    "POST /replay/export": {"tags": ["🎬 回放"], "summary": "导出回放报告",
        "openapi_extra": {"requestBody": {"content": {"application/json": {"example": {"report_id": "报告ID", "export_type": "csv"}}}}}},

    # 对象存储
    "POST /object-storage/upload": {"tags": ["🗄️ 对象存储"], "summary": "上传文件", "description": "参数: object_key=存储路径"},
    "GET /object-storage/list": {"tags": ["🗄️ 对象存储"], "summary": "文件列表"},
    "DELETE /object-storage/{object_key:path}": {"tags": ["🗄️ 对象存储"], "summary": "删除文件"},
    "POST /object-storage/backup-persist": {"tags": ["🗄️ 对象存储"], "summary": "持久化备份"},

    # 管理员
    "POST /admin/tenant/grant": {"tags": ["👑 管理员"], "summary": "管理员授权", "description": "参数: target_tenant_id, feature"},
}

for _route in app.routes:
    if not hasattr(_route, "methods"):
        continue
    for _m in _route.methods:
        _doc = _ROUTE_DOC.get(f"{_m} {_route.path}")
        if _doc:
            if "tags" in _doc:
                _route.tags = _doc["tags"]
            if "summary" in _doc:
                _route.summary = _doc["summary"]
            if "description" in _doc:
                _route.description = _doc["description"]
            if "openapi_extra" in _doc:
                _route.openapi_extra = _doc["openapi_extra"]


# ──────────────────────────────────────────────────
# 前端静态文件挂载 (合并为单端口 8000)
# 必须放在所有 API 路由之后, 避免拦截 API 请求
# ──────────────────────────────────────────────────
_FRONTEND_DIR = Path(__file__).resolve().parent.parent.parent / "frontend"

# admin 管理面板 (独立子目录, html=True 支持目录索引)
if (_FRONTEND_DIR / "public" / "admin").exists():
    app.mount("/admin", StaticFiles(directory=str(_FRONTEND_DIR / "public" / "admin"), html=True), name="admin")

# 主前端 SPA fallback — 所有非 API 路径返回前端文件或 index.html
@app.get("/{full_path:path}", include_in_schema=False)
async def serve_frontend(full_path: str):
    file_path = _FRONTEND_DIR / full_path
    if file_path.is_file():
        return FileResponse(file_path)
    index = _FRONTEND_DIR / "index.html"
    if index.is_file():
        return FileResponse(index)
    return FileResponse(str(_FRONTEND_DIR / "index.html"))
