"""代理池配置与状态：配置中心读写、status 拉取、test-batch、delete-invalid。"""

import asyncio
import json
import logging
import random
import time
from typing import Any, Optional

import httpx

from src.services.config_center_service import get_config, set_config
from src.utils.log_entries import log_entry
from src.services.data_service.validated_proxy_store import (
    VALIDATED_PROXY_SET,
    clear_validated_proxy_pool,
    get_validated_proxy_client,
    proxy_raw_delete,
    proxy_raw_sadd,
    proxy_raw_smembers,
    validated_proxy_del_meta,
    validated_proxy_get_meta,
    validated_proxy_sadd,
    validated_proxy_set_meta,
    validated_proxy_smembers,
    validated_proxy_srem,
    validated_proxy_srandmember,
)

logger = logging.getLogger(__name__)

CONFIG_NAMESPACE = "default"
CONFIG_KEY = "proxy_pool"

SCHEDULE_LOG_KEY_PREFIX = "proxy_pool:schedule_log:"
SCHEDULE_LOG_MAX = 300

DEFAULT_PROXY_FILE_URLS = [
    "https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/http.txt",
    "https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/https.txt",
]

DEFAULT_CONFIG = {
    "url": "",
    "enabled": False,
    "concurrent": 1,
    "schedule_enabled": False,
    "schedule_interval_seconds": 3600,
    "schedule_last_run_at": 0,
    "business_test_url": "",
    "redis_url": "",
    "verbose_log": False,
    "proxy_file_urls": DEFAULT_PROXY_FILE_URLS,
    "download_proxy": "",
    "fast_schedule_enabled": False,
    "fast_schedule_interval_seconds": 600,
    "fast_schedule_last_run_at": 0,
    "fast_concurrency": 500,
}

# 第一条线（定时更新）执行中时置入，供第二条线本周期跳过
_refresh_running: set[str] = set()
# 第二条线（快速更新代理池）执行中时置入，供前端禁用一键测试/删除无效
_fast_running: set[str] = set()

# 业务目标：入池与一键测试均验证五域名（eastmoney / sse / sina / gtimg.cn / proxy.finance.qq.com），每域测 HTTP+HTTPS，eastmoney.com 通过且至少一个其他域名通过才入池；协议为 http / https / http/s
# 每域名延迟需低于此值才视为通过（入池与 test-batch 一致）
BUSINESS_TEST_MAX_LATENCY_MS = 2000
BUSINESS_TEST_DOMAINS = [
    ("eastmoney.com", "http://quote.eastmoney.com/", "https://quote.eastmoney.com/"),
    ("sse.com.cn", "http://www.sse.com.cn/", "https://www.sse.com.cn/"),
    ("finance.sina.com.cn", "http://finance.sina.com.cn/", "https://finance.sina.com.cn/"),
    ("gtimg.cn", "https://web.ifzq.gtimg.cn/", "https://web.ifzq.gtimg.cn/"),
    ("proxy.finance.qq.com", "https://proxy.finance.qq.com/", "https://proxy.finance.qq.com/"),
]
TEST_TARGET_LABELS = [d[0] for d in BUSINESS_TEST_DOMAINS]

EASTMONEY_LABEL = "eastmoney.com"


def _valid_for_pool(domain_results: list[dict]) -> bool:
    """入池/复测通过条件：eastmoney.com 必须通过且至少一个其他域名通过。通过 = 该域 valid=True（含延迟 < 2000ms）。"""
    if not domain_results:
        return False
    eastmoney_ok = False
    other_ok = False
    for d in domain_results:
        if not isinstance(d, dict):
            continue
        label = d.get("label") or ""
        valid = d.get("valid") is True
        if label == EASTMONEY_LABEL:
            eastmoney_ok = valid
        else:
            if valid:
                other_ok = True
    return eastmoney_ok and other_ok


def _proxy_item_valid_for_domain(proxy_item: dict, domain: str) -> bool:
    """检查 proxies_list 项中 domain_results 是否包含该 domain 且 valid=True。与 get_proxies_top_pct 过滤逻辑一致。"""
    if not domain or not domain.strip():
        return True
    dr = proxy_item.get("domain_results") or []
    if isinstance(dr, str):
        try:
            dr = json.loads(dr) if dr else []
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
    if not isinstance(dr, list):
        return False
    for d in dr:
        if d.get("label") == domain and d.get("valid") is True:
            return True
    return False


def _proxy_passes_domain(meta: dict, domain: str) -> bool:
    """检查 meta.domain_results 中该 domain 是否 valid。domain 为空时返回 True。"""
    if not domain or not domain.strip():
        return True
    raw_dr = meta.get("domain_results")
    if not raw_dr:
        return False
    try:
        domain_results = json.loads(raw_dr) if isinstance(raw_dr, str) else raw_dr
        if not isinstance(domain_results, list):
            return False
        for d in domain_results:
            if d.get("label") == domain and d.get("valid") is True:
                return True
        return False
    except (TypeError, ValueError, json.JSONDecodeError):
        return False


def get_business_test_domains() -> list[dict]:
    """返回当前入池校验使用的域名列表（只读），供配置接口/前端展示。"""
    return [{"label": d[0], "http_url": d[1], "https_url": d[2]} for d in BUSINESS_TEST_DOMAINS]


# 兼容旧逻辑：若配置了 business_test_url 则仅用该 URL 做单目标校验（不推荐）；未配置则用五域名
HTTP_TEST_URL = "http://httpbin.org/get"
HTTPS_TEST_URL = "https://httpbin.org/get"
TEST_TIMEOUT = 10.0
SOURCE_FROM_FILE = "file"  # 来自代理文件 URL 的候选在 meta 中 source 用此值，与 API 来源区分


def _get_test_urls_from_config(cfg: dict) -> tuple[str, str]:
    """从配置取校验 URL：若配置了 business_test_url 则用该 URL（HTTP/HTTPS 同）；否则兜底 httpbin 并打日志。"""
    u = (cfg.get("business_test_url") or "").strip()
    if u:
        return (u, u)
    logger.info("未配置 business_test_url，使用 httpbin")
    return (HTTP_TEST_URL, HTTPS_TEST_URL)


def _redis_url_from_config(cfg: dict) -> Optional[str]:
    """从配置取 redis_url，非空则返回，否则返回 None（store 用环境变量）。"""
    u = (cfg.get("redis_url") or "").strip()
    return u or None


def _normalize_ip(ip_str: str) -> str:
    """对 IPv4 字符串做规范整形：按 '.' 分四段，每段去掉前导零后拼回。非法段数或非数字段返回空串。"""
    ip_str = (ip_str or "").strip()
    if not ip_str:
        return ""
    parts = ip_str.split(".")
    if len(parts) != 4:
        return ""
    out = []
    for p in parts:
        p = p.strip()
        if not p or not p.isdigit():
            return ""
        out.append(str(int(p)))
    return ".".join(out)


def _parse_proxy_file_content(text: str) -> list[str]:
    """按行解析文本，每行 http(s)://host:port，取 host:port 并对 host 做 IP 整形，返回去重后的 ip:port 列表。"""
    seen: set[str] = set()
    result: list[str] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        lower = line.lower()
        if lower.startswith("http://"):
            rest = line[7:].strip()
        elif lower.startswith("https://"):
            rest = line[8:].strip()
        else:
            continue
        if ":" not in rest:
            continue
        host, _, port = rest.rpartition(":")
        if not host or not port:
            continue
        norm_ip = _normalize_ip(host)
        if not norm_ip:
            continue
        key = f"{norm_ip}:{port}"
        if key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


async def _fetch_proxy_file_urls(
    urls: list[str],
    redis_url: Optional[str],
    verbose: bool,
    download_proxy: Optional[str] = None,
) -> tuple[list[str], list[dict]]:
    """拉取每个 URL 的文本，解析为 ip:port 列表，汇总后替换写入 proxy_raw:set。返回 (汇总 ip:port 列表, log_entries)。
    download_proxy 非空时经该 HTTP 代理发起请求（如 http://127.0.0.1:10809）。"""
    log_entries: list[dict] = []
    all_proxies: list[str] = []
    proxy_for_fetch: Optional[str] = None
    dp = (download_proxy or "").strip()
    if dp:
        lower = dp.lower()
        if lower.startswith("http://") or lower.startswith("https://"):
            proxy_for_fetch = dp
        else:
            logger.warning("_fetch_proxy_file_urls: download_proxy 格式非法，直连: %s", dp[:80])
    for url in (urls or []):
        url = (url or "").strip()
        if not url:
            continue
        if verbose:
            log_entries.append(log_entry("INFO", f"[代理文件] GET {url}" + (f" via {proxy_for_fetch}" if proxy_for_fetch else "")))
        try:
            client_kw: dict[str, Any] = {"timeout": 15.0, "follow_redirects": True}
            if proxy_for_fetch:
                client_kw["proxy"] = proxy_for_fetch
            async with httpx.AsyncClient(**client_kw) as client:
                r = await client.get(url)
                r.raise_for_status()
                text = r.text
        except Exception as e:
            logger.warning("proxy file url fetch failed %s: %s", url, e)
            if verbose:
                log_entries.append(log_entry("WARN", f"[代理文件] failed {url}: {type(e).__name__} {str(e)[:150]}"))
            continue
        parsed = _parse_proxy_file_content(text)
        if verbose:
            log_entries.append(log_entry("INFO", f"[代理文件] {url} parsed={len(parsed)}"))
        all_proxies.extend(parsed)
    # 去重保持顺序（首次出现为准）
    seen = set()
    unique: list[str] = []
    for p in all_proxies:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    if urls:
        try:
            await proxy_raw_delete(redis_url=redis_url)
            if unique:
                await proxy_raw_sadd(unique, redis_url=redis_url)
            if verbose:
                log_entries.append(log_entry("INFO", f"[代理文件] proxy_raw:set replaced count={len(unique)}"))
        except Exception as e:
            logger.warning("proxy_raw write failed: %s", e)
            if verbose:
                log_entries.append(log_entry("ERROR", f"[代理文件] Redis raw write failed: {type(e).__name__} {str(e)[:100]}"))
    return (unique, log_entries)


async def get_proxy_pool_config(tenant_id: str) -> dict:
    """从配置中心读取 proxy_pool 配置。"""
    raw = await get_config(tenant_id, CONFIG_NAMESPACE, CONFIG_KEY)
    if not raw or not raw.get("value"):
        return dict(DEFAULT_CONFIG)
    val = raw["value"]
    if isinstance(val, dict):
        out = dict(DEFAULT_CONFIG)
        out.update({k: val[k] for k in out if k in val})
        return out
    if isinstance(val, str):
        try:
            out = json.loads(val)
            return {**DEFAULT_CONFIG, **out}
        except Exception:
            return dict(DEFAULT_CONFIG)
    return dict(DEFAULT_CONFIG)


def _normalize_download_proxy(value: Any) -> str:
    """规范化 download_proxy：strip 后空串视为未配置；非 http(s) 开头打日志并视为未配置。"""
    s = (value or "").strip()
    if not s:
        return ""
    lower = s.lower()
    if not (lower.startswith("http://") or lower.startswith("https://")):
        logger.warning("download_proxy 格式非法（需 http:// 或 https:// 开头），已忽略: %s", s[:80])
        return ""
    return s


async def set_proxy_pool_config(tenant_id: str, payload: dict) -> dict:
    """写入 proxy_pool 配置到配置中心（与现有配置合并后写入）。"""
    current = await get_proxy_pool_config(tenant_id)
    allowed = {
        "url", "enabled", "concurrent", "schedule_enabled", "schedule_interval_seconds", "schedule_last_run_at",
        "business_test_url", "redis_url", "verbose_log", "proxy_file_urls", "download_proxy",
        "fast_schedule_enabled", "fast_schedule_interval_seconds", "fast_schedule_last_run_at", "fast_concurrency",
    }
    body = {**current, **{k: v for k, v in payload.items() if k in allowed}}
    if "verbose_log" in body:
        v = body["verbose_log"]
        body["verbose_log"] = v is True or (isinstance(v, str) and v.strip().lower() in ("true", "1"))
    if "schedule_interval_seconds" in body:
        try:
            v = int(body["schedule_interval_seconds"])
            body["schedule_interval_seconds"] = max(60, v) if v >= 60 else 3600
        except (TypeError, ValueError):
            body["schedule_interval_seconds"] = DEFAULT_CONFIG["schedule_interval_seconds"]
    if "fast_schedule_interval_seconds" in body:
        try:
            v = int(body["fast_schedule_interval_seconds"])
            body["fast_schedule_interval_seconds"] = max(60, min(86400, v))
        except (TypeError, ValueError):
            body["fast_schedule_interval_seconds"] = DEFAULT_CONFIG["fast_schedule_interval_seconds"]
    if "fast_concurrency" in body:
        try:
            v = int(body["fast_concurrency"])
            body["fast_concurrency"] = max(1, min(500, v))
        except (TypeError, ValueError):
            body["fast_concurrency"] = DEFAULT_CONFIG["fast_concurrency"]
    if "proxy_file_urls" in body:
        v = body["proxy_file_urls"]
        if isinstance(v, list):
            body["proxy_file_urls"] = [str(x).strip() for x in v if x and str(x).strip()]
        elif isinstance(v, str):
            body["proxy_file_urls"] = [s.strip() for s in (v.replace(",", "\n").splitlines()) if s.strip()]
        else:
            body["proxy_file_urls"] = list(DEFAULT_PROXY_FILE_URLS)
    if "download_proxy" in body:
        body["download_proxy"] = _normalize_download_proxy(body["download_proxy"])
    value_str = json.dumps(body, ensure_ascii=False)
    await set_config(
        tenant_id, CONFIG_NAMESPACE, CONFIG_KEY, value_str, "json", "proxy_pool 代理池配置"
    )
    return await get_proxy_pool_config(tenant_id)


async def fetch_status_from_redis(tenant_id: str, verbose: bool = False) -> dict:
    """从 Redis validated_proxy 主数据取 status：count、全量 meta 聚合 http_type/source、抽样列表。reachable=True 表示 Redis 可读。verbose 时返回 log_entries。"""
    log_entries: list[dict] = []
    cfg = await get_proxy_pool_config(tenant_id)
    redis_url = _redis_url_from_config(cfg)
    try:
        if verbose:
            log_entries.append(log_entry("INFO", "[status] connecting to Redis"))
        client = await get_validated_proxy_client(redis_url)
        count = await client.scard(VALIDATED_PROXY_SET)
        members = await validated_proxy_smembers(redis_url=redis_url)
        if verbose:
            log_entries.append(log_entry("INFO", f"[status] SCARD={count} aggregating meta for all"))
        http_type: dict[str, int] = {}
        source: dict[str, int] = {}
        proxies_list: list[dict] = []
        for proxy in members:
            meta = await validated_proxy_get_meta(proxy, redis_url=redis_url)
            p = (meta.get("protocol") or "unknown").strip() or "unknown"
            http_type[p] = http_type.get(p, 0) + 1
            s = (meta.get("source") or "unknown").strip() or "unknown"
            source[s] = source.get(s, 0) + 1
            raw_ms = meta.get("latency_ms")
            try:
                latency_ms = int(raw_ms) if raw_ms is not None else None
            except (TypeError, ValueError):
                latency_ms = None
            raw_dr = meta.get("domain_results")
            domain_results: list[dict] = []
            if raw_dr:
                try:
                    domain_results = json.loads(raw_dr)
                    if not isinstance(domain_results, list):
                        domain_results = []
                except (TypeError, ValueError, json.JSONDecodeError):
                    domain_results = []
            # 入池时若为三域名，meta 仅存 3 条；补齐为五列便于前端展示，缺项标为「入池时未测」
            if len(domain_results) < len(TEST_TARGET_LABELS):
                dr_by_label = {d.get("label"): d for d in domain_results if d.get("label")}
                domain_results = []
                for lbl in TEST_TARGET_LABELS:
                    if lbl in dr_by_label:
                        domain_results.append(dr_by_label[lbl])
                    else:
                        domain_results.append({"label": lbl, "valid": False, "latency_ms": None, "error": "入池时未测", "protocol": None})
            proxies_list.append({"proxy": proxy, "latency_ms": latency_ms, "protocol": p, "domain_results": domain_results})
        proxies_list.sort(key=lambda x: (x["latency_ms"] is None, x["latency_ms"] or 999999))
        if verbose:
            log_entries.append(log_entry("INFO", f"[status] http_type={http_type} source={source}"))
        # 抽样列表仅用于展示，最多 20 条
        sample = members[:20] if len(members) > 20 else members
        if verbose:
            log_entries.append(log_entry("INFO", f"[status] proxies_sample size={len(sample)}"))
        out = {
            "reachable": True,
            "count": count,
            "http_type": http_type,
            "source": source,
            "proxies_sample": sample,
            "proxies_list": proxies_list,
            "fast_schedule_running": tenant_id in _fast_running,
        }
        if verbose:
            await _merge_schedule_log_entries(tenant_id, redis_url, log_entries, out)
        return out
    except Exception as e:
        logger.warning("fetch_status_from_redis failed: %s", e)
        if verbose:
            log_entries.append(log_entry("ERROR", f"[status] Redis failed: {type(e).__name__} {str(e)[:150]}"))
        out = {
            "reachable": False,
            "count": 0,
            "http_type": {},
            "source": {},
            "proxies_sample": [],
            "proxies_list": [],
            "fast_schedule_running": tenant_id in _fast_running,
        }
        if verbose:
            await _merge_schedule_log_entries(tenant_id, redis_url, log_entries, out)
        return out


async def _merge_schedule_log_entries(tenant_id: str, redis_url: Optional[str], log_entries: list[dict], out: dict) -> None:
    """当 verbose 时，从 Redis 读取 schedule 历史日志并合并到 out['log_entries']。"""
    schedule_entries: list[dict] = []
    try:
        client = await get_validated_proxy_client(redis_url)
        key = f"{SCHEDULE_LOG_KEY_PREFIX}{tenant_id}"
        raw_list = await client.lrange(key, 0, -1)
        for raw in raw_list or []:
            try:
                schedule_entries.append(json.loads(raw))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
        out["log_entries"] = log_entries + schedule_entries
    except Exception as e:
        logger.warning("_merge_schedule_log_entries failed: %s", e)
        if log_entries:
            out["log_entries"] = log_entries


async def append_schedule_log_entries(tenant_id: str, log_entries: list[dict]) -> None:
    """将定时/快速更新当次执行的 log_entries 追加到 Redis list，仅保留最近 SCHEDULE_LOG_MAX 条。"""
    if not log_entries:
        return
    try:
        cfg = await get_proxy_pool_config(tenant_id)
        redis_url = _redis_url_from_config(cfg)
        client = await get_validated_proxy_client(redis_url)
        key = f"{SCHEDULE_LOG_KEY_PREFIX}{tenant_id}"
        for e in log_entries:
            await client.rpush(key, json.dumps(e, ensure_ascii=False))
        await client.ltrim(key, -SCHEDULE_LOG_MAX, -1)
    except Exception as e:
        logger.warning("append_schedule_log_entries failed: %s", e)


async def _fetch_external_reachable(base_url: str) -> bool:
    """请求外部 proxy_pool /count/ 或 /，成功则 True。仅作运维提示。"""
    base_url = (base_url or "").rstrip("/")
    if not base_url:
        return False
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{base_url}/count/")
            return r.status_code == 200
    except Exception:
        return False


async def fetch_status(base_url: str) -> dict:
    """请求 proxy_pool 的 /count/ 与 /all/（抽样），返回 reachable、count、http_type、source、proxies_sample。"""
    base_url = (base_url or "").rstrip("/")
    if not base_url:
        return {"reachable": False, "count": 0, "http_type": {}, "source": {}, "proxies_sample": []}

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(f"{base_url}/count/")
            r.raise_for_status()
            data = r.json()
            count = data.get("count", 0)
            http_type = data.get("http_type") or {}
            source = data.get("source") or {}

            sample: list = []
            try:
                r2 = await client.get(f"{base_url}/all/")
                if r2.status_code == 200:
                    all_list = r2.json()
                    if isinstance(all_list, list):
                        sample = all_list[:20]
            except Exception:
                pass

            logger.info("proxy_pool fetch_status: base_url=%s 池内总数=%s http_type=%s", base_url, count, http_type)
            return {
                "reachable": True,
                "count": count,
                "http_type": http_type,
                "source": source,
                "proxies_sample": sample,
            }
    except Exception as e:
        logger.warning("proxy_pool status fetch failed: %s", e)
        return {"reachable": False, "count": 0, "http_type": {}, "source": {}, "proxies_sample": []}


def _test_one_url(proxy: str, test_url: str) -> tuple[bool, int, Optional[str]]:
    """同步测一个代理访问某 URL：返回 (成功, 延迟ms, 错误信息)。"""
    proxy_url = f"http://{proxy}"
    t0 = time.perf_counter()
    try:
        with httpx.Client(proxy=proxy_url, timeout=TEST_TIMEOUT) as client:
            r = client.get(test_url)
            ok = r.status_code == 200
    except Exception as e:
        ok = False
        err = str(e)
    else:
        err = None
    latency_ms = round((time.perf_counter() - t0) * 1000)
    return ok, latency_ms, err


async def run_test_batch(
    tenant_id: str,
    limit: int = 20,
    test_url: Optional[str] = None,
    verbose: bool = False,
    sorted_by_latency: bool = False,
):
    """从 Redis 本地池取最多 limit 个代理，逐条测五域名；返回 per-domain 结果。valid=eastmoney 通过且至少一个其他域通过。sorted_by_latency=True 时按延迟升序取前 limit 条。"""
    cfg = await get_proxy_pool_config(tenant_id)
    verbose = _bool_verbose(cfg, verbose)
    redis_url = _redis_url_from_config(cfg)
    log_entries: list[dict] = []

    if sorted_by_latency:
        members = await validated_proxy_smembers(redis_url=redis_url)
        if not members:
            if verbose:
                log_entries.append(log_entry("INFO", "[test-batch] pool empty (sorted)"))
            return {"results": [], "log_entries": log_entries} if verbose else []
        # 按 meta 中的 latency_ms 升序，取前 limit 个
        with_latency: list[tuple[str, int]] = []
        for p in members:
            meta = await validated_proxy_get_meta(p, redis_url=redis_url)
            raw_ms = meta.get("latency_ms")
            try:
                ms = int(raw_ms) if raw_ms is not None else 999999
            except (TypeError, ValueError):
                ms = 999999
            with_latency.append((p, ms))
        with_latency.sort(key=lambda x: x[1])
        proxies = [p for p, _ in with_latency[:limit]]
    else:
        proxies = await validated_proxy_srandmember(limit, redis_url=redis_url)
    if not proxies:
        if verbose:
            log_entries.append(log_entry("INFO", "[test-batch] pool empty, no sample"))
        return {"results": [], "log_entries": log_entries} if verbose else []

    if verbose:
        log_entries.append(log_entry("INFO", f"[test-batch] sample size={len(proxies)} 五域名: {TEST_TARGET_LABELS}"))
    results = []
    for p in proxies:
        r = await _test_one_proxy_full(tenant_id, p)
        if verbose:
            domain_results = r.get("domain_results") or []
            parts = [f"{d['label']}={d.get('protocol') or '否'} {'✓'+str(d.get('latency_ms'))+'ms' if d.get('valid') else str(d.get('error') or '')[:30]}" for d in domain_results]
            log_entries.append(log_entry("INFO", f"  proxy={p} " + " ".join(parts) + f" valid={r.get('valid')}"))
        results.append(r)
    valid_count = sum(1 for r in results if r.get("valid"))
    if verbose:
        log_entries.append(log_entry("INFO", f"[test-batch] results={len(results)} valid={valid_count}"))
    logger.info("proxy_pool run_test_batch: tenant_id=%s 测试数=%s 有效数=%s 无效数=%s", tenant_id, len(results), valid_count, len(results) - valid_count)
    if verbose and log_entries:
        return {"results": results, "log_entries": log_entries}
    return results


def _bool_verbose(cfg: dict, verbose: bool) -> bool:
    return verbose or bool(cfg.get("verbose_log"))


async def _test_one_proxy_full(tenant_id: str, proxy: str) -> dict:
    """对单个代理测五域名（与 run_test_batch 一致），返回 {proxy, valid, region, domain_results, test_domains}。"""
    cfg = await get_proxy_pool_config(tenant_id)
    redis_url = _redis_url_from_config(cfg)
    meta = await validated_proxy_get_meta(proxy, redis_url=redis_url)
    region = meta.get("region") or None
    loop = asyncio.get_event_loop()
    domain_results: list[dict] = []
    for label, http_url, https_url in BUSINESS_TEST_DOMAINS:
        ok_http, ms_http, err_http = await loop.run_in_executor(None, _test_one_url, proxy, http_url)
        ok_https, ms_https, err_https = await loop.run_in_executor(None, _test_one_url, proxy, https_url)
        best_ms: Optional[int] = None
        if ok_http:
            best_ms = ms_http if best_ms is None else min(best_ms, ms_http)
        if ok_https:
            best_ms = ms_https if best_ms is None else min(best_ms, ms_https)
        if ok_http and ok_https:
            protocol_display = "HTTP/S"
            latency_ms = (ms_http + ms_https) // 2
        elif ok_https:
            protocol_display = "HTTPS"
            latency_ms = ms_https
        elif ok_http:
            protocol_display = "HTTP"
            latency_ms = ms_http
        else:
            protocol_display = None
            latency_ms = None
        domain_valid = (ok_http or ok_https) and (best_ms is not None and best_ms < BUSINESS_TEST_MAX_LATENCY_MS)
        if domain_valid and latency_ms is not None:
            valid = True
            err = None
            out_latency_ms = latency_ms
        else:
            valid = False
            err = ("延迟超 2000ms" if (ok_http or ok_https) and best_ms is not None else (err_https or err_http or "失败"))[:80]
            out_latency_ms = best_ms if best_ms is not None else None
        domain_results.append({
            "label": label,
            "valid": valid,
            "latency_ms": out_latency_ms,
            "error": err,
            "protocol": protocol_display,
        })
    valid = _valid_for_pool(domain_results)
    return {
        "proxy": proxy,
        "valid": valid,
        "region": region,
        "domain_results": domain_results,
        "test_domains": list(TEST_TARGET_LABELS),
    }


async def run_refresh_pipeline(tenant_id: str, verbose: bool = False) -> dict:
    """拉取外部候选 → 业务校验 → 通过者入 Redis 本地池。返回摘要；verbose 时含 log_entries。"""
    cfg = await get_proxy_pool_config(tenant_id)
    verbose = _bool_verbose(cfg, verbose)
    log_entries: list[dict] = []

    base_url = (cfg.get("url") or "").strip().rstrip("/")
    redis_url = _redis_url_from_config(cfg)
    if not base_url or not cfg.get("enabled"):
        if verbose:
            log_entries.append(log_entry("WARN", "skip: no url or disabled"))
        return {"skipped": True, "reason": "no url or disabled", **({"log_entries": log_entries} if verbose else {})}

    if verbose:
        log_entries.append(log_entry("INFO", f"[拉取候选] GET {base_url}/all/"))
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(f"{base_url}/all/")
            r.raise_for_status()
            all_list = r.json()
        if verbose:
            log_entries.append(log_entry("INFO", f"[拉取候选] status={r.status_code} candidates_raw={len(all_list) if isinstance(all_list, list) else 0}"))
    except Exception as e:
        logger.warning("proxy_pool refresh fetch /all/ failed: %s", e)
        if verbose:
            log_entries.append(log_entry("WARN", f"[拉取候选] failed: {type(e).__name__} {str(e)[:200]}"))
        all_list = []

    if not isinstance(all_list, list):
        all_list = []

    # 代理文件 URL → 拉取并写入 proxy_raw:set，再与 API 候选合并
    file_urls = cfg.get("proxy_file_urls") or []
    if isinstance(file_urls, str):
        file_urls = [s.strip() for s in file_urls.replace(",", "\n").splitlines() if s.strip()]
    download_proxy = (cfg.get("download_proxy") or "").strip()
    raw_proxies: list[str] = []
    if file_urls:
        raw_proxies, file_logs = await _fetch_proxy_file_urls(file_urls, redis_url, verbose, download_proxy=download_proxy or None)
        if verbose and file_logs:
            log_entries.extend(file_logs)

    candidates_set: set[str] = set()
    candidates: list[dict] = []
    for item in (all_list or [])[:100]:
        if isinstance(item, dict) and item.get("proxy"):
            p = (item.get("proxy") or "").strip()
            if p and ":" in p and p not in candidates_set:
                candidates_set.add(p)
                candidates.append(item)
        elif isinstance(item, str) and ":" in item:
            p = item.strip()
            if p not in candidates_set:
                candidates_set.add(p)
                candidates.append({"proxy": p})
    raw_members: list[str] = []
    try:
        raw_members = await proxy_raw_smembers(redis_url=redis_url)
    except Exception as e:
        logger.warning("proxy_pool refresh proxy_raw_smembers failed: %s", e)
        if verbose:
            log_entries.append(log_entry("WARN", f"[拉取候选] proxy_raw SMEMBERS failed: {type(e).__name__}"))
    for p in raw_members:
        p = (p or "").strip()
        if p and ":" in p and p not in candidates_set:
            candidates_set.add(p)
            candidates.append({"proxy": p, "source": SOURCE_FROM_FILE})
    if verbose:
        log_entries.append(log_entry("INFO", f"[拉取候选] merged candidates total={len(candidates)} (api + raw)"))

    if verbose:
        log_entries.append(log_entry("INFO", f"[业务校验] eastmoney.com + 至少一个其他域通过才入池: {TEST_TARGET_LABELS}"))
    loop = asyncio.get_event_loop()
    added: list[str] = []
    total_checked = 0
    failed_count = 0

    for item in candidates:
        proxy = item.get("proxy") or ""
        if not proxy or ":" not in proxy:
            continue
        total_checked += 1
        # 每域测 HTTP + HTTPS，取成功请求中的最小延迟 best_ms；该域通过条件：(ok_http or ok_https) and best_ms < 2000
        domain_results: list[dict] = []
        http_any_ok, https_any_ok = False, False
        latencies: list[int] = []
        for label, http_url, https_url in BUSINESS_TEST_DOMAINS:
            ok_http, ms_http, err_http = await loop.run_in_executor(None, _test_one_url, proxy, http_url)
            ok_https, ms_https, err_https = await loop.run_in_executor(None, _test_one_url, proxy, https_url)
            best_ms: Optional[int] = None
            if ok_http:
                best_ms = ms_http if best_ms is None else min(best_ms, ms_http)
                http_any_ok = True
                latencies.append(ms_http)
            if ok_https:
                best_ms = ms_https if best_ms is None else min(best_ms, ms_https)
                https_any_ok = True
                latencies.append(ms_https)
            domain_ok = (ok_http or ok_https) and (best_ms is not None and best_ms < BUSINESS_TEST_MAX_LATENCY_MS)
            if domain_ok and best_ms is not None:
                protocol_display = "HTTP/S" if (ok_http and ok_https) else ("HTTPS" if ok_https else "HTTP")
                domain_results.append({
                    "label": label,
                    "valid": True,
                    "latency_ms": best_ms,
                    "error": None,
                    "protocol": protocol_display,
                })
            else:
                err = (err_https or err_http or ("延迟超 2000ms" if (ok_http or ok_https) and best_ms is not None else "失败"))[:80]
                domain_results.append({
                    "label": label,
                    "valid": False,
                    "latency_ms": best_ms,
                    "error": err,
                    "protocol": None,
                })
        valid = _valid_for_pool(domain_results)
        if verbose:
            parts = [f"{d['label']}={d.get('protocol') or '否'} {'✓'+str(d.get('latency_ms'))+'ms' if d['valid'] else str(d.get('error') or '')[:30]}" for d in domain_results]
            log_entries.append(log_entry("INFO", f"  proxy={proxy} " + " ".join(parts) + f" valid={valid}"))
        if not valid:
            failed_count += 1
            continue
        protocol = "http/s" if (http_any_ok and https_any_ok) else ("https" if https_any_ok else "http")
        latency_ms = sum(latencies) // len(latencies) if latencies else 0
        try:
            await validated_proxy_sadd(proxy, redis_url=redis_url)
            added.append(proxy)
            meta = {
                "protocol": protocol,
                "latency_ms": str(latency_ms),
                "region": str((item.get("region") or "")),
                "source": str((item.get("source") or "")),
                "validated_at": str(int(time.time())),
                "updated_at": str(int(time.time())),
                "domain_results": json.dumps(domain_results, ensure_ascii=False),
            }
            await validated_proxy_set_meta(proxy, meta, redis_url=redis_url)
            if verbose:
                log_entries.append(log_entry("INFO", f"[入池] SADD+HSET proxy={proxy} latency_ms={latency_ms}"))
        except Exception as e:
            logger.warning("validated_proxy_sadd/set_meta failed for %s: %s", proxy, e)
            if verbose:
                log_entries.append(log_entry("ERROR", f"[入池] failed proxy={proxy} error={type(e).__name__} {str(e)[:80]}"))
            failed_count += 1

    if verbose:
        log_entries.append(log_entry("INFO", f"[汇总] checked={total_checked} added={len(added)} failed={failed_count}"))
    logger.info("proxy_pool run_refresh_pipeline: checked=%s added=%s failed=%s", total_checked, len(added), failed_count)
    out = {"ok": True, "added": len(added), "checked": total_checked, "failed": failed_count}
    if verbose and log_entries:
        out["log_entries"] = log_entries
    return out


async def run_schedule_pipeline(tenant_id: str) -> dict:
    """执行定时流水线：拉取候选→业务校验→入 Redis；可选抽样再校验并剔除失效。verbose_log 为 true 时返回 log_entries。"""
    cfg = await get_proxy_pool_config(tenant_id)
    verbose = bool(cfg.get("verbose_log"))
    log_entries: list[dict] = []

    if not cfg.get("schedule_enabled") or not (cfg.get("url") or "").strip():
        if verbose:
            log_entries.append(log_entry("WARN", "[schedule] skip: disabled or no url"))
        return {"skipped": True, "reason": "disabled or no url", **({"log_entries": log_entries} if verbose else {})}
    base_url = (cfg.get("url") or "").strip()

    if verbose:
        log_entries.append(log_entry("INFO", "[schedule] phase 1: refresh (fetch→validate→write Redis)"))
    refresh_out = await run_refresh_pipeline(tenant_id, verbose=verbose)
    added = refresh_out.get("added", 0) if refresh_out.get("ok") else 0
    if verbose and refresh_out.get("log_entries"):
        log_entries.extend(refresh_out["log_entries"])

    if verbose:
        log_entries.append(log_entry("INFO", f"[schedule] phase 2: test-batch sample 50, added={added}"))
    test_out = await run_test_batch(tenant_id, limit=50, verbose=verbose)
    results = test_out.get("results", test_out) if isinstance(test_out, dict) else test_out
    if verbose and isinstance(test_out, dict) and test_out.get("log_entries"):
        log_entries.extend(test_out["log_entries"])
    invalid = [r["proxy"] for r in results if not r.get("valid")]
    deleted = []
    if invalid:
        if verbose:
            log_entries.append(log_entry("INFO", f"[schedule] phase 3: delete-invalid count={len(invalid)}"))
        out = await delete_invalid_proxies(invalid, base_url=None, verbose=verbose, tenant_id=tenant_id)
        deleted = out.get("deleted") or []
        if verbose and out.get("log_entries"):
            log_entries.extend(out["log_entries"])

    now_ts = int(time.time())
    await _update_schedule_last_run(tenant_id, now_ts)
    if verbose:
        log_entries.append(log_entry("INFO", f"[schedule] done tested={len(results)} deleted={len(deleted)} added={added} at={now_ts}"))
    ret = {"skipped": False, "tested": len(results), "deleted": len(deleted), "added": added, "at": now_ts}
    if verbose and log_entries:
        ret["log_entries"] = log_entries
    return ret


async def _update_schedule_last_run(tenant_id: str, ts: int) -> None:
    """仅更新 proxy_pool 配置中的 schedule_last_run_at。"""
    cfg = await get_proxy_pool_config(tenant_id)
    allowed = set(DEFAULT_CONFIG) | {"schedule_last_run_at"}
    body = {k: (ts if k == "schedule_last_run_at" else cfg.get(k)) for k in allowed}
    body["schedule_last_run_at"] = ts
    value_str = json.dumps(body, ensure_ascii=False)
    await set_config(tenant_id, CONFIG_NAMESPACE, CONFIG_KEY, value_str, "json", "proxy_pool 代理池配置")


async def _update_fast_schedule_last_run(tenant_id: str, ts: int) -> None:
    """仅更新 proxy_pool 配置中的 fast_schedule_last_run_at。"""
    cfg = await get_proxy_pool_config(tenant_id)
    allowed = set(DEFAULT_CONFIG) | {"fast_schedule_last_run_at"}
    body = {k: (ts if k == "fast_schedule_last_run_at" else cfg.get(k)) for k in allowed}
    body["fast_schedule_last_run_at"] = ts
    value_str = json.dumps(body, ensure_ascii=False)
    await set_config(tenant_id, CONFIG_NAMESPACE, CONFIG_KEY, value_str, "json", "proxy_pool 代理池配置")


async def run_fast_pool_update(tenant_id: str) -> dict:
    """快速更新代理池：仅对已有 Redis 全池做并发复测，每批测完立即剔除无效。若定时更新正在跑则跳过。"""
    cfg = await get_proxy_pool_config(tenant_id)
    verbose = bool(cfg.get("verbose_log"))
    log_entries: list[dict] = []

    if tenant_id in _refresh_running:
        if verbose:
            log_entries.append(log_entry("WARN", "[fast] skip: 定时更新执行中"))
        return {"skipped": True, "reason": "refresh_running", **({"log_entries": log_entries} if verbose else {})}
    if not cfg.get("fast_schedule_enabled"):
        if verbose:
            log_entries.append(log_entry("WARN", "[fast] skip: disabled"))
        return {"skipped": True, "reason": "disabled", **({"log_entries": log_entries} if verbose else {})}

    redis_url = _redis_url_from_config(cfg)
    _fast_running.add(tenant_id)
    try:
        members = await validated_proxy_smembers(redis_url=redis_url)
        pool_size = len(members)
        raw_cap = int(cfg.get("fast_concurrency") or 500)
        if raw_cap == 6:
            cap = 500
        else:
            cap = max(1, min(500, raw_cap))
        concurrency = min(pool_size, cap) if members else 1
        if not members:
            if verbose:
                log_entries.append(log_entry("INFO", "[fast] pool empty"))
            return {"skipped": False, "tested": 0, "deleted": 0, **({"log_entries": log_entries} if verbose and log_entries else {})}
        if verbose:
            log_entries.append(log_entry("INFO", f"[fast] pool size={len(members)} concurrency={concurrency}"))
        total_deleted = 0
        for i in range(0, len(members), concurrency):
            chunk = members[i : i + concurrency]
            results = await asyncio.gather(*[_test_one_proxy_full(tenant_id, p) for p in chunk])
            invalid = [r["proxy"] for r in results if not r.get("valid")]
            if invalid:
                out = await delete_invalid_proxies(invalid, base_url=None, verbose=verbose, tenant_id=tenant_id)
                deleted = len(out.get("deleted") or [])
                total_deleted += deleted
                if verbose:
                    log_entries.append(log_entry("INFO", f"[fast] chunk deleted={deleted} invalid={invalid[:5]}{'...' if len(invalid) > 5 else ''}"))
                    if out.get("log_entries"):
                        log_entries.extend(out["log_entries"])
        now_ts = int(time.time())
        await _update_fast_schedule_last_run(tenant_id, now_ts)
        if raw_cap == 6:
            await set_proxy_pool_config(tenant_id, {"fast_concurrency": 500})
        if verbose:
            log_entries.append(log_entry("INFO", f"[fast] done tested={len(members)} deleted={total_deleted} at={now_ts}"))
        ret = {"skipped": False, "tested": len(members), "deleted": total_deleted, "at": now_ts}
        if verbose and log_entries:
            ret["log_entries"] = log_entries
        return ret
    finally:
        _fast_running.discard(tenant_id)


async def delete_invalid_proxies(
    proxies: list[str],
    base_url: Optional[str] = None,
    verbose: bool = False,
    tenant_id: Optional[str] = None,
) -> dict:
    """先对每个 proxy 从 Redis 移除（SREM + DEL meta），再可选调用外部 /delete/。tenant_id 非空时用该租户配置的 redis_url。verbose 时返回 log_entries。"""
    log_entries: list[dict] = []
    if not proxies:
        return {"deleted": [], "failed": [], **({"log_entries": log_entries} if verbose else {})}

    redis_url: Optional[str] = None
    if tenant_id:
        cfg = await get_proxy_pool_config(tenant_id)
        redis_url = _redis_url_from_config(cfg)

    if verbose:
        log_entries.append(log_entry("INFO", f"[delete-invalid] requested proxies={len(proxies)}"))
    deleted: list[str] = []
    failed: list[str] = []
    for proxy in proxies:
        try:
            await validated_proxy_srem(proxy, redis_url=redis_url)
            await validated_proxy_del_meta(proxy, redis_url=redis_url)
            deleted.append(proxy)
            if verbose:
                log_entries.append(log_entry("INFO", f"  proxy={proxy} SREM+DEL ok"))
        except Exception as e:
            logger.warning("delete_invalid_proxies Redis failed for %s: %s", proxy, e)
            failed.append(proxy)
            if verbose:
                log_entries.append(log_entry("ERROR", f"  proxy={proxy} Redis failed: {type(e).__name__} {str(e)[:60]}"))

    base_url = (base_url or "").strip().rstrip("/")
    if base_url:
        if verbose:
            log_entries.append(log_entry("INFO", f"[delete-invalid] external DELETE {base_url}/delete/"))
        async with httpx.AsyncClient(timeout=5.0) as client:
            for proxy in deleted[:]:
                try:
                    r = await client.get(f"{base_url}/delete/", params={"proxy": proxy})
                    if r.status_code != 200:
                        failed.append(proxy)
                    if verbose:
                        log_entries.append(log_entry("INFO", f"  proxy={proxy} external status={r.status_code}"))
                except Exception as ex:
                    failed.append(proxy)
                    if verbose:
                        log_entries.append(log_entry("WARN", f"  proxy={proxy} external error={type(ex).__name__}"))
    if verbose:
        log_entries.append(log_entry("INFO", f"[delete-invalid] deleted={len(deleted)} failed={len(failed)}"))
    out = {"deleted": deleted, "failed": failed}
    if verbose and log_entries:
        out["log_entries"] = log_entries
    return out


async def get_proxy(
    tenant_id: str, prefer_https: bool = True, domain: Optional[str] = None
) -> Optional[str]:
    """数据拉取用代理时调用：若启用则从 Redis 本地池取一条，否则返回 None（调用方直连）。
    domain 非空时仅返回该域 valid=True 的代理；无则返回 None。prefer_https=True 时优先 https。"""
    cfg = await get_proxy_pool_config(tenant_id)
    if not cfg.get("enabled"):
        if cfg.get("verbose_log"):
            logger.info("get_proxy: disabled, return None (direct)")
        return None
    redis_url = _redis_url_from_config(cfg)
    try:
        members = await validated_proxy_smembers(redis_url=redis_url)
        if not members:
            if cfg.get("verbose_log"):
                logger.info("get_proxy: pool empty, return None (direct)")
            return None
        # domain 非空时过滤：仅保留该域 valid 的代理
        if domain and domain.strip():
            candidates = []
            for proxy in members:
                meta = await validated_proxy_get_meta(proxy, redis_url=redis_url)
                if _proxy_passes_domain(meta, domain):
                    candidates.append((proxy, meta))
            if not candidates:
                if cfg.get("verbose_log"):
                    logger.info("get_proxy: no proxy for domain=%s, return None", domain)
                return None
        else:
            # domain 为空：保持原逻辑
            if prefer_https:
                random.shuffle(members)
                for p in members:
                    meta = await validated_proxy_get_meta(p, redis_url=redis_url)
                    proto = (meta.get("protocol") or "").strip().lower()
                    if proto == "https":
                        if cfg.get("verbose_log"):
                            logger.info("get_proxy: redis_ok=True proxy=%s (https)", p, extra={"proxy": p})
                        return p
                for p in members:
                    meta = await validated_proxy_get_meta(p, redis_url=redis_url)
                    proto = (meta.get("protocol") or "").strip().lower()
                    if proto == "http":
                        if cfg.get("verbose_log"):
                            logger.warning("get_proxy: no HTTPS proxy, using HTTP-only proxy=%s", p)
                        return p
            proxies = await validated_proxy_srandmember(1, redis_url=redis_url)
            proxy = proxies[0] if proxies else None
            if cfg.get("verbose_log") and proxy:
                logger.info("get_proxy: redis_ok=True proxy=%s", proxy, extra={"proxy": proxy})
            return proxy
        # domain 非空：从 candidates 中按 prefer_https 选
        if prefer_https:
            random.shuffle(candidates)
            for proxy, meta in candidates:
                p = (meta.get("protocol") or "").strip().lower()
                if p == "https":
                    if cfg.get("verbose_log"):
                        logger.info("get_proxy: redis_ok=True proxy=%s (https) domain=%s", proxy, domain, extra={"proxy": proxy})
                    return proxy
            for proxy, meta in candidates:
                p = (meta.get("protocol") or "").strip().lower()
                if p == "http":
                    if cfg.get("verbose_log"):
                        logger.warning("get_proxy: no HTTPS for domain=%s, using HTTP proxy=%s", domain, proxy)
                    return proxy
        proxy = random.choice(candidates)[0]
        if cfg.get("verbose_log"):
            logger.info("get_proxy: redis_ok=True proxy=%s domain=%s", proxy, domain, extra={"proxy": proxy})
        return proxy
    except Exception as e:
        if cfg.get("verbose_log"):
            logger.warning("get_proxy: redis failed %s, direct", e)
        return None


async def get_proxy_pool_available_count(tenant_id: str, domain: Optional[str] = None) -> int:
    """返回本地已校验代理池当前可用数量，供动态并发计算 N 使用。未启用或 Redis 不可达时返回 0。
    domain 为空时返回全池总数；domain 非空时返回该域 valid=True 的代理数（与 get_proxies_top_pct 过滤逻辑一致）。"""
    status = await fetch_status_from_redis(tenant_id, verbose=False)
    if not status.get("reachable"):
        return 0
    if not domain or not domain.strip():
        return int(status.get("count", 0) or 0)
    proxies_list = status.get("proxies_list") or []
    filtered = [x for x in proxies_list if _proxy_item_valid_for_domain(x, domain)]
    return len(filtered)


async def get_proxies(
    tenant_id: str, n: int, prefer_https: bool = True, domain: Optional[str] = None
) -> list[str]:
    """取最多 n 个不重复代理供任务内多 worker 使用。domain 非空时仅返回该域 valid 的代理。n<=0 返回 []。"""
    if n <= 0:
        return []
    cfg = await get_proxy_pool_config(tenant_id)
    if not cfg.get("enabled"):
        return []
    redis_url = _redis_url_from_config(cfg)
    try:
        if domain and domain.strip():
            members = await validated_proxy_smembers(redis_url=redis_url)
            filtered = []
            for p in members:
                meta = await validated_proxy_get_meta(p, redis_url=redis_url)
                if _proxy_passes_domain(meta, domain):
                    filtered.append(p)
            proxies = filtered[:n]
        else:
            proxies = await validated_proxy_srandmember(n, redis_url=redis_url)
        if not proxies:
            return []
        if prefer_https:
            https_list = []
            http_list = []
            for p in proxies:
                meta = await validated_proxy_get_meta(p, redis_url=redis_url)
                p_lower = (meta.get("protocol") or "").strip().lower()
                if p_lower == "https":
                    https_list.append(p)
                else:
                    http_list.append(p)
            proxies = https_list + http_list
        return list(proxies)
    except Exception as e:
        logger.warning("get_proxies failed: %s", e)
        return []


async def get_proxies_top_pct(
    tenant_id: str,
    pct: float = 0.6,
    prefer_https: bool = True,
    domain: Optional[str] = None,
) -> tuple[list[str], list[str]]:
    """按延迟取「前 pct 在用池 / 剩余为备用池」。domain 非空时仅保留该域 valid 的代理。
    返回 (active_list, reserve_list)；池不可达或为空时返回 ([], [])。"""
    status = await fetch_status_from_redis(tenant_id, verbose=False)
    if not status.get("reachable"):
        return ([], [])
    proxies_list = status.get("proxies_list") or []
    if not proxies_list:
        return ([], [])
    if domain and domain.strip():
        filtered = []
        for x in proxies_list:
            dr = x.get("domain_results") or []
            if isinstance(dr, str):
                try:
                    dr = json.loads(dr) if dr else []
                except (TypeError, ValueError, json.JSONDecodeError):
                    dr = []
            if not isinstance(dr, list):
                continue
            for d in dr:
                if d.get("label") == domain and d.get("valid") is True:
                    filtered.append(x)
                    break
        proxies_list = filtered
        if not proxies_list:
            return ([], [])
    if prefer_https:
        https_items = [x for x in proxies_list if (x.get("protocol") or "").strip().lower() == "https"]
        http_items = [x for x in proxies_list if (x.get("protocol") or "").strip().lower() != "https"]
        proxies_list = https_items + http_items
    n_active = max(1, int(len(proxies_list) * pct))
    active = [x["proxy"] for x in proxies_list[:n_active]]
    reserve = [x["proxy"] for x in proxies_list[n_active:]]
    return (active, reserve)


async def clear_proxy_pool(tenant_id: str) -> dict:
    """清空本地已校验代理池（validated_proxy:set 及全部 meta）。返回 {"cleared": 删除的代理数}。"""
    cfg = await get_proxy_pool_config(tenant_id)
    redis_url = _redis_url_from_config(cfg)
    count = await clear_validated_proxy_pool(redis_url=redis_url)
    logger.info("proxy_pool clear: tenant_id=%s cleared=%s", tenant_id, count)
    return {"cleared": count}


async def remove_proxy(tenant_id: str, proxy: str) -> None:
    """数据拉取请求失败时调用：从 Redis 移除该代理；可选调用外部 /delete/。verbose_log 时打详细日志。"""
    cfg = await get_proxy_pool_config(tenant_id)
    redis_url = _redis_url_from_config(cfg)
    try:
        await validated_proxy_srem(proxy, redis_url=redis_url)
        await validated_proxy_del_meta(proxy, redis_url=redis_url)
        if cfg.get("verbose_log"):
            logger.info("remove_proxy: proxy=%s SREM+DEL ok", proxy)
    except Exception as e:
        logger.warning("remove_proxy failed for %s: %s", proxy, e)
        if cfg.get("verbose_log"):
            logger.warning("remove_proxy: proxy=%s Redis failed %s", proxy, e)
    base_url = (cfg.get("url") or "").strip().rstrip("/")
    if base_url:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get(f"{base_url}/delete/", params={"proxy": proxy})
            if cfg.get("verbose_log"):
                logger.info("remove_proxy: proxy=%s external delete status=%s", proxy, r.status_code)
        except Exception as ex:
            if cfg.get("verbose_log"):
                logger.warning("remove_proxy: proxy=%s external delete failed %s", proxy, ex)
