#!/usr/bin/env python3
"""统计 proxy_pool 池内总数与「业务目标」测试后的有效 IP 数，结果打印并可选写入日志文件。

用法：
  cd backend
  python scripts/check_proxy_pool_valid_count.py
  python scripts/check_proxy_pool_valid_count.py --url http://127.0.0.1:5010 --limit 50
  python scripts/check_proxy_pool_valid_count.py --log-file logs/proxy_valid_count.log
"""
import argparse
import json
import sys
import time
from pathlib import Path

try:
    import httpx
except ImportError:
    print("需要 httpx: pip install httpx", file=sys.stderr)
    sys.exit(1)

TEST_URL = "http://httpbin.org/get"
TEST_TIMEOUT = 10.0


def test_one_proxy(proxy: str) -> tuple[bool, int, str | None]:
    """返回 (有效, 延迟ms, 错误信息)。"""
    proxy_url = f"http://{proxy}"
    t0 = time.perf_counter()
    try:
        with httpx.Client(proxy=proxy_url, timeout=TEST_TIMEOUT) as client:
            r = client.get(TEST_URL)
            ok = r.status_code == 200
    except Exception as e:
        ok = False
        err = str(e)
    else:
        err = None
    latency_ms = int((time.perf_counter() - t0) * 1000)
    return ok, latency_ms, err


def main() -> int:
    parser = argparse.ArgumentParser(description="统计 proxy_pool 池内总数与业务目标有效数")
    parser.add_argument("--url", default="http://127.0.0.1:5010", help="proxy_pool API 根地址")
    parser.add_argument("--limit", type=int, default=50, help="最多测试多少个代理（默认 50）")
    parser.add_argument("--log-file", default="", help="追加写入的日志文件路径（相对 backend 或绝对）")
    args = parser.parse_args()
    base = args.url.rstrip("/")

    # 1. 池内总数
    try:
        r = httpx.get(f"{base}/count/", timeout=8.0)
        r.raise_for_status()
        data = r.json()
        pool_total = data.get("count", 0)
        http_type = data.get("http_type") or {}
    except Exception as e:
        print(f"[ERROR] 获取 /count/ 失败: {e}")
        return 1

    # 2. 拉取 /all/ 取前 limit 个
    try:
        r2 = httpx.get(f"{base}/all/", timeout=8.0)
        r2.raise_for_status()
        all_list = r2.json()
    except Exception as e:
        print(f"[ERROR] 获取 /all/ 失败: {e}")
        return 1

    if not isinstance(all_list, list):
        print("[ERROR] /all/ 返回非列表")
        return 1

    proxies = []
    for item in all_list[: args.limit * 2]:
        if isinstance(item, dict) and item.get("proxy"):
            proxies.append(item["proxy"])
        elif isinstance(item, str) and ":" in item:
            proxies.append(item)
        if len(proxies) >= args.limit:
            break

    # 3. 逐条业务目标测试
    valid_count = 0
    invalid_count = 0
    for p in proxies:
        ok, latency_ms, err = test_one_proxy(p)
        if ok:
            valid_count += 1
        else:
            invalid_count += 1

    tested = len(proxies)
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"{ts} | 池内总数={pool_total} | 本次测试数={tested} | 有效={valid_count} | 无效={invalid_count}\n"
    print(line.strip())

    if args.log_file:
        log_path = Path(args.log_file)
        if not log_path.is_absolute():
            log_path = Path(__file__).resolve().parent.parent / args.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line)
        print(f"已追加写入: {log_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
