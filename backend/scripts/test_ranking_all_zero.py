"""
排行数据「是否全部为 0」对比测试：分别从 API 与 AKShare 拉取，对比结果。

用法（在 backend 目录或项目根）：
  python backend/scripts/test_ranking_all_zero.py
  cd backend && python scripts/test_ranking_all_zero.py

环境变量（可选）：
  BASE_URL           默认 http://127.0.0.1:8000（不设或空则跳过 API 拉取）
  LOGIN_PHONE / LOGIN_PASSWORD  登录账号，API 拉取用
"""
import os
import sys

# 加载 .env
def _load_dotenv():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for d in [script_dir, os.path.join(script_dir, "..")]:
        env_path = os.path.join(d, ".env")
        if os.path.isfile(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip('"').strip("'")
                        if k and k not in os.environ:
                            os.environ[k] = v
            return


_load_dotenv()

BASE_URL = (os.getenv("BASE_URL") or "").strip().rstrip("/")
LOGIN_PHONE = os.getenv("LOGIN_PHONE", "13800001111")
LOGIN_PASSWORD = os.getenv("LOGIN_PASSWORD", "Test1234")

# 用于判断「是否全部为 0」的数值字段（与后端 get_ranking 返回结构一致）
NUMERIC_KEYS = ("price", "change_pct", "change_amt", "volume", "turnover", "turnover_rate")


def _is_all_zero_list(items: list, numeric_keys: tuple = NUMERIC_KEYS) -> tuple[bool, int, list]:
    """
    检查列表里每条记录的数值字段是否全为 0。
    返回: (是否全部为0, 总条数, 前3条样本用于打印)
    """
    if not items:
        return True, 0, []
    sample = items[:3]
    all_zero = True
    for item in items:
        if not isinstance(item, dict):
            continue
        for k in numeric_keys:
            val = item.get(k)
            if val is None:
                continue
            try:
                if float(val) != 0:
                    all_zero = False
                    break
            except (TypeError, ValueError):
                continue
        if not all_zero:
            break
    return all_zero, len(items), sample


def _print_result(source: str, all_zero: bool, count: int, sample: list):
    """统一打印：来源、是否全0、条数、样本。"""
    status = "全部为 0" if all_zero else "有有效数据（非全 0）"
    print(f"  [{source}] 条数={count} -> {status}")
    for i, row in enumerate(sample, 1):
        if isinstance(row, dict):
            nums = {k: row.get(k) for k in NUMERIC_KEYS if k in row}
            print(f"    样本{i}: {row.get('name','')} {row.get('symbol','')} | {nums}")


# ---------------------------------------------------------------------------
# 1) 从 API 拉取排行（GET /market/ranking）
# ---------------------------------------------------------------------------
def fetch_ranking_from_api() -> tuple[bool, int, list]:
    """从后端 API 拉取涨幅榜，返回 (是否全部为0, 条数, 前3条样本)。"""
    if not BASE_URL:
        print("  (未设置 BASE_URL，跳过 API 拉取)")
        return True, 0, []
    try:
        import requests
        r = requests.post(
            f"{BASE_URL}/auth/login",
            json={"phone": LOGIN_PHONE, "password": LOGIN_PASSWORD},
            timeout=10,
        )
        body = r.json()
        token = (body.get("data") or {}).get("token")
        if not token:
            print("  登录失败，跳过 API 拉取")
            return True, 0, []
        headers = {"Authorization": f"Bearer {token}"}
        r2 = requests.get(
            f"{BASE_URL}/market/ranking?sort_by=change_pct&order=desc&limit=30",
            headers=headers,
            timeout=30,
        )
        data = r2.json()
        items = data.get("data") if isinstance(data, dict) else None
        if not isinstance(items, list):
            print("  API 返回格式异常，无 data 列表")
            return True, 0, []
        all_zero, count, sample = _is_all_zero_list(items)
        return all_zero, count, sample
    except Exception as e:
        print(f"  API 请求异常: {e}")
        return True, 0, []


# ---------------------------------------------------------------------------
# 2) 直接从 AKShare 拉取（与后端同源）
# ---------------------------------------------------------------------------
def fetch_ranking_from_akshare() -> tuple[bool, int, list]:
    """直接调用 ak.stock_zh_a_spot_em，按涨跌幅取前 30，转成与 API 相同结构，返回 (是否全部为0, 条数, 样本)。"""
    try:
        import akshare as ak
    except ImportError:
        print("  (未安装 akshare，跳过 AKShare 直连拉取)")
        return True, 0, []

    try:
        df = ak.stock_zh_a_spot_em()
        if df is None or len(df) == 0:
            print("  AKShare 返回空 DataFrame")
            return True, 0, []
        df = df.sort_values(by="涨跌幅", ascending=False).head(30)
        items = []
        for _, row in df.iterrows():
            items.append({
                "symbol": str(row.get("代码", "")),
                "name": str(row.get("名称", "")),
                "price": float(row.get("最新价", 0) or 0),
                "change_pct": float(row.get("涨跌幅", 0) or 0),
                "change_amt": float(row.get("涨跌额", 0) or 0),
                "volume": int(row.get("成交量", 0) or 0),
                "turnover": int(row.get("成交额", 0) or 0),
                "turnover_rate": float(row.get("换手率", 0) or 0),
            })
        all_zero, count, sample = _is_all_zero_list(items)
        return all_zero, count, sample
    except Exception as e:
        print(f"  AKShare 请求异常: {e}")
        return True, 0, []


def main():
    print("=" * 60)
    print("排行数据「是否全部为 0」对比测试")
    print("=" * 60)

    # 1) API
    print("\n--- 1) 从后端 API 拉取 (GET /market/ranking?sort_by=change_pct&order=desc&limit=30) ---")
    api_all_zero, api_count, api_sample = fetch_ranking_from_api()
    _print_result("API", api_all_zero, api_count, api_sample)

    # 2) AKShare 直连
    print("\n--- 2) 从 AKShare 直连拉取 (ak.stock_zh_a_spot_em，按涨跌幅前30) ---")
    ak_all_zero, ak_count, ak_sample = fetch_ranking_from_akshare()
    _print_result("AKShare", ak_all_zero, ak_count, ak_sample)

    # 3) 对比结论
    print("\n--- 3) 对比结论 ---")
    if api_count == 0 and ak_count == 0:
        print("  两端均无数据，请检查网络、登录或 akshare 安装。")
    elif api_count > 0 and ak_count > 0:
        if api_all_zero and not ak_all_zero:
            print("  API 返回全部为 0，AKShare 有有效数据 -> 问题在后端（缓存/兜底返回了全 0）。")
        elif not api_all_zero and ak_all_zero:
            print("  API 有有效数据，AKShare 直连全 0 -> 异常（AKShare 直连与后端逻辑不一致）。")
        elif api_all_zero and ak_all_zero:
            print("  两端均为 0 -> 可能非交易时间东方财富无行情，或数据源返回全 0。")
        else:
            print("  两端均有有效数据 -> 正常。")
    elif api_count > 0:
        print("  仅 API 有数据；AKShare 未拉取到。")
    else:
        print("  仅 AKShare 有数据；API 未拉取到或登录失败。")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
