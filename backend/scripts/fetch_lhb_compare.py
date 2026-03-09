#!/usr/bin/env python3
"""直接调用 akshare 拉取龙虎榜，与 data_sync sync_lhb 使用相同日期逻辑，便于对比报错。

用法（在 backend 目录下）:
  python scripts/fetch_lhb_compare.py           # 默认：增量区间（近 5 天）
  python scripts/fetch_lhb_compare.py full     # 全量区间（近 30 天）
  python scripts/fetch_lhb_compare.py 20260214 20260216  # 指定 start end

与 Worker 报错对比：
- 若 Worker 日志只显示 '上榜日期'，多为 KeyError('上榜日期')，即接口返回的列名与预期不一致。
- 本脚本会打印实际列名、并复现 sync_lhb 中对 df["上榜日期"] 的访问，便于对比完整异常信息。
"""
import os
import sys
import traceback
from datetime import datetime, timedelta

# 与 sync 一致：不走代理，避免干扰
for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(k, None)

# sync_lhb 期望的列名（data_sync_service.sync_lhb）
SYNC_EXPECTED_COLUMNS = [
    "代码", "名称", "上榜日期", "解读", "上榜原因", "收盘价", "涨跌幅",
    "龙虎榜净买额", "龙虎榜买入额", "龙虎榜卖出额", "成交额",
]


def _reproduce_sync_lhb_access(df, watermark: str = None):
    """与 sync_lhb 完全一致地访问 df：访问 df['上榜日期'] 与 row.get('上榜日期')。"""
    if df is None or df.empty:
        return
    # 复现 data_sync_service.sync_lhb 第 1264 行（增量时）或 1278 行（遍历 row）
    if watermark:
        df = df.copy()
        df["_dt"] = df["上榜日期"].astype(str).str.replace("-", "")
        df = df[df["_dt"] > watermark].drop(columns=["_dt"], errors="ignore")
    else:
        # 仅访问「上榜日期」列，与 sync 首次使用处一致
        _ = df["上榜日期"].astype(str).str.replace("-", "")
    for _, row in df.head(1).iterrows():
        _ = str(row.get("上榜日期", ""))


def main():
    if len(sys.argv) >= 3 and sys.argv[1].isdigit() and sys.argv[2].isdigit():
        start = sys.argv[1]
        end = sys.argv[2]
        print(f"使用指定日期: start_date={start}, end_date={end}")
    elif len(sys.argv) >= 2 and sys.argv[1].lower() == "full":
        start = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")
        print(f"全量区间(近30天): start_date={start}, end_date={end}")
    else:
        start = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")
        print(f"增量区间(近5天): start_date={start}, end_date={end}")

    import akshare as ak

    print("\n--- 1. 调用 akshare ---")
    print("ak.stock_lhb_detail_em(start_date=%r, end_date=%r)" % (start, end))
    df = None
    try:
        df = ak.stock_lhb_detail_em(start_date=start, end_date=end)
    except TypeError as e:
        print("异常: TypeError:", e)
        if "NoneType" in str(e) and "not subscriptable" in str(e):
            print("(与 sync_lhb 中东方财富 result=null 时一致，sync 已按「无数据」处理)")
        traceback.print_exc()
        return
    except Exception as e:
        print("异常: %s: %s" % (type(e).__name__, e))
        traceback.print_exc()
        return

    if df is None:
        print("结果: 返回 None（接口无数据或 result=null）")
        return
    if df.empty:
        print("结果: 空 DataFrame，行数=0")
        return

    print("结果: 成功，行数:", len(df))
    print("列名:", list(df.columns))

    # 列名对比：sync_lhb 依赖「上榜日期」
    missing = [c for c in SYNC_EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        print("\n⚠ sync_lhb 期望但当前 API 缺失的列:", missing)
        print("  （若缺「上榜日期」，Worker 会报 KeyError，日志中可能只显示 '上榜日期'）")
    if "上榜日期" in df.columns:
        print("上榜日期样例:", df["上榜日期"].head(3).tolist())
    else:
        # 常见变体：文档提到可能为「上榜日」
        alt = "上榜日" if "上榜日" in df.columns else None
        if alt:
            print("上榜日期 缺失；存在替代列「上榜日」:", df[alt].head(3).tolist())

    print("\n前 3 行:")
    print(df.head(3).to_string())

    # 复现 sync_lhb 对 df["上榜日期"] 的访问，便于对比报错
    print("\n--- 2. 复现 sync_lhb 对「上榜日期」的访问 ---")
    try:
        _reproduce_sync_lhb_access(df, watermark=None)
        print("OK：与 sync_lhb 相同访问未报错。")
    except KeyError as e:
        print("KeyError（与 Worker 报错一致）: %s" % e)
        print("说明: sync_lhb 使用 df['上榜日期']，当前 API 返回列名不含该键。")
        traceback.print_exc()
    except Exception as e:
        print("异常: %s: %s" % (type(e).__name__, e))
        traceback.print_exc()


if __name__ == "__main__":
    main()
