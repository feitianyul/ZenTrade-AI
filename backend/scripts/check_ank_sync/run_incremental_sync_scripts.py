#!/usr/bin/env python3
"""
主控脚本：按固定顺序串行执行增量同步脚本，支持按更新频率分类运行（日更/季更/半年更），便于定时任务与限流。

更新频率说明：
- daily（日更）：每个交易日或收盘后更新（K 线、涨跌停、板块、北向、资金流向、龙虎榜、大宗、融资融券、资讯、同行比较等）
- weekly（周更）：数据变化不频繁，每周跑一次即可（交易所日历、A股列表）
- quarterly（季更）：季报披露后更新（股东户数、财务指标、分红配股等）
- semi_annual（半年更）：半年报/年报报告期更新（十大股东等）

用法（在 backend 目录下）：
  跑全部（保持原行为）：
    PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py
  仅跑日更（适合每日定时任务）：
    PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py --frequency daily
  仅跑周更（交易所日历、A股列表）：
    PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py --frequency weekly
  仅跑季更（适合每季报后跑一次）：
    PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py --frequency quarterly
  仅跑半年更（适合半年报/年报后跑一次）：
    PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py --frequency semi_annual
  干跑与并发：
    PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py --dry-run -j 3
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# 脚本所在目录与 backend 根目录
_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _SCRIPT_DIR.parent.parent
_LOG_DIR = _BACKEND_DIR / "logs" / "check_ank_sync"

# 频率枚举：daily=日更, weekly=周更, quarterly=季更, semi_annual=半年更
FREQ_DAILY = "daily"
FREQ_WEEKLY = "weekly"
FREQ_QUARTERLY = "quarterly"
FREQ_SEMI_ANNUAL = "semi_annual"
FREQ_ALL = "all"


def _script_to_category_id(script_name: str) -> str:
    """check_and_sync_kline_latest.py -> kline"""
    s = script_name.replace("check_and_sync_", "").replace("_latest.py", "")
    return s


# (标签, 脚本名, 是否支持 -j, 更新频率)
# 顺序：日更 → 周更 → 季更 → 半年更
_SCRIPTS = [
    # ---------- 日更：每个交易日或收盘后（行情/资金等） ----------
    ("K线行情", "check_and_sync_kline_latest.py", True, FREQ_DAILY),
    ("涨跌停", "check_and_sync_limit_updown_latest.py", False, FREQ_DAILY),
    ("行业/概念板块", "check_and_sync_sector_latest.py", True, FREQ_DAILY),
    ("北向资金", "check_and_sync_northbound_latest.py", True, FREQ_DAILY),
    ("北向持股排行", "check_and_sync_northbound_hold_latest.py", True, FREQ_DAILY),
    ("资金流向", "check_and_sync_capital_flow_latest.py", True, FREQ_DAILY),
    ("龙虎榜", "check_and_sync_lhb_latest.py", True, FREQ_DAILY),
    ("大宗交易", "check_and_sync_block_trade_latest.py", True, FREQ_DAILY),
    ("融资融券", "check_and_sync_margin_latest.py", False, FREQ_DAILY),
    ("资讯/公告", "check_and_sync_news_latest.py", True, FREQ_DAILY),
    ("互动易问答", "check_and_sync_irm_latest.py", True, FREQ_DAILY),
    ("同行比较", "check_and_sync_peer_comparison_latest.py", True, FREQ_DAILY),
    # ---------- 周更：数据变化不频繁，每周跑一次即可 ----------
    ("交易所日历", "check_and_sync_trade_calendar_latest.py", False, FREQ_WEEKLY),
    ("A股列表", "check_and_sync_stock_list_latest.py", False, FREQ_WEEKLY),
    # ---------- 季更：季报披露后 ----------
    ("分红配股", "check_and_sync_dividend_latest.py", True, FREQ_QUARTERLY),
    ("股东户数", "check_and_sync_holder_count_latest.py", True, FREQ_QUARTERLY),
    ("财务指标", "check_and_sync_financial_latest.py", True, FREQ_QUARTERLY),
    # ---------- 半年更：半年报/年报报告期 ----------
    ("十大股东", "check_and_sync_top_holder_latest.py", True, FREQ_SEMI_ANNUAL),
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="按频率串行执行增量同步脚本（daily/weekly/quarterly/semi_annual/all），脚本间无并发。"
    )
    parser.add_argument("--dry-run", action="store_true", help="向各子脚本传递 --dry-run，仅检查不落库")
    parser.add_argument("-j", "--jobs", type=int, default=3, help="向支持 -j 的子脚本传递的并发数，默认 3")
    parser.add_argument(
        "--frequency",
        choices=[FREQ_DAILY, FREQ_WEEKLY, FREQ_QUARTERLY, FREQ_SEMI_ANNUAL, FREQ_ALL],
        default=FREQ_ALL,
        help="仅运行该频率的脚本：daily=日更, weekly=周更, quarterly=季更, semi_annual=半年更, all=全部（默认）",
    )
    args = parser.parse_args()

    dry_run = args.dry_run
    jobs = args.jobs
    frequency = args.frequency
    started_at = datetime.now().isoformat()

    # 按频率过滤（all 则不过滤）
    if frequency == FREQ_ALL:
        scripts_to_run = _SCRIPTS
    else:
        scripts_to_run = [t for t in _SCRIPTS if t[3] == frequency]

    total = len(scripts_to_run)
    print(f"[run_incremental_sync] 频率={frequency}，共 {total} 个脚本（dry_run={dry_run}, jobs={jobs}）")
    print(f"[run_incremental_sync] 工作目录: {_BACKEND_DIR}")
    print()

    env = dict(__import__("os").environ)
    env["PYTHONPATH"] = str(_BACKEND_DIR)

    failed: list[tuple[str, str]] = []
    for i, (label, script_name, supports_j, _freq) in enumerate(scripts_to_run, start=1):
        script_path = _SCRIPT_DIR / script_name
        if not script_path.exists():
            print(f"[{i}/{total}] {label} 跳过：脚本不存在 {script_name}")
            failed.append((label, "脚本不存在"))
            continue

        cmd = [sys.executable, str(script_path)]
        if dry_run:
            cmd.append("--dry-run")
        if supports_j:
            cmd.extend(["-j", str(jobs)])
        # margin 等使用 -j 时可能用 --concurrency，这里统一用 -j，未实现的脚本会忽略
        print(f"[{i}/{total}] {label} [{_freq}] 执行: {' '.join(cmd)}")
        try:
            ret = subprocess.run(
                cmd,
                cwd=str(_BACKEND_DIR),
                env=env,
            )
            if ret.returncode != 0:
                failed.append((label, f"exit code {ret.returncode}"))
                print(f"[{i}/{total}] {label} 失败: exit code {ret.returncode}")
            else:
                print(f"[{i}/{total}] {label} 完成")
        except Exception as e:
            failed.append((label, str(e)))
            print(f"[{i}/{total}] {label} 异常: {e}")
        print()

    # 汇总各脚本的 last_result.json，生成 run_summary_<时间>.json 与 .log
    summary_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    script_results: list[dict] = []
    total_success = 0
    total_failed = 0
    total_empty = 0
    for label, script_name, _, _freq in scripts_to_run:
        cid = _script_to_category_id(script_name)
        result_path = _LOG_DIR / f"{cid}_last_result.json"
        entry = {"label": label, "category_id": cid, "script": script_name, "frequency": _freq}
        if result_path.exists():
            try:
                data = json.loads(result_path.read_text(encoding="utf-8"))
                entry["success"] = data.get("success", 0)
                entry["failed"] = data.get("failed", 0)
                entry["empty"] = data.get("empty", 0)
                entry["ts"] = data.get("ts", "")
                total_success += entry["success"]
                total_failed += entry["failed"]
                total_empty += entry["empty"]
            except Exception:
                entry["success"] = entry["failed"] = entry["empty"] = 0
                entry["error"] = "读取结果失败"
        else:
            entry["success"] = entry["failed"] = entry["empty"] = 0
            entry["note"] = "未运行或未写入结果"
        script_results.append(entry)

    summary = {
        "started_at": started_at,
        "finished_at": datetime.now().isoformat(),
        "frequency": frequency,
        "dry_run": dry_run,
        "scripts": script_results,
        "total_success": total_success,
        "total_failed": total_failed,
        "total_empty": total_empty,
        "run_failed": [(label, err) for label, err in failed],
    }
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    summary_json_path = _LOG_DIR / f"run_summary_{summary_ts}.json"
    summary_log_path = _LOG_DIR / f"run_summary_{summary_ts}.log"
    summary_json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        f"# 增量同步汇总 {summary_ts}",
        f"频率: {frequency}",
        f"开始: {summary['started_at']}",
        f"结束: {summary['finished_at']}",
        f"dry_run: {dry_run}",
        "",
        "各脚本:",
    ]
    for s in script_results:
        lines.append(f"  [{s.get('frequency', '')}] {s['label']} ({s['category_id']}): 成功={s['success']} 失败={s['failed']} 空={s['empty']}")
    lines.extend([
        "",
        f"合计: 成功={total_success} 失败={total_failed} 空={total_empty}",
    ])
    if failed:
        lines.append("")
        lines.append("执行失败:")
        for label, err in failed:
            lines.append(f"  - {label}: {err}")
    summary_log_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[run_incremental_sync] 汇总已写入: {summary_json_path}")
    print(f"[run_incremental_sync] 汇总已写入: {summary_log_path}")

    if failed:
        print("[run_incremental_sync] 结束，存在失败:")
        for label, err in failed:
            print(f"  - {label}: {err}")
        sys.exit(1)
    print(f"[run_incremental_sync] 全部 {total} 个脚本执行完成。")


if __name__ == "__main__":
    main()
