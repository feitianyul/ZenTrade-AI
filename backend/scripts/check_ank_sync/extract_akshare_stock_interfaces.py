#!/usr/bin/env python3
"""
从 dev_doc/akshare 提取所有「接口: stock_」行号、接口名、所在章节；
与现有实现对照标注已拉/未拉，输出全量接口清单文档（Phase 1）。
"""
import re
import json
from pathlib import Path

# 已拉：接口名 -> (落库表, 脚本名或调用处)
PULLED = {
    "stock_info_sh_name_code": ("stock_info", "check_and_sync_stock_list_latest.py"),
    "stock_info_a_code_name": ("stock_info", "check_and_sync_stock_list_latest.py"),
    "stock_zh_a_hist": ("kline_storage", "check_and_sync_kline_latest.py"),
    "stock_zh_a_hist_tx": ("kline_storage", "check_and_sync_kline_latest.py"),
    "stock_financial_analysis_indicator": ("stock_financial", "check_and_sync_financial_latest.py"),
    "stock_financial_analysis_indicator_em": ("stock_financial", "market_source F10/东财主要指标"),
    "stock_financial_abstract": ("-", "market_source F10 兜底"),
    "stock_margin_detail_sse": ("stock_margin_trading", "check_and_sync_margin_latest.py"),
    "stock_margin_detail_szse": ("stock_margin_trading", "check_and_sync_margin_latest.py"),
    "stock_dzjy_mrmx": ("stock_block_trade", "check_and_sync_block_trade_latest.py"),
    "stock_individual_fund_flow": ("stock_capital_flow", "check_and_sync_capital_flow_latest.py"),
    "stock_gdfx_free_top_10_em": ("stock_top_holders", "check_and_sync_top_holder_latest.py"),
    "stock_fhps_em": ("stock_dividends", "check_and_sync_dividend_latest.py"),
    "stock_board_industry_name_em": ("stock_sectors", "check_and_sync_sector_latest.py"),
    "stock_board_concept_name_em": ("stock_sectors", "check_and_sync_sector_latest.py"),
    "stock_board_industry_cons_em": ("stock_sector_members", "check_and_sync_sector_latest.py"),
    "stock_board_concept_cons_em": ("stock_sector_members", "check_and_sync_sector_latest.py"),
    "stock_lhb_detail_em": ("stock_lhb", "check_and_sync_lhb_latest.py"),
    "stock_hsgt_hist_em": ("northbound_flow", "check_and_sync_northbound_latest.py"),
    "stock_hsgt_hold_stock_em": ("northbound_hold_stock", "check_and_sync_northbound_hold_latest.py"),
    "stock_zt_pool_em": ("stock_limit_updown", "check_and_sync_limit_updown_latest.py"),
    "stock_hold_num_cninfo": ("stock_holder_count", "check_and_sync_holder_count_latest.py"),
    "stock_zh_growth_comparison_em": ("stock_peer_comparison", "check_and_sync_peer_comparison_latest.py"),
    "stock_zh_valuation_comparison_em": ("stock_peer_comparison", "check_and_sync_peer_comparison_latest.py"),
    "stock_zh_dupont_comparison_em": ("stock_peer_comparison", "check_and_sync_peer_comparison_latest.py"),
    "stock_zh_scale_comparison_em": ("stock_peer_comparison", "check_and_sync_peer_comparison_latest.py"),
    "stock_irm_cninfo": ("stock_irm_qa", "check_and_sync_irm_latest.py"),
    "stock_sns_sseinfo": ("stock_irm_qa", "check_and_sync_irm_latest.py"),
    "stock_news_em": ("stock_news", "check_and_sync_news_latest.py"),
    "stock_individual_info_em": ("-", "market_source fetch_fundamental"),
    "stock_zh_a_spot_em": ("-", "market_source 行情/人气"),
    "stock_hot_rank_em": ("-", "market_source 人气榜探源，未落库"),
}


def extract_interfaces(doc_path: Path) -> list[dict]:
    text = doc_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    interfaces = []
    for i, line in enumerate(lines, 1):
        m = re.match(r"^接口:\s*(stock_\S+)", line.strip())
        if m:
            interfaces.append((i, m.group(1)))
    sections = []
    for i, line in enumerate(lines, 1):
        s = line.strip()
        if s.startswith("### ") or s.startswith("#### "):
            sections.append((i, s))

    def section_for(line_no):
        last = None
        for ln, title in sections:
            if ln <= line_no:
                last = (ln, title)
            else:
                break
        return last[1] if last else ""

    seen = set()
    unique = []
    for line_no, name in interfaces:
        if name not in seen:
            seen.add(name)
            unique.append({"line": line_no, "interface": name, "section": section_for(line_no)})
    return unique


def main():
    backend_dir = Path(__file__).resolve().parent.parent.parent
    repo = backend_dir.parent
    doc_path = repo / "dev_doc" / "akshare"
    unique = extract_interfaces(doc_path)

    raw_path = repo / "docs" / "plans" / "akshare_stock_interfaces_raw.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(json.dumps(unique, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(unique)} unique interfaces to {raw_path}")

    # 生成正式清单 Markdown
    md_lines = [
        "# akshare 全量 stock_* 接口清单（已拉/未拉）",
        "",
        "依据：`dev_doc/akshare` 中所有「接口: stock_」提取；已拉标注来自 `backend/scripts/check_ank_sync` 与 `market_source_service`、`dev_doc/akshare接口与现有对接规划.md`。",
        "",
        "| 序号 | 接口名 | 章节 | 已拉 | 落库表 | 脚本/调用处 |",
        "|------|--------|------|------|--------|-------------|",
    ]
    for idx, item in enumerate(unique, 1):
        name = item["interface"]
        sec = item["section"].replace("|", "\\|")[:60]
        if name in PULLED:
            table, script = PULLED[name]
            md_lines.append(f"| {idx} | {name} | {sec} | 是 | {table} | {script} |")
        else:
            md_lines.append(f"| {idx} | {name} | {sec} | 否 | - | - |")
    md_lines.extend([
        "",
        "## 统计",
        "",
        f"- 全量唯一接口数：{len(unique)}",
        f"- 已拉（落库或实时调用）：{sum(1 for u in unique if u['interface'] in PULLED)}",
        f"- 未拉：{sum(1 for u in unique if u['interface'] not in PULLED)}",
        "",
        "## 说明",
        "",
        "- 已拉「落库表」为 `-` 表示仅实时调用、未单独落库（如 market_source 的 F10/行情）。",
        "- 后续 Phase 2 对「未拉」接口做原始拉取并落库后，再按主题对比与合并。",
    ])
    inv_path = repo / "docs" / "plans" / "akshare_stock_interfaces_inventory.md"
    inv_path.write_text("\n".join(md_lines), encoding="utf-8")
    print(f"Wrote inventory to {inv_path}")


if __name__ == "__main__":
    main()
