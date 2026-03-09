#!/usr/bin/env python3
"""
验证脚本：拉取个股资讯与互动易样本，确认数据源是否可用于舆情 AI Agent。
不落库，仅打印 000630 的 stock_news_em 与 stock_irm_cninfo（若存在）的列与样本。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/verify_sentiment_data_sources.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_backend_dir = Path(__file__).resolve().parent.parent.parent
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))

_env_file = Path(os.getenv("ENV_FILE", _backend_dir / ".env"))
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=True)
    except Exception:
        pass

TEST_SYMBOL = "000630"


def main() -> None:
    try:
        import akshare as ak
    except ImportError as e:
        print("ERROR: akshare 未安装:", e)
        sys.exit(1)

    print("=" * 60)
    print("Phase 0 数据验证：个股资讯 + 互动易（测试标的", TEST_SYMBOL, "）")
    print("=" * 60)

    # 1) 个股资讯 stock_news_em
    print("\n--- 1. 个股资讯 ak.stock_news_em(symbol=%s) ---" % TEST_SYMBOL)
    try:
        df = ak.stock_news_em(symbol=TEST_SYMBOL)
        if df is None or df.empty:
            print("  返回为空或 None")
        else:
            print("  列名:", list(df.columns))
            title_col = "新闻标题" if "新闻标题" in df.columns else df.columns[0]
            content_col = "新闻内容" if "新闻内容" in df.columns else (df.columns[1] if len(df.columns) > 1 else None)
            time_col = "发布时间" if "发布时间" in df.columns else (df.columns[2] if len(df.columns) > 2 else None)
            for i, (_, row) in enumerate(df.head(3).iterrows()):
                print("  [%d] 标题: %s" % (i + 1, str(row.get(title_col, ""))[:80]))
                if content_col:
                    content = str(row.get(content_col, ""))
                    print("      内容(前120字): %s" % (content[:120] if content else "(空)"))
                if time_col:
                    print("      时间: %s" % row.get(time_col, ""))
            if content_col and df[content_col].notna().any() and df[content_col].astype(str).str.strip().str.len().gt(0).any():
                print("  结论: 新闻内容列存在且样本非空，可用于舆情 agent 摘要。")
            else:
                print("  结论: 新闻内容列缺失或样本为空，需检查接口或标的。")
    except Exception as e:
        print("  异常:", e)
        import traceback
        traceback.print_exc()

    # 2) 互动易 stock_irm_cninfo（深市）
    print("\n--- 2. 互动易 ak.stock_irm_cninfo（若存在）---")
    fn_irm = getattr(ak, "stock_irm_cninfo", None)
    if fn_irm is None:
        print("  当前 akshare 无 stock_irm_cninfo，Phase 2 互动易拉取本期不实现。")
    else:
        try:
            # 常见为 stock_irm_cninfo(symbol="000630") 或 (stock="000630")，以 akshare 为准
            import inspect
            sig = inspect.signature(fn_irm)
            params = list(sig.parameters.keys())
            if "symbol" in params:
                df_irm = fn_irm(symbol=TEST_SYMBOL)
            elif "stock" in params:
                df_irm = fn_irm(stock=TEST_SYMBOL)
            else:
                df_irm = fn_irm(TEST_SYMBOL)
            if df_irm is None or df_irm.empty:
                print("  返回为空或 None（可能该标的无互动易数据）。")
            else:
                print("  列名:", list(df_irm.columns))
                for i, (_, row) in enumerate(df_irm.head(2).iterrows()):
                    print("  [%d] %s" % (i + 1, row.to_dict()))
                print("  结论: 接口可用、按股票维度返回，可作为舆情补充。")
        except Exception as e:
            print("  异常:", e)
            import traceback
            traceback.print_exc()
            print("  结论: 互动易调用失败，Phase 2 本期不实现。")

    print("\n" + "=" * 60)
    print("验证结束。请根据上述输出决定是否实施 Phase 2（互动易同步）。")
    print("=" * 60)


if __name__ == "__main__":
    main()
