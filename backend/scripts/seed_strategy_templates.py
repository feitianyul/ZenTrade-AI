"""
一次性种子：向 strategy_templates 插入 7 条系统模板（tenant_id=system）。
与主站 ST_TEMPLATES 一致，幂等：若已存在则跳过。
用法（在 backend 目录）: python scripts/seed_strategy_templates.py
"""
import asyncio
import json
import os
import sys
import uuid
from datetime import datetime

try:
    import dotenv
    _backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _env = os.path.join(_backend_dir, ".env")
    if os.path.isfile(_env):
        dotenv.load_dotenv(_env)
except Exception:
    print("WARN: .env 加载失败，继续使用当前环境变量")

if __name__ == "__main__":
    _backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _backend_dir not in sys.path:
        sys.path.insert(0, _backend_dir)

SYSTEM_TENANT_ID = "system"

# 与 frontend/index.html ST_TEMPLATES 一致（只存一份）
BUILTIN_TEMPLATES = [
    {"id": "ma_cross", "name": "均线金叉策略", "desc": "当短期均线上穿长期均线时买入，下穿时卖出", "logic": "MA5上穿MA20买入，MA5下穿MA20卖出", "logic_code": "def run(ctx):\n    if ctx.ma5 > ctx.ma20:\n        ctx.buy()\n    elif ctx.ma5 < ctx.ma20:\n        ctx.sell()", "icon": "fa-chart-line", "tags": ["均线", "趋势"], "tp": 10, "sl": 8, "sort_order": 0,
     "intro": "基于双均线交叉的经典趋势跟踪策略。短期均线（如MA5）上穿长期均线（如MA20）视为金叉买入信号，下穿视为死叉卖出信号。", "pros": ["规则简单、易于执行", "适合趋势行情"], "cons": ["震荡市易频繁止损", "存在滞后性"]},
    {"id": "low_pe", "name": "低估蓝筹策略", "desc": "买入市盈率低于行业均值的大盘蓝筹股", "logic": "市盈率<15且市值>500亿，分散买入", "logic_code": "def run(ctx):\n    # TODO: 接入基本面字段后替换为真实判断\n    # 条件: 市盈率 < 15 且市值 > 500亿\n    pass", "icon": "fa-landmark", "tags": ["价值", "蓝筹"], "tp": 15, "sl": 10, "sort_order": 1,
     "intro": "价值投资思路：筛选市盈率低于行业平均、市值较大的蓝筹股，分散配置，等待估值修复。", "pros": ["风险相对可控", "适合中长期持有"], "cons": ["需耐心等待", "短期可能跑输题材"]},
    {"id": "vol_break", "name": "量价突破策略", "desc": "成交量放大且价格突破关键位时买入", "logic": "成交量>5日均量2倍且突破20日高点", "logic_code": "def run(ctx):\n    if ctx.volume > ctx.ma_volume_5 * 2 and ctx.close > ctx.high_20:\n        ctx.buy()\n    elif ctx.close < ctx.ma10:\n        ctx.sell()", "icon": "fa-bolt", "tags": ["量价", "突破"], "tp": 12, "sl": 8, "sort_order": 2,
     "intro": "在价格突破重要阻力位且成交量明显放大时入场，量价配合确认突破有效性。", "pros": ["能捕捉强势启动阶段", "信号较明确"], "cons": ["假突破需严格止损", "对择时要求高"]},
    {"id": "macd_gold", "name": "MACD金叉策略", "desc": "MACD指标金叉时买入，死叉时卖出", "logic": "MACD金叉买入，死叉卖出", "logic_code": "def run(ctx):\n    if ctx.macd_dif > ctx.macd_dea:\n        ctx.buy()\n    elif ctx.macd_dif < ctx.macd_dea:\n        ctx.sell()", "icon": "fa-wave-square", "tags": ["MACD", "技术"], "tp": 10, "sl": 7, "sort_order": 3,
     "intro": "利用MACD快慢线金叉、死叉作为买卖信号，是常用的趋势与动量结合指标。", "pros": ["应用广泛、资料多", "适合趋势行情"], "cons": ["震荡市易反复打脸", "有滞后"]},
    {"id": "multi_indicator_trend", "name": "多指标共振趋势跟踪策略", "desc": "多个技术指标同时发出同向信号时入场，顺势跟踪趋势", "logic": "均线多头排列+MACD金叉+量能放大时买入，任一信号反转减仓或卖出", "logic_code": "def run(ctx):\n    long_signal = ctx.ma5 > ctx.ma20 and ctx.macd_dif > ctx.macd_dea and ctx.volume > ctx.ma_volume_5\n    exit_signal = ctx.ma5 < ctx.ma20 or ctx.macd_dif < ctx.macd_dea\n    if long_signal:\n        ctx.buy()\n    elif exit_signal:\n        ctx.sell()", "icon": "fa-layer-group", "tags": ["多指标", "趋势", "共振"], "tp": 12, "sl": 8, "sort_order": 4,
     "intro": "通过均线、MACD、成交量等多个指标共振确认趋势，减少单一指标的噪音，提高信号可靠性。", "pros": ["过滤假信号、胜率相对高", "适合中短期趋势"], "cons": ["信号较少、可能错过部分行情", "参数需根据品种调整"]},
    {"id": "sentiment_momentum", "name": "情绪动能突破策略", "desc": "结合市场情绪与价格动能，在情绪拐点或动能加速时参与突破", "logic": "情绪指标（如涨跌家数比、恐慌指数）拐点+价格放量突破关键位时买入", "logic_code": "def run(ctx):\n    # TODO: 接入情绪指标后替换为真实判断\n    # 情绪回暖 + 放量突破时买入，跌回关键位卖出\n    pass", "icon": "fa-chart-line", "tags": ["情绪", "动能", "突破"], "tp": 14, "sl": 9, "sort_order": 5,
     "intro": "将市场情绪与价格动能结合：在情绪从极端回归或动能明显增强且价格突破时入场，捕捉情绪与趋势的共振机会。", "pros": ["能捕捉情绪拐点行情", "适合短线与波段"], "cons": ["情绪数据依赖外部源", "需严格风控"]},
    {"id": "triple_golden_cross", "name": "三线金叉策略", "desc": "均线、MACD、成交量三者同时金叉时买入，多重确认趋势启动", "logic": "MA5上穿MA10、MACD金叉、量能上穿均量线三者同时满足时买入", "logic_code": "def run(ctx):\n    signal = ctx.ma5 > ctx.ma10 and ctx.macd_dif > ctx.macd_dea and ctx.volume > ctx.ma_volume_5\n    if signal:\n        ctx.buy()\n    elif ctx.ma5 < ctx.ma10:\n        ctx.sell()", "icon": "fa-braille", "tags": ["三线金叉", "趋势", "技术"], "tp": 11, "sl": 7, "sort_order": 6,
     "intro": "均线金叉、MACD金叉、量能金叉（成交量上穿均量线）三者同时出现时视为强趋势启动信号，多重确认后入场。", "pros": ["信号可靠性较高", "适合趋势初段"], "cons": ["满足条件较少、机会不多", "有一定滞后"]},
]


def _stable_uuid(template_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"strategy_template_system_{template_id}"))


async def main():
    from sqlalchemy import text
    from src.core.db import get_engine

    engine = get_engine()
    inserted = 0
    updated = 0
    skipped = 0
    now = datetime.utcnow()
    async with engine.begin() as conn:
        for t in BUILTIN_TEMPLATES:
            pk = _stable_uuid(t["id"])
            # 幂等：已存在则跳过
            r = await conn.execute(text("SELECT 1 FROM strategy_templates WHERE id = :id"), {"id": pk})
            if r.first() is not None:
                await conn.execute(
                    text("""
                    UPDATE strategy_templates
                    SET name = :name,
                        `desc` = :desc,
                        logic = :logic,
                        logic_code = :logic_code,
                        icon = :icon,
                        tags = :tags,
                        intro = :intro,
                        pros = :pros,
                        cons = :cons,
                        tp = :tp,
                        sl = :sl,
                        sort_order = :sort_order,
                        updated_at = :now
                    WHERE id = :id
                    """),
                    {
                        "id": pk,
                        "name": t["name"],
                        "desc": t["desc"],
                        "logic": t["logic"],
                        "logic_code": t["logic_code"],
                        "icon": t["icon"],
                        "tags": json.dumps(t["tags"], ensure_ascii=False),
                        "intro": t["intro"],
                        "pros": json.dumps(t["pros"], ensure_ascii=False),
                        "cons": json.dumps(t["cons"], ensure_ascii=False),
                        "tp": t["tp"],
                        "sl": t["sl"],
                        "sort_order": t["sort_order"],
                        "now": now,
                    },
                )
                updated += 1
                continue
            tags_j = json.dumps(t["tags"], ensure_ascii=False)
            pros_j = json.dumps(t["pros"], ensure_ascii=False)
            cons_j = json.dumps(t["cons"], ensure_ascii=False)
            await conn.execute(
                text("""
                INSERT INTO strategy_templates
                (id, tenant_id, name, `desc`, logic, logic_code, icon, tags, intro, pros, cons, tp, sl, sort_order, created_at, updated_at)
                VALUES (:id, :tenant_id, :name, :desc, :logic, :logic_code, :icon, :tags, :intro, :pros, :cons, :tp, :sl, :sort_order, :now, :now)
                """),
                {
                    "id": pk,
                    "tenant_id": SYSTEM_TENANT_ID,
                    "name": t["name"],
                    "desc": t["desc"],
                    "logic": t["logic"],
                    "logic_code": t["logic_code"],
                    "icon": t["icon"],
                    "tags": tags_j,
                    "intro": t["intro"],
                    "pros": pros_j,
                    "cons": cons_j,
                    "tp": t["tp"],
                    "sl": t["sl"],
                    "sort_order": t["sort_order"],
                    "now": now,
                },
            )
            inserted += 1
    print(f"OK: 系统策略模板种子完成 inserted={inserted} updated={updated} skipped={skipped}")


if __name__ == "__main__":
    asyncio.run(main())
