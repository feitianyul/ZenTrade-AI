"""公共模块：从 K 线构造 quote 兜底，供 fetch_quote 在非交易时间无 Redis 时使用。"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


async def build_quote_from_kline(symbol: str, code: str) -> dict[str, Any] | None:
    """从 load_kline_from_ch(code, \"daily\", 1) 取最新收盘价构造 quote。
    返回 {symbol, price, change: 0, volume, _from_kline: True, _stale: True} 或 None。
    """
    try:
        from src.services.data_service.kline_storage import load_kline_from_ch

        bars = await load_kline_from_ch(code, "daily", 1)
        if not bars:
            return None
        last = bars[-1]
        return {
            "symbol": symbol,
            "price": last["close"],
            "change": 0,
            "volume": last.get("volume", 0),
            "_from_kline": True,
            "_stale": True,
        }
    except Exception as e:
        logger.warning("kline_fallback.build_quote_from_kline failed: %s", e)
        return None
