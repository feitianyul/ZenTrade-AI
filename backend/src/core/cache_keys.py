"""T207 - 缓存键定义"""


def market_quote_key(symbol: str) -> str:
    return f"market:quote:{symbol}"


def market_depth_key(symbol: str) -> str:
    return f"market:depth:{symbol}"


def strategy_list_key(tenant_id: str) -> str:
    return f"strategy:list:{tenant_id}"


def strategy_detail_key(tenant_id: str, strategy_id: str) -> str:
    return f"strategy:detail:{tenant_id}:{strategy_id}"


def user_profile_key(tenant_id: str, user_id: str) -> str:
    return f"user:profile:{tenant_id}:{user_id}"


def backtest_result_key(tenant_id: str, task_id: str) -> str:
    return f"backtest:result:{tenant_id}:{task_id}"


def config_key(tenant_id: str, key: str) -> str:
    return f"config:{tenant_id}:{key}"


def market_kline_key(symbol: str, period: str) -> str:
    return f"market:kline:{symbol}:{period}"


def market_indices_key() -> str:
    return "market:indices:all"


def market_fundamental_key(symbol: str) -> str:
    return f"market:fundamental:{symbol}"


def market_news_key(symbol: str) -> str:
    return f"market:news:{symbol}"


def market_hot_rank_key() -> str:
    return "market:hot_rank:hot"


def market_sectors_key(sector_type: str) -> str:
    return f"market:sectors:{sector_type}"


def stock_list_key() -> str:
    return "market:stock_list:all"


def sector_detail_key(code: str) -> str:
    return f"market:sector_detail:{code}"


def hot_rank_key(rank_type: str) -> str:
    return f"market:hot_rank:{rank_type}"


def session_key(session_id: str) -> str:
    return f"session:{session_id}"


def market_minute_key(symbol: str) -> str:
    return f"market:minute:{symbol}"


def market_minute_5day_key(symbol: str) -> str:
    return f"market:minute:5day:{symbol}"


def market_depth_l2_key(symbol: str) -> str:
    return f"market:depth:{symbol}"


def market_ranking_key(sort_by: str, order: str, limit: int) -> str:
    return f"market:ranking:{sort_by}:{order}:{limit}"


def market_lhb_key(days: int) -> str:
    return f"market:lhb:{days}"


def market_institutional_key(market: str, indicator: str) -> str:
    return f"market:inst:{market}:{indicator}"


def rate_limit_key(tenant_id: str, user_id: str, action: str) -> str:
    return f"rate_limit:{tenant_id}:{user_id}:{action}"
