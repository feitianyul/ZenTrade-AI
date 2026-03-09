"""T187 - 事件主题常量与路由配置"""

# ---- 交易主题 ----
TRADE_ORDER_CREATED = "trade.order.created"
TRADE_ORDER_FILLED = "trade.order.filled"
TRADE_ORDER_CANCELLED = "trade.order.cancelled"
TRADE_ORDER_REJECTED = "trade.order.rejected"
TRADE_POSITION_UPDATED = "trade.position.updated"

# ---- 行情主题 ----
MARKET_QUOTE_UPDATE = "market.quote.update"
MARKET_DEPTH_UPDATE = "market.depth.update"
MARKET_ALERT_TRIGGERED = "market.alert.triggered"
MARKET_ANOMALY_DETECTED = "market.anomaly.detected"

# ---- 策略主题 ----
STRATEGY_CREATED = "strategy.created"
STRATEGY_UPDATED = "strategy.updated"
STRATEGY_BACKTEST_STARTED = "strategy.backtest.started"
STRATEGY_BACKTEST_COMPLETED = "strategy.backtest.completed"

# ---- 系统主题 ----
SYSTEM_HEALTH_CHECK = "system.health.check"
SYSTEM_CONFIG_CHANGED = "system.config.changed"
SYSTEM_ALERT = "system.alert"
SYSTEM_HEARTBEAT = "system.heartbeat"

# ---- AI 主题 ----
AI_RESULT_READY = "ai.result.ready"
AI_OPTIMIZATION_TRIGGERED = "ai.optimization.triggered"
AI_CONFIG_SYNCED = "ai.config.synced"

# ---- 用户主题 ----
USER_LOGIN = "user.login"
USER_LOGOUT = "user.logout"
USER_ROLE_CHANGED = "user.role.changed"

# ---- 备份主题 ----
BACKUP_STARTED = "backup.started"
BACKUP_COMPLETED = "backup.completed"
BACKUP_FAILED = "backup.failed"

# 主题分组（用于权限控制）
TOPIC_GROUPS = {
    "trade": [
        TRADE_ORDER_CREATED,
        TRADE_ORDER_FILLED,
        TRADE_ORDER_CANCELLED,
        TRADE_ORDER_REJECTED,
        TRADE_POSITION_UPDATED,
    ],
    "market": [
        MARKET_QUOTE_UPDATE,
        MARKET_DEPTH_UPDATE,
        MARKET_ALERT_TRIGGERED,
        MARKET_ANOMALY_DETECTED,
    ],
    "strategy": [
        STRATEGY_CREATED,
        STRATEGY_UPDATED,
        STRATEGY_BACKTEST_STARTED,
        STRATEGY_BACKTEST_COMPLETED,
    ],
    "system": [
        SYSTEM_HEALTH_CHECK,
        SYSTEM_CONFIG_CHANGED,
        SYSTEM_ALERT,
        SYSTEM_HEARTBEAT,
    ],
    "ai": [
        AI_RESULT_READY,
        AI_OPTIMIZATION_TRIGGERED,
        AI_CONFIG_SYNCED,
    ],
}
