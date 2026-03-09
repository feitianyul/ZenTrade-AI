-- 分钟 K 线快照表 — 1/5/15/30/60 分钟周期按日存储，供 L3 读路径使用
-- 执行: mysql ... < migrations/add_market_minute_kline_snapshot.sql

CREATE TABLE IF NOT EXISTS market_minute_kline_snapshot (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL COMMENT '如 000630',
    period_min VARCHAR(6) NOT NULL COMMENT '1/5/15/30/60',
    snapshot_date VARCHAR(10) NOT NULL COMMENT 'YYYY-MM-DD',
    bars LONGTEXT NOT NULL COMMENT 'JSON array of K-line bars',
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_minute_kline_symbol_period_date (symbol, period_min, snapshot_date),
    INDEX ix_minute_kline_snapshot_date (snapshot_date),
    INDEX ix_minute_kline_symbol (symbol),
    INDEX ix_minute_kline_period (period_min)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分钟K线快照';
