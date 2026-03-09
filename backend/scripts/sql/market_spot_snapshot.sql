-- 预热行情快照表：热门/排行拉取后写入，Redis 未命中时读库展示。按日覆盖。
-- 执行一次即可（或通过 backend 的 Base.metadata.create_all 自动建表）。
CREATE TABLE IF NOT EXISTS market_spot_snapshot (
    id INT AUTO_INCREMENT PRIMARY KEY,
    snapshot_date VARCHAR(10) NOT NULL COMMENT 'YYYY-MM-DD',
    symbol VARCHAR(10) NOT NULL,
    name VARCHAR(64) DEFAULT '',
    price DOUBLE DEFAULT 0,
    change_pct DOUBLE DEFAULT 0,
    change_amt DOUBLE DEFAULT 0,
    volume DOUBLE DEFAULT 0,
    turnover DOUBLE DEFAULT 0,
    turnover_rate DOUBLE DEFAULT 0,
    updated_at DATETIME(6) NULL,
    UNIQUE KEY uq_spot_snapshot_date_symbol (snapshot_date, symbol),
    KEY ix_spot_snapshot_date (snapshot_date),
    KEY ix_spot_snapshot_symbol (symbol)
) COMMENT '预热行情快照';
