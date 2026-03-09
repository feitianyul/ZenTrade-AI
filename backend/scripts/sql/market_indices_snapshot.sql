-- 预热大盘指数快照表，按日保留历史。
CREATE TABLE IF NOT EXISTS market_indices_snapshot (
    id INT AUTO_INCREMENT PRIMARY KEY,
    snapshot_date VARCHAR(10) NOT NULL COMMENT 'YYYY-MM-DD',
    code VARCHAR(10) NOT NULL,
    name VARCHAR(32) DEFAULT '',
    price DOUBLE DEFAULT 0,
    change_pct DOUBLE DEFAULT 0,
    change_amt DOUBLE DEFAULT 0,
    volume DOUBLE DEFAULT 0,
    turnover DOUBLE DEFAULT 0,
    updated_at DATETIME(6) NULL,
    UNIQUE KEY uq_indices_snapshot_date_code (snapshot_date, code),
    KEY ix_indices_snapshot_date (snapshot_date)
) COMMENT '预热大盘指数快照';
