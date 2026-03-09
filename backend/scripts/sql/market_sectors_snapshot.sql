-- 预热板块快照表，按日保留历史。
CREATE TABLE IF NOT EXISTS market_sectors_snapshot (
    id INT AUTO_INCREMENT PRIMARY KEY,
    snapshot_date VARCHAR(10) NOT NULL COMMENT 'YYYY-MM-DD',
    sector_type VARCHAR(20) NOT NULL COMMENT 'industry / concept',
    code VARCHAR(20) NOT NULL,
    name VARCHAR(64) DEFAULT '',
    change_pct DOUBLE DEFAULT 0,
    turnover DOUBLE DEFAULT 0,
    leader VARCHAR(32) DEFAULT NULL,
    leader_pct DOUBLE DEFAULT NULL,
    updated_at DATETIME(6) NULL,
    UNIQUE KEY uq_sectors_snapshot_date_type_code (snapshot_date, sector_type, code),
    KEY ix_sectors_snapshot_date (snapshot_date)
) COMMENT '预热板块快照';
