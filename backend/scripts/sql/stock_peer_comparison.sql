-- 同行比较表（仅建表，不修改其他表）
-- 执行前请确认数据库已选；可从项目 backend 目录执行: mysql -u root -p < scripts/sql/stock_peer_comparison.sql

CREATE TABLE IF NOT EXISTS stock_peer_comparison (
  id BIGINT NOT NULL AUTO_INCREMENT,
  symbol VARCHAR(10) NOT NULL COMMENT '6 位代码',
  sub_type VARCHAR(20) NOT NULL COMMENT 'growth|valuation|dupont|scale',
  as_of_date VARCHAR(10) NOT NULL COMMENT '同步日 YYYY-MM-DD',
  raw_data TEXT NULL COMMENT '整表 JSON',
  updated_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_peer_comparison_symbol_sub_date (symbol, sub_type, as_of_date),
  KEY ix_peer_comparison_symbol (symbol),
  KEY ix_peer_comparison_as_of_date (as_of_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同行比较-成长性/估值/杜邦/规模';
