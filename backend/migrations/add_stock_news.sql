-- 个股资讯/公告表 — 来源 AKShare stock_news_em
-- 执行前请确认数据库已存在

CREATE TABLE IF NOT EXISTS stock_news (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    title VARCHAR(512) NOT NULL DEFAULT '',
    content TEXT,
    publish_time VARCHAR(32) NOT NULL DEFAULT '',
    source VARCHAR(128) NOT NULL DEFAULT '',
    url VARCHAR(512) NOT NULL DEFAULT '',
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    INDEX ix_stock_news_symbol (symbol),
    INDEX ix_stock_news_symbol_time (symbol, publish_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='个股资讯/公告';
