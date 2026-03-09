-- 互动易问答表 — 来源 AKShare stock_irm_cninfo（深市）
-- 执行前请确认数据库已存在

CREATE TABLE IF NOT EXISTS stock_irm_qa (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    question_id VARCHAR(64) NOT NULL DEFAULT '',
    content TEXT,
    ask_time VARCHAR(32) NOT NULL DEFAULT '',
    answer_time VARCHAR(32) NOT NULL DEFAULT '',
    source VARCHAR(128) NOT NULL DEFAULT '',
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    INDEX ix_stock_irm_qa_symbol (symbol),
    INDEX ix_stock_irm_qa_symbol_time (symbol, answer_time),
    UNIQUE KEY uk_stock_irm_qa_symbol_question (symbol, question_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='互动易问答';
