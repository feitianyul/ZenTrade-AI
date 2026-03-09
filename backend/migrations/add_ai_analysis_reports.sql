-- AI 分析报告版本表：按标的存储每次生成/采纳的分析快照，供列表与恢复
-- 执行: mysql ... < migrations/add_ai_analysis_reports.sql

CREATE TABLE IF NOT EXISTS ai_analysis_reports (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    symbol VARCHAR(20) NOT NULL COMMENT '标的代码',
    report_snapshot JSON NOT NULL COMMENT '完整分析结果 dimensions/main/zones/suggest',
    user_notes TEXT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'draft' COMMENT 'draft|adopted',
    created_by VARCHAR(64) NULL,
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    INDEX ix_ai_reports_tenant_symbol (tenant_id, symbol),
    INDEX ix_ai_reports_created_at (tenant_id, symbol, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='AI分析报告版本';
