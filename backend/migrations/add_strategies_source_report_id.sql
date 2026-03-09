-- 策略表增加来源分析报告 ID，用于「分析→发现」衔接与审计追溯
-- 执行前请确认 strategies 表已存在

ALTER TABLE strategies ADD COLUMN source_report_id VARCHAR(36) NULL COMMENT '来源分析报告ID，来自 ai_analysis_reports.id';
