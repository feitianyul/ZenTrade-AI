-- 互动易问答表：新增 question_content、answer_content
-- 执行前请确认 stock_irm_qa 表已存在（仅需执行一次）

ALTER TABLE stock_irm_qa ADD COLUMN question_content TEXT NULL COMMENT '问题原文';
ALTER TABLE stock_irm_qa ADD COLUMN answer_content TEXT NULL COMMENT '回答原文';
