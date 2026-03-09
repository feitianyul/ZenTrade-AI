-- 策略表增加中文描述字段 logic_desc（双面板左中文右代码）
-- 执行前请确认 strategies 表已存在

ALTER TABLE strategies ADD COLUMN logic_desc TEXT NULL;
-- 策略当前启用版本号，用于「策略版本管理」启用/禁用
ALTER TABLE strategies ADD COLUMN active_version_no INT NULL;
