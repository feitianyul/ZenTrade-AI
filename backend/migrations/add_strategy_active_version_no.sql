-- 策略当前启用版本号，用于「策略版本管理」启用/禁用
ALTER TABLE strategies ADD COLUMN active_version_no INT NULL;
