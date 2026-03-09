-- 备份进度与步骤日志：progress_percent (0-100)、log_entries (JSON 数组字符串)
-- 执行前请确认 backups 表已存在

ALTER TABLE backups ADD COLUMN progress_percent INT NULL;
ALTER TABLE backups ADD COLUMN log_entries TEXT NULL;
