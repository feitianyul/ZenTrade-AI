-- 数据备份一期：为 backups 表增加 content、content_summary、destination、error_detail、log_url
-- 执行前请确认 backups 表已存在（由 ORM 或其它迁移创建）
-- MySQL：逐条执行；若某列已存在会报 Duplicate column，可忽略该条继续执行下一条。

ALTER TABLE backups ADD COLUMN content VARCHAR(512) NULL;
ALTER TABLE backups ADD COLUMN content_summary VARCHAR(255) NULL;
ALTER TABLE backups ADD COLUMN destination VARCHAR(32) DEFAULT 'local';
ALTER TABLE backups ADD COLUMN error_detail VARCHAR(1024) NULL;
ALTER TABLE backups ADD COLUMN log_url VARCHAR(512) NULL;
