-- 增量备份依赖的全量备份 ID，用于恢复链
ALTER TABLE backups ADD COLUMN parent_id VARCHAR(36) NULL AFTER tenant_id;
CREATE INDEX ix_backups_parent_id ON backups (parent_id);
