-- data_sync_tasks 增加 updated_at，用于「长时间未更新」判定
-- 执行前请确认表 data_sync_tasks 已存在

ALTER TABLE data_sync_tasks
ADD COLUMN updated_at DATETIME(6) NULL COMMENT '最后活动时间，用于长时间未更新(stale)判定';
