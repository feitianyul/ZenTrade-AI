-- 数据备份：BackupStatus 增加 cancelled（已取消）
-- 若 backups.status 为 MySQL ENUM，执行下方 MODIFY；若为 VARCHAR 则无需执行（应用层已支持 cancelled 字符串）。

ALTER TABLE backups MODIFY COLUMN status ENUM('pending','in_progress','success','failed','cancelled') NOT NULL DEFAULT 'pending';
