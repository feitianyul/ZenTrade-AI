-- 用户封禁状态：0=正常，1=已封禁
ALTER TABLE users ADD COLUMN is_banned TINYINT(1) NOT NULL DEFAULT 0;
