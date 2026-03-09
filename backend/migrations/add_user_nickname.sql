-- 用户表增加昵称字段
-- 执行前请确认 users 表已存在

ALTER TABLE users ADD COLUMN nickname VARCHAR(64) NULL;
