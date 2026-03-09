-- 用户表增加 AI 调用次数用户级限额覆盖
-- 非空时优先于角色限额

ALTER TABLE users ADD COLUMN ai_calls_limit_override INT NULL;
