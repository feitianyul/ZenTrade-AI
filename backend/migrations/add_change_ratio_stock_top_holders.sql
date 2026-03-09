-- 十大股东表与东财接口对齐：新增 change_ratio，扩展 holder_type
-- 执行前请确认表名与库名正确；若列已存在会报 Duplicate column，可忽略或先检查。

-- MySQL:
ALTER TABLE stock_top_holders ADD COLUMN change_ratio FLOAT NULL COMMENT '变动比率 CHANGE_RATIO';
ALTER TABLE stock_top_holders MODIFY COLUMN holder_type VARCHAR(64) COMMENT '股东性质 HOLDER_TYPE';
