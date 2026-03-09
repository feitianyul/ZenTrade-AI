-- 落库改为仅新增/更新不删：涨跌停、十大股东 按唯一键 UPSERT 用
-- 若表内已有重复行会 1062，需先执行下面的去重，再执行 ADD UNIQUE。

-- ========== 第一步：去重（每组保留 id 最小的一条） ==========

-- 涨跌停：保留每个 (symbol, trade_date, limit_type) 一条
DELETE t1 FROM stock_limit_updown t1
INNER JOIN stock_limit_updown t2
ON t1.symbol = t2.symbol AND t1.trade_date = t2.trade_date AND t1.limit_type = t2.limit_type AND t1.id > t2.id;

-- 十大股东：保留每个 (symbol, report_date, rank) 一条
DELETE t1 FROM stock_top_holders t1
INNER JOIN stock_top_holders t2
ON t1.symbol = t2.symbol AND t1.report_date = t2.report_date AND t1.`rank` = t2.`rank` AND t1.id > t2.id;

-- ========== 第二步：加唯一键 ==========

-- 涨跌停
ALTER TABLE stock_limit_updown ADD UNIQUE KEY uq_limit_updown_symbol_date_type (symbol, trade_date, limit_type);

-- 十大股东（rank 为保留字用反引号）
ALTER TABLE stock_top_holders ADD UNIQUE KEY uq_top_holder_symbol_date_rank (symbol, report_date, `rank`);
