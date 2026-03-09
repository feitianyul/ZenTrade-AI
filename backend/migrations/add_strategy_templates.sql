-- 策略模板表：供模板库与创建策略向导使用，管理员可增删改
-- 若使用 Base.metadata.create_all 则无需手动执行；否则可执行本脚本

CREATE TABLE IF NOT EXISTS strategy_templates (
  id VARCHAR(36) PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL,
  name VARCHAR(128) NOT NULL,
  `desc` VARCHAR(512) DEFAULT '',
  logic TEXT,
  logic_code TEXT NULL,
  icon VARCHAR(64) DEFAULT 'fa-chart-line',
  tags JSON,
  intro TEXT,
  pros JSON,
  cons JSON,
  tp FLOAT DEFAULT 10.0,
  sl FLOAT DEFAULT 8.0,
  sort_order INT DEFAULT 0,
  created_at DATETIME(6) NULL,
  updated_at DATETIME(6) NULL,
  INDEX idx_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
