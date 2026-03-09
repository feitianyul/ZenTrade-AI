# ZenTrade-AI

`ZenTrade-AI` 是一个面向散户低频量化交易场景的代码精简仓库，当前版本只保留了前端静态页面、后端核心源码、脚本、数据库迁移和基础配置，适合继续开发策略模板、行情数据、交易接口与后台管理功能。

## 当前保留范围

本仓库按精简版本整理，仅保留以下目录内容：

- `frontend`
- `backend/src`
- `backend/scripts`
- `backend/migrations`
- `backend/config`

其余原始项目文件、部署材料、测试数据、临时文件和非白名单目录均已移除，因此这个仓库现在更像是一个可继续整理和二次开发的源码快照，而不是开箱即用的完整交付物。

## 项目概览

### 前端

- `frontend/index.html` 是用户端单页应用，包含登录、策略中心、AI 输入区、行情、仪表盘等页面逻辑。
- `frontend/public/admin/feitian/index.html` 是独立的后台管理控制台，覆盖权限管理、AI 配置、配置中心、策略模板、备份和日志等管理功能。
- `frontend/package.json` 使用 Vite 作为本地开发与构建工具，整体实现偏向原生 HTML + CSS + JavaScript 的静态 SPA 结构。

### 后端

- `backend/src/main.py` 基于 FastAPI 组织服务入口，挂载认证、策略、交易、行情、数据同步、AI 配置、配置中心、备份、社区、因子、对象存储等路由。
- `backend/src/models` 定义了策略、订单、持仓、策略模板、社区、备份、配置、审计等核心数据模型。
- `backend/src/routers` 提供 REST API，适用于前端页面、管理台和后续外部集成。
- `backend/src/services` 集中放置业务服务，包括行情读取、数据同步、策略导入、回测、AI 分析、权限校验、配置治理和备份调度等能力。

### 数据与配置

- `backend/migrations` 保存数据库迁移脚本，例如策略模板表、AI 分析报告、备份扩展字段等。
- `backend/config/dev.yaml` 与 `backend/config/prod.yaml` 提供基础环境配置，包含 MySQL、ClickHouse、Redis、向量存储和 JWT 相关参数。
- `backend/scripts` 提供数据初始化、迁移执行、检查校验、同步任务和运维辅助脚本。
- 当前仓库未保留后端根目录依赖清单、`.env` 模板和完整部署材料，因此保留下来的启动脚本与迁移脚本主要用于源码参考，不代表可直接一键运行。

## 当前重点能力

- 策略模板管理：后端提供 `strategy_templates` 模型、API 与 SQL 迁移，前后端都围绕模板库进行了实现。
- 策略导入：支持从 `CSV / XLSX / JSON` 批量导入策略。
- 行情与数据同步：包含热榜、K 线、代理池、增量同步、定时预热等服务逻辑。
- 交易与账户：保留了交易、持仓、订单和账户相关路由与模型。
- 管理后台：保留独立后台页面，可继续扩展权限、配置、审计与备份能力。

## 推荐阅读顺序

如果你准备继续在这个仓库上开发，建议优先阅读下面几个入口：

1. `backend/src/main.py`
2. `backend/src/routers/strategy_templates.py`
3. `backend/src/models/strategy_template.py`
4. `backend/scripts/seed_strategy_templates.py`
5. `backend/migrations/add_strategy_templates.sql`
6. `frontend/index.html`
7. `frontend/public/admin/feitian/index.html`

## 目录结构

```text
ZenTrade-AI/
├── README.md
├── frontend/
│   ├── index.html
│   ├── package.json
│   ├── vite.config.ts
│   └── public/
│       └── admin/
│           └── feitian/
│               └── index.html
└── backend/
    ├── config/
    │   ├── dev.yaml
    │   └── prod.yaml
    ├── migrations/
    ├── scripts/
    └── src/
        ├── core/
        ├── models/
        ├── routers/
        ├── schemas/
        ├── services/
        └── utils/
```

## 使用说明

### 前端

前端目录保留了 Vite 配置和依赖声明，可以直接在 `frontend` 目录安装并启动：

```bash
cd frontend
npm install
npm run dev
```

### 后端

后端核心源码已经保留，但当前精简仓库没有保留原始根级依赖清单、环境样例、测试与部署文件。如果要重新运行后端，需要你根据 `backend/src`、`backend/config` 和实际数据库环境，自行补齐：

- Python 依赖安装方式
- `.env` 环境变量
- 启动脚本或进程管理方式
- 数据库、缓存和外部服务连接

另外，`backend/migrations` 当前更适合作为 SQL/迁移脚本资料目录使用。虽然保留了 `env.py`，但仓库并未保留完整 Alembic 工程文件，因此不要默认按完整 Alembic 项目直接执行。

## 仓库定位

这个版本的 `ZenTrade-AI` 更适合作为以下用途：

- 上传到 GitHub 进行源码存档
- 继续围绕策略模板和管理后台做二次开发
- 从完整项目中抽离核心逻辑后进行重构
- 为后续重新整理依赖、文档和部署方案打基础

## License

未在当前精简版本中单独附带许可证文件；如需开源发布，建议在后续补充 `LICENSE`、依赖说明和部署文档。