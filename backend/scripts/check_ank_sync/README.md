# AKShare 增量同步脚本（check_ank_sync）

本目录包含各数据分类的增量同步脚本，由 `run_incremental_sync_scripts.py` 主控，支持**按更新频率**分类执行，便于定时任务与限流。

## 更新频率分类

| 频率 | 说明 | 脚本（分类） |
|------|------|--------------|
| **daily**（日更） | 每个交易日或收盘后更新（行情、资金、龙虎榜等） | K线行情、涨跌停、行业/概念板块、北向资金、北向持股排行、资金流向、龙虎榜、大宗交易、融资融券、资讯/公告、同行比较 |
| **weekly**（周更） | 数据变化不频繁，每周跑一次即可 | 交易所日历、A股列表 |
| **quarterly**（季更） | 季报披露后更新 | 分红配股、股东户数、财务指标 |
| **semi_annual**（半年更） | 半年报/年报报告期更新 | 十大股东 |

**说明**：交易所日历仅在节假日/调休公布时变化，A股列表仅在 IPO/退市时变化，故归为周更；同行比较按「最近交易日」同步，属日更行情数据。

## 主控用法（在 backend 目录下）

```bash
# 跑全部（默认，保持原一次性全量行为）
PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py

# 仅跑日更（适合每日定时任务，如收盘后）
PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py --frequency daily

# 仅跑季更（适合每季报披露后跑一次）
PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py --frequency quarterly

# 仅跑周更（交易所日历、A股列表，适合每周跑一次）
PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py --frequency weekly

# 仅跑半年更（适合半年报/年报报告期后跑一次）
PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py --frequency semi_annual

# 干跑（不落库）+ 并发数
PYTHONPATH=. python scripts/check_ank_sync/run_incremental_sync_scripts.py --dry-run -j 3
```

## 定时任务建议

- **日更**：每日 15:30 后或 16:00 跑一次 `--frequency daily`。
- **周更**：每周（如周六）跑一次 `--frequency weekly`，覆盖交易所日历、A股列表。
- **季更**：财报可能延后披露，建议在「披露季 + 延后 1～2 月」的**每周六**跑，覆盖迟报：如 1/4/5 月（年报、一季）、8/9 月（半年）、10/11 月（三季）。脚本为增量/幂等，多跑只会补漏。
- **半年更**：半年报（约 8/31）、年报（约 4/30）同样会延后，建议 4/5/8/9 月**每周六**跑，避免漏掉迟报公司的十大股东等数据。

如需调整某脚本所属频率，请修改 `run_incremental_sync_scripts.py` 中 `_SCRIPTS` 的第四项（`FREQ_DAILY` / `FREQ_WEEKLY` / `FREQ_QUARTERLY` / `FREQ_SEMI_ANNUAL`）。

## 云主机定时任务（crontab）

已提供模板与安装脚本，需在云主机上指定 `BACKEND_DIR` 后执行：

```bash
# 1. 上传本目录下的 crontab.example 和 install_cron.sh 到云主机（或 git pull 后进入 backend/scripts/check_ank_sync）

# 2. 设置 backend 路径并安装（路径按实际部署修改）
export BACKEND_DIR=/opt/trading/backend
bash install_cron.sh

# 或一行
BACKEND_DIR=/opt/trading/backend bash install_cron.sh
```

- **crontab.example**：四条 cron 示例（日更 周一～五 15:30；周更 周六 9:00；季更 1/4/5/8/9/10/11 月周六 10:00 以覆盖披露+延后；半年更 4/5/8/9 月周六 11:00），其中的 `BACKEND_DIR` 会在安装时被替换。
- **install_cron.sh**：合并当前 crontab 与上述条目并安装，避免覆盖已有任务。云主机需已配置好 Python 环境与项目依赖。
