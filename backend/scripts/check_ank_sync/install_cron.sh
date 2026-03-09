#!/bin/bash
# 在云主机上安装 AKShare 增量同步的 crontab 条目
# 用法：在云主机上执行
#   export BACKEND_DIR=/opt/trading/backend   # 改为你的 backend 实际路径
#   bash install_cron.sh
# 或：BACKEND_DIR=/opt/trading/backend bash install_cron.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="${BACKEND_DIR:-$(dirname "$(dirname "$SCRIPT_DIR")")}"

if [[ ! -d "$BACKEND_DIR" ]]; then
  echo "错误: BACKEND_DIR 不存在: $BACKEND_DIR"
  echo "请设置: export BACKEND_DIR=/path/to/backend"
  exit 1
fi

mkdir -p "$BACKEND_DIR/logs/check_ank_sync"
ENTRIES=$(sed "s|BACKEND_DIR|$BACKEND_DIR|g" "$SCRIPT_DIR/crontab.example" | grep -v '^#' | grep -v '^$')

echo "即将添加以下 crontab 条目（BACKEND_DIR=$BACKEND_DIR）："
echo "$ENTRIES"
echo ""

# 备份当前 crontab，合并新条目，去除重复后安装
(crontab -l 2>/dev/null || true; echo "$ENTRIES") | sort -u | crontab -
echo "已安装。当前 crontab："
crontab -l
