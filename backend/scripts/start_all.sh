#!/bin/bash
cd "$(dirname "$0")/.."
echo "Starting API + Worker..."
trap 'kill 0' SIGINT SIGTERM
python -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000 &
sleep 2
python scripts/backtest_worker.py &
python scripts/data_sync_worker.py &
wait
