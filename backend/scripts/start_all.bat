@echo off
cd /d %~dp0..
echo Starting API on port 8000...
start "Backtest API" cmd /k "python -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000"
timeout /t 2 /nobreak >nul
echo Starting Backtest Worker...
start "Backtest Worker" cmd /k "python scripts/backtest_worker.py"
echo Starting Data Sync Worker...
start "Data Sync Worker" cmd /k "python scripts/data_sync_worker.py"
echo All started. Close windows to stop.
