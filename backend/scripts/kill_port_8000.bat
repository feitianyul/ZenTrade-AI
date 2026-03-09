@echo off
REM 结束占用 8000 端口的进程（本项目的后端 API = uvicorn）。
REM 若杀完后端口又被占用，多半是「运行 run_all.py 或 uvicorn 的终端」还在，请关闭该终端或先结束 run_all.py 的 PID。
echo Checking port 8000...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8000"') do (
  if not "%%a"=="0" (
    echo Killing PID %%a
    taskkill /PID %%a /F 2>nul
  )
)
echo Done. If 8000 is still in use, close the terminal where you ran run_all.py or uvicorn.
