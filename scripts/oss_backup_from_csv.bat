@echo off
REM Windows 双击或任务计划程序调用此文件
cd /d "%~dp0\.."
python "%~dp0oss_backup_from_csv.py" %*
if errorlevel 1 exit /b 1
