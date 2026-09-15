@echo off
cd /d "%~dp0"
python "%~dp0oss_backup_from_csv.py" %*
if errorlevel 1 pause
