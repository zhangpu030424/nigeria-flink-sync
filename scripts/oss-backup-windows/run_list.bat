@echo off
cd /d "%~dp0"
python "%~dp0oss_backup_from_csv.py" --list %*
if errorlevel 1 pause
