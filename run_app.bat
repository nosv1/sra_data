@echo off

REM Change to the current directory
cd /d "%~dp0"

REM Get the current directory
set current_dir=%cd%
echo Current directory: %current_dir%

start cmd /c ".\env_pc\Scripts\activate & python -m flask run"