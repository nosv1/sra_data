@echo off

REM Change to the current directory
cd /d "%~dp0"

REM Get the current directory
set current_dir=%cd%
echo Current directory: %current_dir%

REM Run the Python script in a new command prompt window and close it after 5 seconds
start cmd /c "python results_downloader.py & cd .. & .\env_pc\Scripts\activate & python race_result_parser_neo4j.py & timeout /t 5"