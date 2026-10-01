@echo off
rem First-time setup for quake-pipeline on Windows.
rem Double-click this file from inside the quake-pipeline folder.

cd /d "%~dp0"
echo === quake-pipeline setup ===
echo.

python -c "import sys; assert sys.version_info >= (3, 10)" >nul 2>nul
if errorlevel 1 (
    echo Python 3.10 or newer was not found.
    echo Install it from https://www.python.org/downloads/ and tick "Add python.exe to PATH",
    echo then double-click this file again.
    goto :end
)
python --version

if not exist .venv (
    echo.
    echo [1/4] Creating virtual environment...
    python -m venv .venv || goto :failed
) else (
    echo.
    echo [1/4] Virtual environment already exists, skipping.
)

echo.
echo [2/4] Installing packages...
.venv\Scripts\python.exe -m pip install --quiet --upgrade pip || goto :failed
.venv\Scripts\python.exe -m pip install --quiet -e ".[dev]" || goto :failed

echo.
echo [3/4] Running tests...
.venv\Scripts\python.exe -m pytest -q || goto :failed

echo.
echo [4/4] Pulling one month of real USGS data as a test...
.venv\Scripts\python.exe -m quake_pipeline.ingest.usgs --backfill --start 2026-09-01 || goto :failed

echo.
echo === All done. Data is in the "data" folder. ===
goto :end

:failed
echo.
echo *** Something failed. Take a screenshot of this window and send it to Claude. ***

:end
echo.
pause
