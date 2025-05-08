@echo off
REM Setup script for Shipment Tracking Tool
REM This script installs required packages and sets up the environment

echo Setting up Shipment Tracking Tool...

REM Check if Python is installed
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Python is not installed. Please install Python 3.7+ and try again.
    exit /b 1
)

REM Check Python version
for /f "tokens=2" %%i in ('python --version 2^>^&1') do set python_version=%%i
echo Found Python version: %python_version%

REM Create config.py from example if it doesn't exist
if not exist config.py (
    echo Creating config.py from example...
    copy config.py.example config.py
    echo Please edit config.py to match your environment.
)

REM Install required packages
echo Installing required packages...
pip install -r requirements.txt

REM Create required directories
echo Creating required directories...
for /f "tokens=*" %%i in ('python -c "import config; print(config.SERIAL_NUMBERS_DIR)"') do mkdir "%%i" 2>nul
for /f "tokens=*" %%i in ('python -c "import config; print(config.DELIVERY_INFO_DIR)"') do mkdir "%%i" 2>nul
for /f "tokens=*" %%i in ('python -c "import config; print(config.OUT_DIR)"') do mkdir "%%i" 2>nul
mkdir visualizations 2>nul

echo Setup complete! Running environment check...
echo.

REM Run environment check
python check_environment.py

echo.
echo Next steps:
echo 1. Edit config.py to match your environment
echo 2. Generate sample data: python generate_sample_data.py
echo 3. Run the tool: python shipment_tracker.py
echo    or run the entire workflow: python run_workflow.py

pause
