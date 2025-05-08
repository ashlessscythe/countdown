@echo off
REM Install Shipment Tracking Tool as a Windows service
REM This script creates a Windows service that runs the shipment tracker in the background
REM Requires NSSM (Non-Sucking Service Manager) - https://nssm.cc/

echo Installing Shipment Tracking Tool as a Windows service...

REM Check if running as administrator
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo Please run this script as Administrator
    echo Right-click on the script and select "Run as administrator"
    pause
    exit /b 1
)

REM Check if NSSM is available
where nssm >nul 2>&1
if %errorLevel% neq 0 (
    echo NSSM (Non-Sucking Service Manager) is required but not found.
    echo Please download and install NSSM from https://nssm.cc/
    echo Add the NSSM directory to your PATH or copy nssm.exe to this directory.
    pause
    exit /b 1
)

REM Get the current directory
set SCRIPT_DIR=%~dp0
set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%
echo Installing service from directory: %SCRIPT_DIR%

REM Check if Python is available
where python >nul 2>&1
if %errorLevel% neq 0 (
    echo Python is not found in PATH. Please install Python and try again.
    pause
    exit /b 1
)

REM Get Python executable path
for /f "tokens=*" %%i in ('where python') do set PYTHON_PATH=%%i
echo Using Python: %PYTHON_PATH%

REM Install the service using NSSM
echo Installing service "ShipmentTracker"...
nssm install ShipmentTracker "%PYTHON_PATH%" "%SCRIPT_DIR%\shipment_tracker.py"
nssm set ShipmentTracker DisplayName "Shipment Tracking Tool"
nssm set ShipmentTracker Description "Processes shipment and delivery snapshots to track scanning progress"
nssm set ShipmentTracker AppDirectory "%SCRIPT_DIR%"
nssm set ShipmentTracker AppStdout "%SCRIPT_DIR%\service_stdout.log"
nssm set ShipmentTracker AppStderr "%SCRIPT_DIR%\service_stderr.log"
nssm set ShipmentTracker AppRotateFiles 1
nssm set ShipmentTracker AppRotateBytes 1048576
nssm set ShipmentTracker Start SERVICE_AUTO_START

echo Service installed successfully!
echo.
echo You can now manage the service with these commands:
echo   sc start ShipmentTracker   - Start the service
echo   sc stop ShipmentTracker    - Stop the service
echo   sc query ShipmentTracker   - Check service status
echo.
echo Or use the Windows Services management console (services.msc)
echo.
echo The service is set to start automatically on system boot.
echo To start it now, run: sc start ShipmentTracker
echo.
echo Service logs will be written to:
echo   %SCRIPT_DIR%\service_stdout.log
echo   %SCRIPT_DIR%\service_stderr.log
echo.

pause
