@echo off
echo Starting backend and frontend...

:: Start the backend in a new window with a different port
start cmd /k "python main.py --port 8080"

:: Wait for 5 seconds to allow the backend to initialize
echo Waiting for backend to initialize...
timeout /t 5 /nobreak > nul

:: Start the frontend
cd frontend
npm start

echo Both services have been started.
