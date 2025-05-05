# PowerShell script to run both backend and frontend

Write-Host "Starting backend and frontend..." -ForegroundColor Green

# Start the backend in a new window with a different port
Write-Host "Starting backend server on port 8080..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-Command python main.py --port 8080"

# Wait for the backend to initialize (5 seconds)
Write-Host "Waiting for backend to initialize..." -ForegroundColor Cyan
Start-Sleep -Seconds 5

# Start the frontend
Write-Host "Starting frontend server..." -ForegroundColor Cyan
Set-Location -Path frontend
npm start

Write-Host "Both services have been started." -ForegroundColor Green
