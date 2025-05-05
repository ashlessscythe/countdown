# Delivery Dashboard

A dashboard application for tracking deliveries with a Python FastAPI backend and React frontend.

## Prerequisites

- Python 3.x with pip
- Node.js and npm

## Setup

1. Clone this repository
2. Install backend dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Install frontend dependencies:
   ```
   cd frontend
   npm install
   ```
4. Create a `config.py` file based on the `config.py.example` template

## Running the Application

### Option 1: Using the run script (recommended)

You can run both the backend and frontend with a single command:

#### Windows Command Prompt:

```
run.bat
```

#### PowerShell:

```
.\run.ps1
```

This will start both the backend server on port 8080 and the frontend development server on port 3000.

### Option 2: Running manually

#### Backend:

```
python main.py --port 8080
```

#### Frontend:

```
cd frontend
npm start
```

## Accessing the Application

Once both servers are running:

- Frontend: http://localhost:3000
- Backend API: http://localhost:8080/api
- API Documentation: http://localhost:8080/docs

## Troubleshooting

If you see "Proxy error: Could not proxy request" errors in the frontend:

1. Make sure the backend server is running
2. The backend might need more time to initialize - try refreshing the page after a few seconds
3. Check that the `proxy` setting in `frontend/package.json` is set to `"http://localhost:8080"`
