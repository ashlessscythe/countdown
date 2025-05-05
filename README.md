# Delivery Dashboard

A dashboard application for tracking delivery progress and user activity.

## Project Overview

The Delivery Dashboard is designed to track and visualize delivery progress and user activity in real-time. It processes data from ZMDESNR and VL06O Excel files, transforms the data, and provides a RESTful API for frontend consumption.

## Features

- Data processing from ZMDESNR and VL06O Excel files
- Real-time data updates via WebSockets
- RESTful API for dashboard data
- User activity tracking
- Delivery progress calculation
- Scan time tracking

## Project Structure

```
delivery-dashboard/
├── backend/
│   ├── data_processing/      # Data processing modules
│   ├── storage/              # Data storage and caching
│   ├── api/                  # API endpoints and services
│   └── app.py                # Main backend application
├── frontend/                 # Frontend application (to be implemented)
├── config.py                 # Configuration file
├── main.py                   # Application entry point
├── test_api.py               # API test script
└── requirements.txt          # Dependencies
```

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd delivery-dashboard
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Configure the application:

Copy the example configuration file and modify it as needed:

```bash
cp config.py.example config.py
```

Edit `config.py` to set the appropriate paths for your environment.

## Usage

### Running the Application

To run the full application (data processing + API server):

```bash
python main.py
```

To run only the API server without data processing:

```bash
python main.py --api-only
```

To test the data processing without starting the API server:

```bash
python main.py --test
```

### API Endpoints

The API is available at `http://localhost:8000/api` with the following endpoints:

- `GET /api/dashboard` - Get all dashboard data
- `GET /api/users` - Get user activity data
  - Query parameters:
    - `active_only` (boolean): If true, return only active users
- `GET /api/progress` - Get delivery progress data
  - Query parameters:
    - `delivery_id` (string): Filter by delivery ID
    - `user_id` (string): Filter by user ID
- `GET /api/scan-times` - Get scan time data
  - Query parameters:
    - `user_id` (string): Filter by user ID
- `POST /api/track-activity` - Track user activity
  - Query parameters:
    - `user_id` (string): User ID
    - `activity_type` (string): Type of activity (e.g., 'scan', 'view')
- `WebSocket /api/ws` - WebSocket endpoint for real-time updates

### Testing the API

To test the API endpoints:

```bash
# Start the API server in one terminal
python main.py

# Run the test script in another terminal
python test_api.py
```

## Development

### Adding New Features

1. Implement data processing in `backend/data_processing/`
2. Add storage functionality in `backend/storage/`
3. Create API endpoints in `backend/api/routes.py`
4. Implement business logic in `backend/api/services.py`
5. Update the frontend to consume the API

### Running Tests

```bash
python test_api.py
```

## License

See the [LICENSE](LICENSE) file for details.
