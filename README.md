# Delivery Dashboard

A dashboard application for tracking delivery shipments and user activity in real-time.

## Overview

This application processes data from ZMDESNR and VL06O Excel files to provide a real-time dashboard showing:

- User ID with shipment information
- Progress indicators (e.g., "30 out of 75")
- Current and previous scan times
- Warehouse-specific data (filtered for configured warehouse)

The system watches for new files in the specified directories and automatically updates the dashboard when new data is available.

## Project Structure

```
delivery-dashboard/
├── backend/
│   ├── data_processing/      # Data processing modules
│   ├── storage/              # Data storage and caching
│   └── api/                  # API endpoints
├── frontend/                 # Frontend dashboard (to be implemented)
├── config.py                 # Configuration settings
├── main.py                   # Application entry point
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

## Requirements

- Python 3.8+
- Pandas, NumPy, and other dependencies listed in requirements.txt

## Installation

1. Clone the repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

1. Copy the example configuration file to create your own configuration:

```bash
cp config.py.example config.py
```

2. Edit `config.py` to configure:

- Data file directories
- Warehouse filter (replace WAREHOUSE_CODE with your specific warehouse code)
- Update intervals
- Output directory for processed data

Note: `config.py` is excluded from version control via .gitignore to prevent sensitive information from being shared.

## Usage

### Running the Application

To run the dashboard application:

```bash
python main.py
```

This will:

1. Process the latest ZMDESNR and VL06O files
2. Generate dashboard data
3. Start watching for new files
4. Update the dashboard when new files are detected

### Testing Data Processing

To test the data processing modules without starting the full application:

```bash
python main.py --test
```

This will process the latest files and display statistics without starting the file watcher.

## Data Processing

The application processes data in the following steps:

1. **Reading**: Reads the latest ZMDESNR and VL06O Excel files
2. **Sanitization**: Standardizes headers and cleans data
3. **Filtering**: Filters for Pallet=1 and the configured Warehouse Number
4. **Transformation**: Extracts metrics like progress and scan times
5. **Storage**: Saves processed data as JSON files

## Development Status

- [x] Tollgate 1: Data Processing Setup
- [ ] Tollgate 2: Data Storage and Caching
- [ ] Tollgate 3: Backend API Development
- [ ] Tollgate 4: Frontend Dashboard Development
- [ ] Tollgate 5: Testing and Deployment

See `todo.md` for detailed development status and upcoming tasks.
