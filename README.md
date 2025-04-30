# SAP Snapshot Comparison Dashboard

A Flask-based dashboard for displaying differences (delta) between SAP snapshots. The dashboard shows time between scans for each user, providing insights into scan patterns and efficiency.

## Features

- Comparison of Excel snapshots from SAP
- Calculation of time differences between scans for each user
- Interactive dashboard with charts and tables
- Automatic updates on a configurable interval
- Support for large datasets (5-15k rows)

## Project Structure

```
├── app.py                 # Main Flask application
├── compare_snapshots.py   # Script to compare Excel snapshots
├── analyze_excel.py       # Script to analyze Excel files
├── config.py              # Configuration settings
├── convert.py             # Script to convert Excel files to JSON
├── static/                # Static files
│   ├── css/               # CSS stylesheets
│   │   └── style.css      # Custom styles
│   └── js/                # JavaScript files
│       └── dashboard.js   # Dashboard functionality
├── templates/             # HTML templates
│   └── index.html         # Dashboard template
├── utils/                 # Utility modules
│   ├── __init__.py
│   ├── background_tasks.py # Background task handling
│   ├── data_utils.py       # Data utility functions
│   └── time_utils.py       # Time-related utility functions
└── sample_files/          # Sample Excel files for testing
    ├── sample_1.xlsx
    └── sample_2.xlsx
```

## Setup and Installation

1. Ensure you have Python 3.6+ installed
2. Install required packages:
   ```
   pip install flask pandas plotly openpyxl
   ```
3. Configure the application in `config.py`
4. Run the application:
   ```
   python app.py
   ```
5. Access the dashboard at http://localhost:5000

## Configuration

Edit `config.py` to customize the following settings:

- `INPUT_DIR`: Directory where Excel files are stored
- `UPDATE_INTERVAL`: Interval for automatic updates (in seconds)
- `FILTER_WHSE`: Warehouse filter for data processing

## Usage

1. Place Excel files in the configured input directory
2. The application will automatically compare the files on the configured interval
3. View the dashboard to see the comparison results
4. Use the "Refresh Data" button to manually trigger an update

## Dashboard Components

- **Comparison Summary**: Overview of files compared, common serials, total users, and average scan time
- **Average Scan Time by User**: Bar chart showing average scan time for each user
- **Distribution of Scan Time Differences**: Histogram of time differences between scans
- **Scan Timeline by Serial Number**: Timeline chart showing scan events for each serial number
- **Detailed Scan Data**: Table with detailed information about each scan

## Development

To extend or modify the dashboard:

1. Edit `templates/index.html` to change the dashboard layout
2. Modify `static/css/style.css` to customize the appearance
3. Update `static/js/dashboard.js` to change the dashboard functionality
4. Modify `app.py` to add new API endpoints or features

## License

See the LICENSE file for details.
