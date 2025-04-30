# Delivery Analysis System

A system for analyzing delivery and shipment status data from Excel files.

## Overview

This system processes Excel files containing serial numbers, their statuses, and other information like delivery, customer, shipment, etc. It implements a workflow of snapshots and deltas to track historical changes and provide a dashboard with current status and changes over time.

## Workflow

The system follows this workflow:

1. **Snapshots**: Each Excel file is converted to a JSON snapshot, representing the state at a specific point in time.
2. **Deltas**: Differences between snapshots are computed and saved as delta files.
3. **History**: Deltas can be appended to a master history file for a complete historical record.
4. **Dashboard**: The web dashboard displays current status and historical changes.

## Folder Structure

```
/data/
  ├── snapshots/
  │     ├── 20250430-1034.json
  │     ├── 20250430-1040.json
  ├── changes/
  │     ├── 20250430-1040_delta.json
  └── master_history.json
```

## Components

- **convert.py**: Converts Excel files to JSON snapshots.
- **compare.py**: Compares snapshots to identify changes and generates delta files.
- **log_updates.py**: Appends delta files to the master history.
- **analyze_status.py**: Analyzes the data to provide summaries and statistics.
- **app.py**: A Flask application that serves a web dashboard to visualize the data.
- **scheduled_compare.py**: A script that runs the comparison process at scheduled intervals.
- **status_cli.py**: A command-line interface for querying the status of serials.

## Usage

### Basic Usage

1. Place Excel files in the input directory specified in config.py.
2. Run compare.py to process the files and generate snapshots, deltas, and statistics.
3. Run app.py to start the web dashboard.

### Advanced Usage

- **Manual Snapshot Creation**: Use convert.py to manually create a snapshot from an Excel file.

  ```
  python convert.py path/to/excel_file.xlsx
  ```

- **Manual Delta Processing**: Use log_updates.py to manually append delta files to the master history.

  ```
  python log_updates.py path/to/delta_file.json
  ```

- **Scheduled Execution**: Use scheduled_compare.py to run the comparison process at scheduled intervals.
  ```
  python scheduled_compare.py
  ```

## Configuration

Configuration settings are stored in config.py:

- **BASE_DIR**: Base directory for data files.
- **INPUT_DIR**: Input directory where Excel files are stored.
- **OUTPUT_JSON**: Output JSON file path.
- **UPDATE_INTERVAL**: Interval for scheduled execution (in seconds).
- **FILTER_WHSE**: Warehouse filter.

## Dashboard Display

The dashboard provides several views:

- **Current Status**: Count of ASH vs SHP by delivery/customer.
- **Timeline View**: Serial status changes over time.
- **User Activity**: Serials created by each user.
