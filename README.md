# Shipment Tracking Tool

A Python tool for processing shipment and delivery snapshots to produce an aggregated view of scanning progress and delivery status per user.

## Overview

This tool consolidates shipment serial-level data and delivery package-level data from two sets of Excel snapshots:

- **ZMDESNR** – Excel snapshots containing serial-level shipment status records
- **VL06O** – Excel snapshots containing delivery-level information

The tool processes the latest snapshots from each source, aggregates the data by user and delivery, and outputs the results in Parquet format. It runs at a regular interval (default: 60 seconds) and retains only the last 5 output files.

## Features

- Automatically processes the latest snapshot files
- Filters records to a specific warehouse (configurable)
- Maps status codes to human-readable text
- Aggregates data by user and delivery
- Calculates time metrics for user activity
- Detects changes between processing cycles
- Outputs data in Parquet format
- Retains only the last 5 output files
- Comprehensive logging

## Requirements

- Python 3.7+
- pandas
- pyarrow (for Parquet support)
- matplotlib (for visualization)

## Installation

### Automatic Setup

For convenience, setup scripts are provided for both Linux/macOS and Windows:

**Linux/macOS:**

```bash
chmod +x setup.sh
./setup.sh
```

**Windows:**

```
setup.bat
```

These scripts will:

1. Check if Python is installed
2. Create `config.py` from the example if it doesn't exist
3. Install required packages from `requirements.txt`
4. Create necessary directories

### Manual Setup

If you prefer to set up manually:

1. Clone this repository
2. Install the required packages:

```bash
pip install pandas pyarrow matplotlib
```

3. Copy `config.py.example` to `config.py` and update the settings as needed:

```bash
cp config.py.example config.py
```

4. Update the configuration in `config.py` to match your environment:
   - Set `BASE_DIR` to the directory where your Excel snapshots are stored
   - Adjust `WAREHOUSE_FILTER` to match your warehouse code
   - Modify `INTERVAL` if you want to change the update frequency

### Verifying Your Environment

To check if your environment meets all requirements, run:

```bash
python check_environment.py
```

This script will verify:

- Python version (3.7+)
- Required packages and their versions
- Configuration file existence and required variables
- Directory access and permissions

If any issues are found, the script will provide detailed error messages to help you resolve them.

## Usage

Run the tool with:

```bash
python shipment_tracker.py
```

The tool will:

1. Start logging to both console and `shipment_tracker.log`
2. Look for the latest Excel files in the configured directories
3. Process the data and output to Parquet files in the configured output directory
4. Run continuously at the configured interval

## Running as a Service

For production environments, you can install the tool as a system service to run automatically in the background, even after system reboots.

### Linux Service (systemd)

To install as a Linux systemd service:

```bash
chmod +x install_service.sh
sudo ./install_service.sh
```

This creates a systemd service that:

- Runs the shipment tracker in the background
- Starts automatically on system boot
- Restarts automatically if it crashes

You can manage the service with standard systemd commands:

```bash
sudo systemctl start shipment-tracker.service   # Start the service
sudo systemctl stop shipment-tracker.service    # Stop the service
sudo systemctl status shipment-tracker.service  # Check service status
sudo journalctl -u shipment-tracker.service     # View service logs
```

### Windows Service

To install as a Windows service:

1. Download and install [NSSM (Non-Sucking Service Manager)](https://nssm.cc/)
2. Add the NSSM directory to your PATH or copy nssm.exe to the project directory
3. Run the installation script as Administrator:
   ```
   install_service.bat
   ```

This creates a Windows service that:

- Runs the shipment tracker in the background
- Starts automatically on system boot
- Restarts automatically if it crashes

You can manage the service through the Windows Services management console (`services.msc`) or with commands:

```
sc start ShipmentTracker   # Start the service
sc stop ShipmentTracker    # Stop the service
sc query ShipmentTracker   # Check service status
```

Service logs are written to `service_stdout.log` and `service_stderr.log` in the project directory.

## Output Format

The tool produces Parquet files with the following structure:

- **user**: Scanner's ID
- **delivery**: Delivery number
- **scanned_packages**: Count of packages this user scanned for that delivery
- **picked_count**: Number of packages with "picked" status
- **shipped_count**: Number of packages with "shipped / closed" status
- **delivery_total_packages**: Total packages in that delivery (from VL06O)
- **last_scan_time**: Timestamp of the user's most recent scan
- **time_since_last_scan**: Time elapsed since the user's last scan
- **time_between_scans**: Time difference between the user's last two scans

## File Structure

- `config.py` - Configuration settings
- `shipment_tracker.py` - Main script
- `test_shipment_tracker.py` - Script for running a single processing cycle (for testing)
- `generate_sample_data.py` - Script for generating sample test data
- `visualize_results.py` - Script for visualizing the output data
- `run_workflow.py` - Script for running the entire workflow in one go
- `check_environment.py` - Script for verifying system requirements and dependencies
- `cleanup.py` - Script for resetting the environment to its initial state
- `install_service.sh` - Script for installing as a Linux systemd service
- `install_service.bat` - Script for installing as a Windows service
- `setup.sh` - Setup script for Linux/macOS
- `setup.bat` - Setup script for Windows
- `requirements.txt` - Python package dependencies
- `docs/howto.md` - Detailed documentation
- `README.md` - This file

## Logging

The tool logs information to both the console and a file (`shipment_tracker.log`). The log includes:

- Processing start/end times
- Files being processed
- Output file creation
- Errors and warnings

## Testing and Visualization

### Test Script

For testing and debugging purposes, you can use the `test_shipment_tracker.py` script:

```bash
python test_shipment_tracker.py
```

This script runs a single processing cycle without the continuous loop, which is useful for testing changes without waiting for the full interval.

### Sample Data Generator

To generate sample test data, use the `generate_sample_data.py` script:

```bash
python generate_sample_data.py
```

This script creates:

- Sample ZMDESNR data (serial-level shipment status records)
- Sample VL06O data (delivery-level information)

The generated Excel files are saved in the configured directories and can be used to test the shipment tracker without real data.

### Visualization Tool

To visualize the output data, use the `visualize_results.py` script:

```bash
python visualize_results.py
```

This script reads the latest Parquet output file and generates several visualizations:

1. **Packages Scanned by User** - Bar chart showing the total number of packages scanned by each user
2. **Status Breakdown** - Bar chart showing the number of packages in each status (picked vs shipped)
3. **Scanning Progress by Delivery** - Bar chart showing the percentage of packages scanned for the top 10 deliveries
4. **User Activity Timeline** - Horizontal bar chart showing the last scan time for each user

The visualizations are saved as PNG files in a `visualizations` directory.

### Complete Workflow Runner

To run the entire workflow in one go, use the `run_workflow.py` script:

```bash
python run_workflow.py
```

This script:

1. Generates sample data using `generate_sample_data.py`
2. Processes the data once using `test_shipment_tracker.py`
3. Creates visualizations using `visualize_results.py`

This is useful for quickly testing the entire system without having to run each script separately. The script logs its progress to both the console and a `workflow.log` file.

### Cleanup

To reset the environment to its initial state, use the `cleanup.py` script:

```bash
python cleanup.py
```

This script will:

- Remove all Excel files in the configured directories
- Remove all Parquet output files
- Remove all log files
- Remove the visualizations directory

The script will ask for confirmation before proceeding and will not remove the configuration file (`config.py`).

## Troubleshooting

If the tool is not working as expected:

1. Check the log file (`shipment_tracker.log`) for errors
2. Verify that the Excel files exist in the configured directories
3. Ensure the Excel files have the expected column names
4. Check that the warehouse filter matches the data in your Excel files
