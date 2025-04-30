# Delivery Analysis System

A system for analyzing delivery and shipment status data from Excel files.

## Overview

This system processes Excel files containing serial numbers, their statuses, and other information like delivery, customer, shipment, etc. It compares two Excel files to identify changes in serial number statuses and generates an output.json file with the results. The system also provides a web dashboard to visualize the data.

## Components

- **compare.py**: Compares two Excel files to identify changes in serial number statuses and generates an output.json file.
- **analyze_status.py**: Analyzes the output.json file to provide summaries and statistics.
- **app.py**: A Flask application that serves a web dashboard to visualize the data.
- **scheduled_compare.py**: A script that runs the comparison process at scheduled intervals.
- **status_cli.py**: A command-line interface for querying the status of serials.

## Usage

1. Place Excel files in the input directory specified in config.py.
2. Run compare.py to generate output.json.
3. Run app.py to start the web dashboard.

## Configuration

Configuration settings are stored in config.py:

- **BASE_DIR**: Base directory for data files.
- **INPUT_DIR**: Input directory where Excel files are stored.
- **OUTPUT_JSON**: Output JSON file path.
- **UPDATE_INTERVAL**: Interval for scheduled execution (in seconds).
- **FILTER_WHSE**: Warehouse filter.
