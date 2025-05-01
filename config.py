"""
Configuration file for delivery analysis tools.
Contains paths and settings used by all scripts in the project.
"""
import os

# Base directory for data files
# Default is the ZMDESNR folder in Documents/Reports
BASE_DIR = os.path.join("C:\\", "temp", "reports", "ZMDESNR")

# Input directory where Excel files are stored (where XLSX snapshots are dropped)
DATA_DIR = BASE_DIR

# Output directory for Parquet files
OUT_DIR = os.path.join(BASE_DIR, "parquet_output")

# Ensure output directory exists
os.makedirs(OUT_DIR, exist_ok=True)

# Interval settings for scheduled execution (in seconds)
INTERVAL_SECONDS = 60  # Default: Run every 60 seconds

# Window for time-based metrics (in minutes)
WINDOW_MINUTES = 60  # Default: 60 minutes window for user activity metrics

# Column mapping for Excel files
COLUMNS = {
    "Serial #": "serial",
    "Status": "status",
    "Delivery": "delivery",
    "Shipment Number": "shipment",
    "Created by": "user",
    "Time": "time",
    "Created on": "created_on"
}

# Status code mapping
STATUS_MAPPING = {
    "ASH": "picked",
    "SHP": "shipped / closed"
}
