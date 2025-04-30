"""
Configuration file for delivery analysis tools.
Contains paths and settings used by all scripts in the project.
"""
import os

# Base directory for data files
# Default is the ZMDESNR folder in Documents/Reports
BASE_DIR = os.path.join("C:\\", "temp", "reports", "ZMDESNR")

# Input directory where Excel files are stored
INPUT_DIR = BASE_DIR

# Output JSON file path
OUTPUT_JSON = os.path.join(BASE_DIR, "output.json")

# Default settings
DEFAULT_OUTPUT_FORMAT = "text"  # Options: "text", "json", "dataframe"

# Interval settings for scheduled execution (in seconds)
UPDATE_INTERVAL = 60  # Default: Run every 60 seconds

FILTER_WHSE = "E01" # Default warehouse filter
