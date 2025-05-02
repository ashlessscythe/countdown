"""
Main entry point for the Delivery Dashboard application.
"""
import os
import sys
import time
import logging
import pandas as pd
import json
from datetime import datetime

# Import data processing modules
from backend.data_processing.readers import read_zmdesnr_file, read_vl06o_file, get_combined_data
from backend.data_processing.sanitizers import sanitize_zmdesnr_dataframe, sanitize_vl06o_dataframe
from backend.data_processing.transformers import prepare_dashboard_data
from backend.data_processing.watchers import poll_for_new_files, file_change_callback

# Import configuration
from config import (
    SERIAL_NUMBERS_DIR, 
    DELIVERY_INFO_DIR, 
    OUT_DIR, 
    INTERVAL_SECONDS,
    WAREHOUSE_FILTER
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_files():
    """
    Process the latest ZMDESNR and VL06O files and generate dashboard data.
    
    Returns:
        dict: Dashboard data
    """
    logger.info("Processing latest files...")
    
    # Get the combined data
    serials_df, deliveries_df, combined_df = get_combined_data()
    
    if serials_df.empty or deliveries_df.empty:
        logger.warning("One or both dataframes are empty")
        return {}
    
    # Sanitize the dataframes
    serials_df = sanitize_zmdesnr_dataframe(serials_df)
    deliveries_df = sanitize_vl06o_dataframe(deliveries_df)
    
    # Prepare dashboard data
    dashboard_data = prepare_dashboard_data(combined_df)
    
    # Save the dashboard data to a JSON file
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    json_path = os.path.join(OUT_DIR, f"dashboard_data_{timestamp}.json")
    
    with open(json_path, 'w') as f:
        json.dump(dashboard_data, f, default=str, indent=2)
    
    logger.info(f"Dashboard data saved to {json_path}")
    
    return dashboard_data

def file_update_handler(file_type, file_path):
    """
    Handle updates when new files are detected.
    
    Args:
        file_type (str): Type of file ('zmdesnr' or 'vl06o')
        file_path (str): Path to the new file
    """
    logger.info(f"New {file_type.upper()} file detected: {file_path}")
    
    # Process the files and update the dashboard data
    dashboard_data = process_files()
    
    # Here you would typically:
    # 1. Update any caches or storage
    # 2. Notify the frontend of new data
    
    logger.info(f"Processed new {file_type.upper()} file. Dashboard data updated.")

def run_dashboard():
    """
    Run the dashboard application.
    """
    logger.info("Starting Delivery Dashboard application...")
    
    # Process files initially
    dashboard_data = process_files()
    
    # Print some stats
    if dashboard_data:
        num_users = len(dashboard_data.get('users', []))
        num_deliveries = len(dashboard_data.get('deliveries', []))
        logger.info(f"Initial data loaded: {num_users} users, {num_deliveries} deliveries")
    
    # Start watching for file changes
    logger.info(f"Starting file watcher with {INTERVAL_SECONDS} second interval...")
    poll_for_new_files(file_update_handler, INTERVAL_SECONDS)

def test_data_processing():
    """
    Test the data processing modules.
    """
    logger.info("Testing data processing modules...")
    
    # Read the latest files
    serials_df = read_zmdesnr_file()
    deliveries_df = read_vl06o_file()
    
    # Print some stats
    logger.info(f"ZMDESNR file: {len(serials_df)} rows")
    logger.info(f"VL06O file: {len(deliveries_df)} rows")
    
    # Sanitize the dataframes
    serials_df = sanitize_zmdesnr_dataframe(serials_df)
    deliveries_df = sanitize_vl06o_dataframe(deliveries_df)
    
    # Get the combined data
    _, _, combined_df = get_combined_data()
    
    # Print some stats
    logger.info(f"Combined data: {len(combined_df)} rows")
    
    # Prepare dashboard data
    dashboard_data = prepare_dashboard_data(combined_df)
    
    # Print some stats
    num_users = len(dashboard_data.get('users', []))
    num_deliveries = len(dashboard_data.get('deliveries', []))
    logger.info(f"Dashboard data: {num_users} users, {num_deliveries} deliveries")
    
    return dashboard_data

if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Run in test mode
        test_data_processing()
    else:
        # Run the dashboard application
        run_dashboard()
