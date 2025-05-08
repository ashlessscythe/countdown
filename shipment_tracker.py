"""
Shipment Tracking Tool

This script processes shipment serial-level data and delivery package-level data
from Excel snapshots to produce an aggregated view per user of their scanning progress
and delivery status. It runs at a regular interval defined in config.py.

The tool:
1. Retrieves the latest snapshot files from ZMDESNR and VL06O directories
2. Filters records to the configured warehouse
3. Aggregates data by user and delivery
4. Calculates time metrics for user activity
5. Outputs the aggregated data in Parquet format
6. Retains only the last 5 output files
"""

import os
import time
import logging
from pathlib import Path
import pandas as pd
import config

def get_latest_file(directory, pattern="*.xlsx"):
    """
    Get the latest file in a directory matching the given pattern.
    
    Args:
        directory (str): Directory path to search
        pattern (str): Glob pattern to match files
        
    Returns:
        Path: Path object of the latest file, or None if no files found
    """
    files = list(Path(directory).glob(pattern))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)

def process_snapshot():
    """
    Process one cycle of snapshot data:
    - Load latest Excel files
    - Filter and process data
    - Output aggregated results if changes detected
    """
    # 1. Get latest files
    serial_file = get_latest_file(config.SERIAL_NUMBERS_DIR)
    delivery_file = get_latest_file(config.DELIVERY_INFO_DIR)
    
    if not serial_file or not delivery_file:
        logging.warning("No files found to process (serial_file=%s, delivery_file=%s)", 
                       serial_file, delivery_file)
        return

    logging.info(f"Processing files: {serial_file.name} and {delivery_file.name}")

    # 2. Read Excel files
    try:
        df_serial = pd.read_excel(serial_file)
        df_delivery = pd.read_excel(delivery_file)
    except Exception as e:
        logging.error(f"Error reading Excel files: {e}")
        return

    # 3. Filter warehouse and clean status data
    df_serial = df_serial[df_serial['Warehouse Number'] == config.WAREHOUSE_FILTER]
    
    # Map status codes to text
    df_serial['StatusText'] = df_serial['Status'].map(config.STATUS_MAPPING)
    
    # Handle the case where a serial appears as both ASH and SHP
    # If a serial is shipped, remove any picked records for that serial
    shipped_serials = set(df_serial[df_serial['Status'] == 'SHP']['Serial #'])
    df_serial = df_serial[~((df_serial['Serial #'].isin(shipped_serials)) & 
                           (df_serial['Status'] == 'ASH'))]

    # 4. Aggregate scanned packages by user and delivery
    # First, collapse to one record per pallet (package)
    df_pallets = df_serial.groupby(['Created by', 'Delivery', 'Pallet']).agg({
        'StatusText': 'first'  # Take the status of the first serial in the pallet
    }).reset_index()
    
    # Count unique pallets per user-delivery (scanned packages)
    agg = df_pallets.groupby(['Created by', 'Delivery']).agg(
        scanned_packages=('Pallet', 'count')
    ).reset_index()
    
    # Get status breakdown (picked vs shipped counts)
    status_counts = df_pallets.groupby(['Created by', 'Delivery', 'StatusText']).size().unstack(fill_value=0)
    # Ensure both status columns exist, even if no data
    for status in config.STATUS_MAPPING.values():
        if status not in status_counts.columns:
            status_counts[status] = 0
    
    # Rename columns for clarity
    status_counts.columns = [f"{col.replace(' / ', '_')}_count" for col in status_counts.columns]
    status_counts = status_counts.reset_index()
    
    # Merge status counts with the aggregated data
    agg = agg.merge(status_counts, on=['Created by', 'Delivery'], how='left')
    agg = agg.fillna(0)  # Fill NaN values with 0 for status counts
    
    # 5. Add total delivery packages from VL06O
    # Ensure delivery numbers are treated as strings to avoid type mismatches
    df_delivery['Delivery'] = df_delivery['Delivery'].astype(str)
    agg['Delivery'] = agg['Delivery'].astype(str)
    
    # Merge delivery totals
    agg = agg.merge(df_delivery[['Delivery', 'Number of packages']], 
                   on='Delivery', how='left')
    agg = agg.rename(columns={'Number of packages': 'delivery_total_packages'})
    
    # 6. Calculate time metrics per user
    # Convert date and time columns to a single timestamp
    df_serial['Timestamp'] = pd.to_datetime(df_serial['Created on'] + ' ' + df_serial['Time'])
    
    # Get timestamps for each user's scans, sorted chronologically
    user_timestamps = df_serial.sort_values('Timestamp').groupby('Created by')['Timestamp'].agg(list)
    
    # Calculate time metrics for each user
    user_metrics = []
    for user, timestamps in user_timestamps.items():
        if not timestamps:
            continue
            
        last_time = timestamps[-1]
        prev_time = timestamps[-2] if len(timestamps) > 1 else None
        
        time_since_last = pd.Timestamp.now() - last_time
        time_between = (last_time - prev_time) if prev_time else pd.Timedelta(0)
        
        user_metrics.append({
            'Created by': user,
            'last_scan_time': last_time,
            'time_since_last_scan': time_since_last,
            'time_between_scans': time_between
        })
    
    # Convert to DataFrame and merge with aggregated data
    if user_metrics:
        user_metrics_df = pd.DataFrame(user_metrics)
        agg = agg.merge(user_metrics_df, on='Created by', how='left')
    
    # 7. Rename columns for final output
    agg = agg.rename(columns={'Created by': 'user', 'Delivery': 'delivery'})
    
    # 8. Compare with previous output to detect changes
    latest_out = get_latest_file(config.OUT_DIR, "*.parquet")
    changed = True
    
    if latest_out:
        try:
            prev_df = pd.read_parquet(latest_out)
            
            # Compare relevant columns (excluding time-based ones which will always change)
            compare_cols = [col for col in agg.columns if not any(
                time_col in col for time_col in ['time_', 'last_scan'])]
            
            # Sort both DataFrames for stable comparison
            prev_df_sorted = prev_df[compare_cols].sort_values(['user', 'delivery']).reset_index(drop=True)
            new_df_sorted = agg[compare_cols].sort_values(['user', 'delivery']).reset_index(drop=True)
            
            if new_df_sorted.equals(prev_df_sorted):
                changed = False
                logging.info("No significant changes detected in data")
        except Exception as e:
            logging.error(f"Error comparing with previous output: {e}")
    
    # 9. Write new output if changes detected
    if changed:
        # Ensure output directory exists
        Path(config.OUT_DIR).mkdir(exist_ok=True)
        
        # Generate timestamped filename
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path(config.OUT_DIR) / f"output_{timestamp}.parquet"
        
        # Write to Parquet
        agg.to_parquet(out_path)
        logging.info(f"Wrote output file: {out_path}")
        
        # 10. Cleanup old outputs (keep last 5)
        outputs = sorted(Path(config.OUT_DIR).glob("output_*.parquet"), 
                        key=lambda f: f.stat().st_mtime, reverse=True)
        
        for old_file in outputs[5:]:
            try:
                old_file.unlink()
                logging.info(f"Removed old output file: {old_file.name}")
            except Exception as e:
                logging.warning(f"Failed to remove {old_file.name}: {e}")
    else:
        logging.info("No changes detected, skipping output generation")

def main():
    """
    Main function to run the shipment tracking process at regular intervals.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Log to console
            logging.FileHandler('shipment_tracker.log')  # Log to file
        ]
    )
    
    logging.info("Starting shipment tracking service...")
    
    # Run the process in a loop
    while True:
        try:
            process_snapshot()
        except Exception as e:
            logging.exception("Unexpected error in processing cycle")
        
        logging.info(f"Sleeping for {config.INTERVAL} seconds...")
        time.sleep(config.INTERVAL)

if __name__ == "__main__":
    main()
