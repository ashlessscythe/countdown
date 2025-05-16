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
import numpy as np
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

def sanitize_delivery_number(delivery):
    """
    Sanitize delivery number to ensure consistent format.
    
    Args:
        delivery: Delivery number (could be int, float, or string)
        
    Returns:
        str: Sanitized delivery number as string without decimal part
    """
    if pd.isna(delivery):
        return None
    
    # Convert to string and remove any decimal part
    delivery_str = str(delivery).split('.')[0]
    return delivery_str

def ensure_timestamp_column(df):
    """
    Ensure dataframe has a Timestamp column by combining 'Created on' and 'Time' columns.
    
    Args:
        df (DataFrame): DataFrame to process
        
    Returns:
        bool: True if timestamp column exists or was created, False otherwise
    """
    if 'Timestamp' not in df.columns:
        if 'Created on' in df.columns and 'Time' in df.columns:
            logging.info("Creating Timestamp column from 'Created on' and 'Time' columns")
            
            # First ensure both columns are strings
            created_on_str = df['Created on'].astype(str)
            time_str = df['Time'].astype(str)
            
            # Combine date and time columns to create a timestamp
            df['Timestamp'] = created_on_str + ' ' + time_str
            
            try:
                df['Timestamp'] = pd.to_datetime(df['Timestamp'])
                logging.info(f"Converted timestamps to datetime. Sample: {df['Timestamp'].head(3)}")
                return True
            except Exception as e:
                logging.error(f"Error converting timestamp to datetime: {e}")
                return False
        else:
            logging.warning("Cannot create timestamp: 'Created on' or 'Time' columns missing")
            return False
    return True

def filter_by_warehouse(df, warehouse_code):
    """
    Filter dataframe to only include records from the specified warehouse.
    
    Args:
        df (DataFrame): DataFrame to filter
        warehouse_code (str): Warehouse code to filter by
        
    Returns:
        DataFrame: Filtered DataFrame
    """
    pre_length = len(df)
    df_filtered = df[df['Warehouse Number'] == warehouse_code]
    logging.info(f"Filtered data to warehouse {warehouse_code}. length before: {pre_length}, after: {len(df_filtered)}")
    return df_filtered

def filter_by_parent_serial(df):
    """
    Filter dataframe to only include records with no parent serial number.
    
    Args:
        df (DataFrame): DataFrame to filter
        
    Returns:
        DataFrame: Filtered DataFrame
    """
    pre_length = len(df)
    df_filtered = df[df['Parent serial number'].isna() | (df['Parent serial number'] == '')]
    logging.info(f"Filtered data to only include parent serial numbers. length before: {pre_length}, after: {len(df_filtered)}")
    return df_filtered

def filter_by_time_window(df, reference_time, window_minutes):
    """
    Filter dataframe to only include records within the specified time window.
    
    Args:
        df (DataFrame): DataFrame to filter
        reference_time (datetime): Reference time for the window
        window_minutes (int): Window size in minutes
        
    Returns:
        DataFrame: Filtered DataFrame
    """
    if 'Timestamp' not in df.columns:
        if not ensure_timestamp_column(df):
            return df
    
    pre_length = len(df)
    cutoff_time = reference_time - pd.Timedelta(minutes=window_minutes)
    df_filtered = df[df['Timestamp'] >= cutoff_time]
    logging.info(f"Filtered data to only include records within the last {window_minutes} minutes")
    logging.info(f"length before: {pre_length}, after: {len(df_filtered)}")
    return df_filtered

def deduplicate_serial_status(df):
    """
    Deduplicate records by Serial # and Status, keeping the most recent.
    Assumes df is already sorted by timestamp in descending order.
    
    Args:
        df (DataFrame): DataFrame to deduplicate
        
    Returns:
        DataFrame: Deduplicated DataFrame
    """
    pre_length = len(df)
    if 'Serial #' in df.columns and 'Status' in df.columns:
        df_deduped = df.drop_duplicates(subset=['Serial #', 'Status'], keep='first')
        logging.info("Deduplicated serial data by Serial # and Status (keeping most recent timestamp)")
        logging.info(f"length before: {pre_length}, after: {len(df_deduped)}")
        return df_deduped
    return df

def process_ash_shp_statuses(df):
    """
    Process ASH and SHP statuses according to business rules:
    1. Identify serials with both ASH and SHP records
    2. Remove SHP records for serials that only have SHP (no corresponding ASH)
    3. For serials with both ASH and SHP, keep only the SHP records (count as shipped)
    
    Args:
        df (DataFrame): DataFrame to process
        
    Returns:
        DataFrame: Processed DataFrame, set of ASH users
    """
    # Identify users who do ASH (for filtering visualizations later)
    ash_users = set(df[df['Status'] == 'ASH']['Created by'])
    
    # Find serials that have both ASH and SHP records
    # Group by Serial # and check if both ASH and SHP statuses exist
    serial_status_counts = df.groupby('Serial #')['Status'].apply(set)
    
    # Serials with both ASH and SHP
    serials_with_both = serial_status_counts[serial_status_counts.apply(lambda x: 'ASH' in x and 'SHP' in x)].index
    
    # Serials with only SHP (no corresponding ASH)
    serials_with_only_shp = serial_status_counts[serial_status_counts.apply(lambda x: 'SHP' in x and 'ASH' not in x)].index
    
    # Remove SHP records for serials that only have SHP (no corresponding ASH)
    df = df[~((df['Serial #'].isin(serials_with_only_shp)) & 
             (df['Status'] == 'SHP'))]
    
    # For serials with both ASH and SHP, keep only the SHP records (count as shipped)
    df = df[~((df['Serial #'].isin(serials_with_both)) & 
             (df['Status'] == 'ASH'))]
    
    # Add a flag to identify users who do ASH
    df['is_ash_user'] = df['Created by'].isin(ash_users)
    
    return df, ash_users

def apply_shp_ceiling_timestamp(df):
    """
    Apply timestamp ceiling for serials where SHP is the last status.
    For each serial, find the latest SHP timestamp and filter out any records after that.
    
    Args:
        df (DataFrame): DataFrame to process
        
    Returns:
        DataFrame: Processed DataFrame
    """
    if 'Timestamp' not in df.columns:
        return df
    
    # Get serials with SHP status
    serials_with_shp = df[df['Status'] == 'SHP']
    
    if not serials_with_shp.empty:
        # Get the latest SHP timestamp for each serial
        latest_shp = serials_with_shp.groupby('Serial #')['Timestamp'].max()
        
        # More efficient approach using vectorized operations
        # Create a temporary column with the SHP ceiling timestamp for each serial
        df['shp_ceiling'] = df['Serial #'].map(latest_shp)
        
        # Keep only rows where either:
        # 1. The serial doesn't have a SHP ceiling timestamp, or
        # 2. The timestamp is not after the SHP ceiling
        df = df[(df['shp_ceiling'].isna()) | (df['Timestamp'] <= df['shp_ceiling'])]
        
        # Drop the temporary column
        df = df.drop('shp_ceiling', axis=1)
        
        logging.info(f"Applied timestamp ceiling for {len(latest_shp)} serials with SHP status")
    
    return df

def calculate_time_metrics(df_serial, reference_time=None):
    """
    Calculate time between scans metrics for each user.
    
    Args:
        df_serial (DataFrame): DataFrame containing serial scan data with timestamps
        reference_time (datetime, optional): Reference time to use for calculations.
            If None, current system time will be used.
        
    Returns:
        DataFrame: DataFrame with time metrics grouped by user
    """
    # Print the head of the dataframe to see column types
    logging.info("Serial data columns and types:")
    for col in df_serial.columns:
        logging.info(f"{col}: {df_serial[col].dtype}")
    
    # Ensure we have a timestamp column
    if not ensure_timestamp_column(df_serial):
        return pd.DataFrame()
    
    # Sort by user and timestamp
    df_sorted = df_serial.sort_values(['Created by', 'Timestamp'])
    
    # Group by user to calculate time differences between consecutive scans
    time_metrics = []
    
    # Get reference time for calculations - ensure it's timezone-naive for consistent comparison
    if reference_time is None:
        now = pd.Timestamp.now().replace(tzinfo=None)
        logging.info(f"Using current system time for calculations: {now}")
    else:
        now = reference_time.replace(tzinfo=None)
        logging.info(f"Using reference time from filename for calculations: {now}")
    
    for user, group in df_sorted.groupby('Created by'):
        # Skip if user has less than 2 scans
        if len(group) < 2:
            continue
            
        # Calculate time differences between consecutive scans
        group = group.sort_values('Timestamp')
        
        # Ensure timestamps are timezone-naive for consistent comparison
        if group['Timestamp'].iloc[0].tzinfo is not None:
            group['Timestamp'] = group['Timestamp'].dt.tz_localize(None)
            
        time_diffs = group['Timestamp'].diff().dropna()
        
        # Convert time differences to seconds
        time_diffs_seconds = time_diffs.dt.total_seconds()
        
        # Calculate metrics with safeguards for empty dataframes
        if time_diffs_seconds.empty:
            avg_time_between_scans = 0
        else:
            avg_time_between_scans = time_diffs_seconds.mean()
            
        last_scan_time = group['Timestamp'].max()
        
        # Ensure last_scan_time is timezone-naive for comparison with now
        if last_scan_time.tzinfo is not None:
            last_scan_time = last_scan_time.replace(tzinfo=None)
            
        # Log the last scan time for debugging
        logging.info(f"User {user} - Last scan time: {last_scan_time}")
        
        # Calculate time since last scan, ensuring it's not negative
        time_since_last_scan = (now - last_scan_time).total_seconds()
        
        # If time_since_last_scan is negative, it means the last scan time is in the future
        # This could be due to clock synchronization issues or timezone problems
        if time_since_last_scan < 0:
            logging.warning(f"Negative time since last scan detected for user {user}. "
                           f"Last scan time: {last_scan_time}, Current time: {now}. "
                           f"Setting time_since_last_scan to 0.")
            time_since_last_scan = 0
        
        # Create a list of all time differences for this user
        time_diff_list = time_diffs_seconds.tolist() if not time_diffs_seconds.empty else []
        
        # Add to results
        time_metrics.append({
            'user': user,
            'avg_time_between_scans': avg_time_between_scans,
            'time_since_last_scan': time_since_last_scan,
            'last_scan_time': last_scan_time,
            'time_diff_list': time_diff_list,
            'scan_count': len(group)
        })
    
    # Convert to DataFrame
    if time_metrics:
        df_time_metrics = pd.DataFrame(time_metrics)
        
        # Convert seconds to minutes for readability
        df_time_metrics['avg_time_between_scans_minutes'] = df_time_metrics['avg_time_between_scans'] / 60
        df_time_metrics['time_since_last_scan_minutes'] = df_time_metrics['time_since_last_scan'] / 60
        
        # Round to 2 decimal places
        df_time_metrics['avg_time_between_scans_minutes'] = df_time_metrics['avg_time_between_scans_minutes'].round(2)
        df_time_metrics['time_since_last_scan_minutes'] = df_time_metrics['time_since_last_scan_minutes'].round(2)
        
        return df_time_metrics
    else:
        return pd.DataFrame()

def extract_timestamp_from_filename(filename):
    """
    Extract timestamp from filename in format YYYYMMDDHHMMSS.
    
    Args:
        filename (str): Filename containing timestamp
        
    Returns:
        datetime: Extracted timestamp as datetime object, or None if not found
    """
    import re
    
    # Try to extract a timestamp pattern (8 digits for date + 6 digits for time)
    match = re.search(r'(\d{8})(\d{6})', filename)
    if match:
        date_str, time_str = match.groups()
        try:
            # Parse the timestamp
            timestamp_str = f"{date_str}_{time_str}"
            return pd.to_datetime(timestamp_str, format="%Y%m%d_%H%M%S")
        except Exception as e:
            logging.error(f"Error parsing timestamp from filename {filename}: {e}")
    
    return None

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
    
    # Extract timestamp from filenames to use as reference time
    serial_timestamp = extract_timestamp_from_filename(serial_file.name)
    delivery_timestamp = extract_timestamp_from_filename(delivery_file.name)
    
    # Use the most recent timestamp as the reference time
    reference_time = None
    if serial_timestamp and delivery_timestamp:
        reference_time = max(serial_timestamp, delivery_timestamp)
        logging.info(f"Using reference time from filenames: {reference_time}")
    elif serial_timestamp:
        reference_time = serial_timestamp
        logging.info(f"Using reference time from serial file: {reference_time}")
    elif delivery_timestamp:
        reference_time = delivery_timestamp
        logging.info(f"Using reference time from delivery file: {reference_time}")

    # 2. Read Excel files
    try:
        df_serial = pd.read_excel(serial_file)
        df_delivery = pd.read_excel(delivery_file)
    except Exception as e:
        logging.error(f"Error reading Excel files: {e}")
        return

    # 3. Filter and process serial data
    # Apply filters in sequence
    df_serial = filter_by_warehouse(df_serial, config.WAREHOUSE_FILTER)
    df_serial = filter_by_parent_serial(df_serial)
    
    # Ensure timestamp column exists
    ensure_timestamp_column(df_serial)
    
    # Sort by timestamp in descending order (newest first)
    if 'Timestamp' in df_serial.columns:
        df_serial = df_serial.sort_values('Timestamp', ascending=False)
        logging.info("Sorted serial data by timestamp in descending order (newest first)")
    
    # Deduplicate by serial number and status
    df_serial = deduplicate_serial_status(df_serial)
    
    # Filter by time window
    if reference_time:
        df_serial = filter_by_time_window(df_serial, reference_time, config.WINDOW_MINUTES)
    
    # Map status codes to text
    df_serial['StatusText'] = df_serial['Status'].map(config.STATUS_MAPPING)
    
    # Process ASH and SHP statuses
    df_serial, ash_users = process_ash_shp_statuses(df_serial)
    
    # Apply timestamp ceiling for serials where SHP is the last status
    df_serial = apply_shp_ceiling_timestamp(df_serial)

    # 4. Create a base aggregation by user and delivery
    agg = df_serial.groupby(['Created by', 'Delivery']).size().reset_index(name='total_records')
    
    # Get status breakdown (shipped vs picked counts)
    # This gives us counts by status for each user-delivery combination
    status_counts = df_serial.groupby(['Created by', 'Delivery', 'StatusText']).size().unstack(fill_value=0)
    
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
    
    # Drop the total_records column as it's not needed
    agg = agg.drop('total_records', axis=1)
    
    # 5. Add total delivery packages from VL06O
    # Sanitize delivery numbers in both dataframes to ensure consistent format
    agg['delivery_clean'] = agg['Delivery'].apply(sanitize_delivery_number)
    df_delivery['delivery_clean'] = df_delivery['Delivery'].apply(sanitize_delivery_number)
    
    # Check for duplicate delivery numbers in delivery data
    delivery_clean_counts = df_delivery['delivery_clean'].value_counts()
    duplicate_deliveries = delivery_clean_counts[delivery_clean_counts > 1].index.tolist()
    
    if duplicate_deliveries:
        logging.warning(f"Found {len(duplicate_deliveries)} duplicate delivery_clean values in df_delivery")
        
        # For each delivery, keep only one row with the total package count
        # Group by delivery_clean and take the first row for each group
        df_delivery_deduped = df_delivery.groupby('delivery_clean').first().reset_index()
        
        logging.info(f"Deduplicated delivery data. Shape before: {df_delivery.shape}, after: {df_delivery_deduped.shape}")
        
        # Merge delivery totals using the sanitized delivery numbers
        df_delivery_matched = df_delivery_deduped[['delivery_clean', 'Number of packages']]
        
        logging.info(f"Created package counts based on matching materials. Shape: {df_delivery_matched.shape}")
        
        # Merge delivery totals using the sanitized delivery numbers and matched package counts
        agg = agg.merge(df_delivery_matched, on='delivery_clean', how='left')
    else:
        # If no duplicates, proceed as before
        agg = agg.merge(df_delivery[['delivery_clean', 'Number of packages']], 
                       on='delivery_clean', how='left')
    
    agg = agg.rename(columns={'Number of packages': 'delivery_total_packages'})
    
    # Drop the temporary column used for merging
    agg = agg.drop('delivery_clean', axis=1)
    
    # 6. Calculate progress metrics
    # For progress calculation, consider ASH (assigned to shipper) as "picked"
    # If all items are shipped, progress should be 100%
    agg['scanned_packages'] = agg['assigned to shipper_count']
    
    # Calculate progress percentage
    # Only calculate where delivery_total_packages is not null
    agg['progress_percentage'] = 0.0  # Default to 0
    mask = ~agg['delivery_total_packages'].isna()
    
    # Calculate progress percentage based on scanned packages vs total packages
    # Add safeguard for division by zero
    agg.loc[mask, 'progress_percentage'] = np.where(
        agg.loc[mask, 'delivery_total_packages'] > 0,
        (agg.loc[mask, 'scanned_packages'] / agg.loc[mask, 'delivery_total_packages']) * 100,
        0
    )
    
    # If all items are shipped, set progress to 100%
    all_shipped_mask = (agg['shipped_count'] > 0) & (agg['assigned to shipper_count'] == 0)
    agg.loc[all_shipped_mask, 'progress_percentage'] = 100.0
    
    # Round percentage to 2 decimal places
    agg['progress_percentage'] = agg['progress_percentage'].round(2)
    
    # 7. Calculate time metrics for users
    # Only calculate time metrics for users who do ASH
    # Note: We need to use the original dataframe before deduplication for time metrics
    # to ensure we have enough data points for each user
    
    # First, get the original dataframe before deduplication
    # We need to recreate it from the Excel file
    try:
        df_serial_original = pd.read_excel(serial_file)
        
        # Apply the same filters as before
        df_serial_original = filter_by_warehouse(df_serial_original, config.WAREHOUSE_FILTER)
        df_serial_original = filter_by_parent_serial(df_serial_original)
        ensure_timestamp_column(df_serial_original)
        
        # Filter by time window
        if reference_time:
            df_serial_original = filter_by_time_window(df_serial_original, reference_time, config.WINDOW_MINUTES)
        
        # Get ASH users from the original dataframe
        ash_users_original = set(df_serial_original[df_serial_original['Status'] == 'ASH']['Created by'])
        
        # Calculate time metrics using the original dataframe
        df_serial_ash_users = df_serial_original[df_serial_original['Created by'].isin(ash_users_original)]
        time_metrics_df = calculate_time_metrics(df_serial_ash_users, reference_time)
        
        logging.info(f"Calculated time metrics using original dataframe with {len(df_serial_original)} records")
        logging.info(f"Time metrics calculated for {len(time_metrics_df)} users")
        
    except Exception as e:
        logging.error(f"Error calculating time metrics: {e}")
        time_metrics_df = pd.DataFrame()
    
    # Add the is_ash_user flag to the aggregated data
    agg['is_ash_user'] = agg['Created by'].isin(ash_users)
    
    # 8. Rename columns for final output
    agg = agg.rename(columns={'Created by': 'user', 'Delivery': 'delivery'})
    
    # 9. Compare with previous output to detect changes
    latest_out = get_latest_file(config.OUT_DIR, "output_*.parquet")
    changed = True
    
    if latest_out:
        try:
            prev_df = pd.read_parquet(latest_out)
            
            # Compare relevant columns (excluding time-based ones which will always change)
            # Get columns that exist in both dataframes
            common_cols = [col for col in agg.columns if col in prev_df.columns and not any(
                time_col in col for time_col in ['time_', 'last_scan'])]
            
            # Sort both DataFrames for stable comparison
            prev_df_sorted = prev_df[common_cols].sort_values(['user', 'delivery']).reset_index(drop=True)
            new_df_sorted = agg[common_cols].sort_values(['user', 'delivery']).reset_index(drop=True)
            
            if new_df_sorted.equals(prev_df_sorted):
                changed = False
                logging.info("No significant changes detected in data")
        except Exception as e:
            logging.error(f"Error comparing with previous output: {e}")
    
    # 10. Write new output if changes detected
    if changed:
        # Ensure output directory exists
        Path(config.OUT_DIR).mkdir(exist_ok=True)
        
        # Generate timestamped filename
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path(config.OUT_DIR) / f"output_{timestamp}.parquet"
        time_metrics_path = Path(config.OUT_DIR) / f"time_metrics_{timestamp}.parquet"
        
        # Write to Parquet
        agg.to_parquet(out_path)
        logging.info(f"Wrote output file: {out_path}")
        
        # Write time metrics to a separate Parquet file if we have data
        if not time_metrics_df.empty:
            time_metrics_df.to_parquet(time_metrics_path)
            logging.info(f"Wrote time metrics file: {time_metrics_path}")
        
        # 11. Cleanup old outputs (keep last 5)
        cleanup_old_files(config.OUT_DIR, "output_*.parquet", 5)
        cleanup_old_files(config.OUT_DIR, "time_metrics_*.parquet", 5)
    else:
        logging.info("No changes detected, skipping output generation")

def cleanup_old_files(directory, pattern, keep_count):
    """
    Cleanup old files matching the pattern, keeping only the specified number of most recent files.
    
    Args:
        directory (str): Directory to clean up
        pattern (str): Glob pattern to match files
        keep_count (int): Number of most recent files to keep
    """
    files = sorted(Path(directory).glob(pattern), 
                  key=lambda f: f.stat().st_mtime, reverse=True)
    
    for old_file in files[keep_count:]:
        try:
            old_file.unlink()
            logging.info(f"Removed old file: {old_file.name}")
        except Exception as e:
            logging.warning(f"Failed to remove {old_file.name}: {e}")

def main():
    """
    Main function to run the shipment tracking process at regular intervals.
    Also runs visualization after a short delay.
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
        
        # Wait 10 seconds before running visualization
        logging.info("Waiting 10 seconds before running visualization...")
        time.sleep(10)
        
        # Run visualization if enabled (default: True)
        run_visualization = getattr(config, 'RUN_VISUALIZATION', True)
        if run_visualization:
            try:
                # Import visualization functions here to avoid circular imports
                import visualize_results
                logging.info("Running visualization...")
                visualize_results.main()
                logging.info("Visualization complete.")
            except Exception as e:
                logging.exception("Unexpected error in visualization")
        else:
            logging.info("Visualization disabled in config")
        
        # Calculate remaining sleep time
        remaining_sleep = config.INTERVAL_SECONDS - 10
        if remaining_sleep > 0:
            logging.info(f"Sleeping for remaining {remaining_sleep} seconds...")
            time.sleep(remaining_sleep)

if __name__ == "__main__":
    main()
