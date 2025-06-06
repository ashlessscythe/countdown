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

Command line options:
    --run-once    Run the process once and exit
    --no-wait     Disable the 10-second wait before visualization
"""

import os
import time
import logging
import re
import argparse
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

def sanitize_column_headers(df):
    """
    Sanitize column headers and convert them to snake_case.
    
    Args:
        df (DataFrame): DataFrame with headers to sanitize
        
    Returns:
        DataFrame: DataFrame with sanitized headers
    """
    # Create a mapping of original column names to sanitized names
    sanitized_columns = {}
    
    for col in df.columns:
        # Convert to string in case it's not already
        col_str = str(col)
        
        # Replace special characters and spaces with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9]', '_', col_str)
        
        # Convert to lowercase
        sanitized = sanitized.lower()
        
        # Replace multiple underscores with a single underscore
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remove leading and trailing underscores
        sanitized = sanitized.strip('_')
        
        # If empty or starts with a digit, prepend 'col_'
        if not sanitized or sanitized[0].isdigit():
            sanitized = f"col_{sanitized}"
            
        # Handle duplicate column names by appending a number
        base_sanitized = sanitized
        counter = 1
        while sanitized in sanitized_columns.values():
            sanitized = f"{base_sanitized}_{counter}"
            counter += 1
            
        sanitized_columns[col] = sanitized
    
    # Rename the columns
    df = df.rename(columns=sanitized_columns)
    
    # Log the column name changes
    logging.info("Sanitized column headers to snake_case format")
    for original, sanitized in sanitized_columns.items():
        if original != sanitized:
            logging.debug(f"Column renamed: '{original}' -> '{sanitized}'")
    
    return df

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
    if 'timestamp' not in df.columns:
        # Check for original or sanitized column names
        created_on_col = next((col for col in df.columns if col in ['created_on', 'Created on']), None)
        time_col = next((col for col in df.columns if col in ['time', 'Time']), None)
        
        if created_on_col and time_col:
            logging.info(f"Creating timestamp column from '{created_on_col}' and '{time_col}' columns")
            
            # First ensure both columns are strings
            created_on_str = df[created_on_col].astype(str)
            time_str = df[time_col].astype(str)
            
            # Combine date and time columns to create a timestamp
            df['timestamp'] = created_on_str + ' ' + time_str
            
            try:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                logging.info(f"Converted timestamps to datetime. Sample: {df['timestamp'].head(3)}")
                return True
            except Exception as e:
                logging.error(f"Error converting timestamp to datetime: {e}")
                return False
        else:
            logging.warning(f"Cannot create timestamp: Required columns missing. Available columns: {df.columns.tolist()}")
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
    
    # Check for original or sanitized column name
    warehouse_col = next((col for col in df.columns if col in ['warehouse_number', 'Warehouse Number']), None)
    
    if warehouse_col:
        df_filtered = df[df[warehouse_col] == warehouse_code]
        logging.info(f"Filtered data to warehouse {warehouse_code}. length before: {pre_length}, after: {len(df_filtered)}")
        return df_filtered
    else:
        logging.warning(f"Warehouse column not found. Available columns: {df.columns.tolist()}")
        return df
    
def filter_by_parent_serial(df):
    """
    Filter dataframe to only include records with no parent serial number.
    
    Args:
        df (DataFrame): DataFrame to filter
        
    Returns:
        DataFrame: Filtered DataFrame
    """
    pre_length = len(df)
    
    # Check for original or sanitized column name
    parent_serial_col = next((col for col in df.columns if col in ['parent_serial_number', 'Parent serial number']), None)
    
    if parent_serial_col:
        df_filtered = df[df[parent_serial_col].isna() | (df[parent_serial_col] == '')]
        logging.info(f"Filtered data to only include parent serial numbers. length before: {pre_length}, after: {len(df_filtered)}")
        return df_filtered
    else:
        logging.warning(f"Parent serial column not found. Available columns: {df.columns.tolist()}")
        return df

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
    if 'timestamp' not in df.columns:
        if not ensure_timestamp_column(df):
            return df
    
    pre_length = len(df)
    cutoff_time = reference_time - pd.Timedelta(minutes=window_minutes)
    df_filtered = df[df['timestamp'] >= cutoff_time]
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
    
    # Check for original or sanitized column names
    serial_col = next((col for col in df.columns if col in ['serial', 'Serial #']), None)
    status_col = next((col for col in df.columns if col in ['status', 'Status']), None)
    
    if serial_col and status_col:
        df_deduped = df.drop_duplicates(subset=[serial_col, status_col], keep='first')
        logging.info(f"Deduplicated serial data by {serial_col} and {status_col} (keeping most recent timestamp)")
        logging.info(f"length before: {pre_length}, after: {len(df_deduped)}")
        return df_deduped
    else:
        logging.warning(f"Serial or Status column not found. Available columns: {df.columns.tolist()}")
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
    # Check for original or sanitized column names
    serial_col = next((col for col in df.columns if col in ['serial', 'Serial #']), None)
    status_col = next((col for col in df.columns if col in ['status', 'Status']), None)
    created_by_col = next((col for col in df.columns if col in ['created_by', 'Created by']), None)
    
    if not all([serial_col, status_col, created_by_col]):
        logging.warning(f"Required columns not found. Available columns: {df.columns.tolist()}")
        return df, set()
    
    # Identify users who do ASH (for filtering visualizations later)
    ash_users = set(df[df[status_col] == 'ASH'][created_by_col])
    
    # Find serials that have both ASH and SHP records
    # Group by Serial # and check if both ASH and SHP statuses exist
    serial_status_counts = df.groupby(serial_col)[status_col].apply(set)
    
    # Serials with both ASH and SHP
    serials_with_both = serial_status_counts[serial_status_counts.apply(lambda x: 'ASH' in x and 'SHP' in x)].index
    
    # Serials with only SHP (no corresponding ASH)
    serials_with_only_shp = serial_status_counts[serial_status_counts.apply(lambda x: 'SHP' in x and 'ASH' not in x)].index
    
    # Remove SHP records for serials that only have SHP (no corresponding ASH)
    df = df[~((df[serial_col].isin(serials_with_only_shp)) & 
             (df[status_col] == 'SHP'))]
    
    # For serials with both ASH and SHP, keep only the SHP records (count as shipped)
    df = df[~((df[serial_col].isin(serials_with_both)) & 
             (df[status_col] == 'ASH'))]
    
    # Add a flag to identify users who do ASH
    df['is_ash_user'] = df[created_by_col].isin(ash_users)
    
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
    # Check for original or sanitized column names
    serial_col = next((col for col in df.columns if col in ['serial', 'Serial #']), None)
    status_col = next((col for col in df.columns if col in ['status', 'Status']), None)
    
    if not all([serial_col, status_col]) or 'timestamp' not in df.columns:
        logging.warning(f"Required columns not found. Available columns: {df.columns.tolist()}")
        return df
    
    # Get serials with SHP status
    serials_with_shp = df[df[status_col] == 'SHP']
    
    if not serials_with_shp.empty:
        # Get the latest SHP timestamp for each serial
        latest_shp = serials_with_shp.groupby(serial_col)['timestamp'].max()
        
        # More efficient approach using vectorized operations
        # Create a temporary column with the SHP ceiling timestamp for each serial
        df['shp_ceiling'] = df[serial_col].map(latest_shp)
        
        # Keep only rows where either:
        # 1. The serial doesn't have a SHP ceiling timestamp, or
        # 2. The timestamp is not after the SHP ceiling
        df = df[(df['shp_ceiling'].isna()) | (df['timestamp'] <= df['shp_ceiling'])]
        
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
    
    # Check for original or sanitized column name
    created_by_col = next((col for col in df_serial.columns if col in ['created_by', 'Created by']), None)
    
    if not created_by_col:
        logging.warning(f"Created by column not found. Available columns: {df_serial.columns.tolist()}")
        return pd.DataFrame()
    
    # Sort by user and timestamp
    df_sorted = df_serial.sort_values([created_by_col, 'timestamp'])
    
    # Group by user to calculate time differences between consecutive scans
    time_metrics = []
    
    # Get reference time for calculations - ensure it's timezone-naive for consistent comparison
    if reference_time is None:
        now = pd.Timestamp.now().replace(tzinfo=None)
        logging.info(f"Using current system time for calculations: {now}")
    else:
        now = reference_time.replace(tzinfo=None)
        logging.info(f"Using reference time from filename for calculations: {now}")
    
    for user, group in df_sorted.groupby(created_by_col):
        # Skip if user has less than 2 scans
        if len(group) < 2:
            continue
            
        # Calculate time differences between consecutive scans
        group = group.sort_values('timestamp')
        
        # Ensure timestamps are timezone-naive for consistent comparison
        if group['timestamp'].iloc[0].tzinfo is not None:
            group['timestamp'] = group['timestamp'].dt.tz_localize(None)
            
        time_diffs = group['timestamp'].diff().dropna()
        
        # Convert time differences to seconds
        time_diffs_seconds = time_diffs.dt.total_seconds()
        
        # Calculate metrics with safeguards for empty dataframes
        if time_diffs_seconds.empty:
            avg_time_between_scans = 0
        else:
            avg_time_between_scans = time_diffs_seconds.mean()
            
        last_scan_time = group['timestamp'].max()
        
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
    - Save material completion data for visualization
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

    # 2. Read Excel files and sanitize column headers
    try:
        df_serial = pd.read_excel(serial_file)
        df_serial = sanitize_column_headers(df_serial)
        
        df_delivery = pd.read_excel(delivery_file)
        df_delivery = sanitize_column_headers(df_delivery)
        
        logging.info(f"Sanitized column headers for both dataframes")
        logging.info(f"Serial dataframe columns: {df_serial.columns.tolist()}")
        logging.info(f"Delivery dataframe columns: {df_delivery.columns.tolist()}")
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
    if 'timestamp' in df_serial.columns:
        df_serial = df_serial.sort_values('timestamp', ascending=False)
        logging.info("Sorted serial data by timestamp in descending order (newest first)")
    
    # Deduplicate by serial number and status
    df_serial = deduplicate_serial_status(df_serial)
    
    # Filter by time window
    if reference_time:
        df_serial = filter_by_time_window(df_serial, reference_time, config.WINDOW_MINUTES)
    
    # Check for original or sanitized column name
    status_col = next((col for col in df_serial.columns if col in ['status', 'Status']), None)
    
    if status_col:
        # Map status codes to text
        df_serial['status_text'] = df_serial[status_col].map(config.STATUS_MAPPING)
        
        # Process ASH and SHP statuses
        df_serial, ash_users = process_ash_shp_statuses(df_serial)
        
        # Apply timestamp ceiling for serials where SHP is the last status
        df_serial = apply_shp_ceiling_timestamp(df_serial)
    else:
        logging.warning(f"Status column not found. Available columns: {df_serial.columns.tolist()}")
        ash_users = set()

    # 4. Create a base aggregation by user and delivery
    # Check for original or sanitized column names
    created_by_col = next((col for col in df_serial.columns if col in ['created_by', 'Created by']), None)
    delivery_col = next((col for col in df_serial.columns if col in ['delivery', 'Delivery']), None)
    
    if not all([created_by_col, delivery_col]):
        logging.warning(f"Required columns not found. Available columns: {df_serial.columns.tolist()}")
        return
    
    agg = df_serial.groupby([created_by_col, delivery_col]).size().reset_index(name='total_records')
    
    # Get status breakdown (shipped vs picked counts)
    # This gives us counts by status for each user-delivery combination
    status_counts = df_serial.groupby([created_by_col, delivery_col, 'status_text']).size().unstack(fill_value=0)
    
    # Ensure both status columns exist, even if no data
    for status in config.STATUS_MAPPING.values():
        if status not in status_counts.columns:
            status_counts[status] = 0
    
    # Rename columns for clarity
    status_counts.columns = [f"{col.replace(' / ', '_')}_count" for col in status_counts.columns]
    status_counts = status_counts.reset_index()
    
    # Merge status counts with the aggregated data
    agg = agg.merge(status_counts, on=[created_by_col, delivery_col], how='left')
    agg = agg.fillna(0)  # Fill NaN values with 0 for status counts
    
    # Drop the total_records column as it's not needed
    agg = agg.drop('total_records', axis=1)
    
    # 5. Add total delivery packages from VL06O
    # Sanitize delivery numbers in both dataframes to ensure consistent format
    agg['delivery_clean'] = agg[delivery_col].apply(sanitize_delivery_number)
    
    # Check for original or sanitized column name for delivery in df_delivery
    delivery_col_delivery = next((col for col in df_delivery.columns if col in ['delivery', 'Delivery']), None)
    
    if delivery_col_delivery:
        df_delivery['delivery_clean'] = df_delivery[delivery_col_delivery].apply(sanitize_delivery_number)
        
        # Check for duplicate delivery numbers in delivery data
        delivery_clean_counts = df_delivery['delivery_clean'].value_counts()
        duplicate_deliveries = delivery_clean_counts[delivery_clean_counts > 1].index.tolist()
        
        # Check for original or sanitized column name for number of packages
        packages_col = next((col for col in df_delivery.columns if col in ['number_of_packages', 'Number of packages']), None)
        
        # Check for overall pick status column
        overall_pick_status_col = 'overall_pick_status' if 'overall_pick_status' in df_delivery.columns else None
        
        if packages_col:
            if duplicate_deliveries:
                logging.warning(f"Found {len(duplicate_deliveries)} duplicate delivery_clean values in df_delivery")
                
                # For each delivery, keep only one row with the total package count
                # Group by delivery_clean and take the first row for each group
                df_delivery_deduped = df_delivery.groupby('delivery_clean').first().reset_index()
                
                logging.info(f"Deduplicated delivery data. Shape before: {df_delivery.shape}, after: {df_delivery_deduped.shape}")
                
                # Merge delivery totals using the sanitized delivery numbers
                df_delivery_matched = df_delivery_deduped[['delivery_clean', packages_col]]

                # merge overall pick status if it exists
                if overall_pick_status_col:
                    df_delivery_matched = df_delivery_matched.merge(
                        df_delivery_deduped[['delivery_clean', overall_pick_status_col]],
                        on='delivery_clean', how='left'
                    )
                
                logging.info(f"Created package counts based on matching materials. Shape: {df_delivery_matched.shape}")
                
                # Merge delivery totals using the sanitized delivery numbers and matched package counts
                agg = agg.merge(df_delivery_matched, on='delivery_clean', how='left')
            else:
                # If no duplicates, proceed as before
                agg = agg.merge(df_delivery[['delivery_clean', packages_col]], 
                               on='delivery_clean', how='left')
            
            agg = agg.rename(columns={packages_col: 'delivery_total_packages'})
        else:
            logging.warning(f"Number of packages column not found. Available columns: {df_delivery.columns.tolist()}")
    else:
        logging.warning(f"Delivery column not found in delivery dataframe. Available columns: {df_delivery.columns.tolist()}")
    
    # Drop the temporary column used for merging
    if 'delivery_clean' in agg.columns:
        agg = agg.drop('delivery_clean', axis=1)
    
    # 6. Calculate progress metrics
    # For progress calculation, consider both ASH and SHP as "scanned"
    # If all items are shipped, progress should be 100%
    
    # Initialize scanned_packages with sum of ASH and SHP counts
    agg['scanned_packages'] = agg['assigned to shipper_count'] + agg['shipped_count']
    
    # Calculate progress percentage
    # Only calculate where delivery_total_packages is not null
    agg['progress_percentage'] = 0.0  # Default to 0
    mask = ~agg['delivery_total_packages'].isna()
    
    # Check for original or sanitized column names for qty and material
    qty_col = next((col for col in df_serial.columns if col in ['qty', 'Qty']), None)
    material_col = next((col for col in df_serial.columns if col in ['material_number', 'Material Number']), None)
    
    # Check for original or sanitized column names for delivery_qty and material in df_delivery
    delivery_qty_col = next((col for col in df_delivery.columns if col in ['delivery_quantity', 'actual_delivery_qty', 'Delivery Quantity', 'Actual Delivery Qty']), None)
    delivery_material_col = next((col for col in df_delivery.columns if col in ['material', 'Material']), None)
    
    # Add a column to track if scanned exceeds expected
    agg['scanned_exceeds_expected'] = False
    
    # Only proceed with detailed comparison if we have all required columns
    if all([qty_col, material_col, delivery_qty_col, delivery_material_col]):
        logging.info(f"Found all required columns for detailed quantity comparison")
        
        # Group by delivery and material in df_serial to get total qty for each material in each delivery
        if delivery_col in df_serial.columns and material_col in df_serial.columns and qty_col in df_serial.columns:
            # Create a mapping of delivery to a dict of material -> total qty
            delivery_material_qty = {}
            
            # Group by delivery and material, and sum the quantities
            serial_grouped = df_serial.groupby([delivery_col, material_col])[qty_col].sum().reset_index()
            
            # Store in a nested dictionary for easy lookup
            for _, row in serial_grouped.iterrows():
                delivery = row[delivery_col]
                material = row[material_col]
                qty = row[qty_col]
                
                sanitized_delivery = sanitize_delivery_number(delivery)
                if sanitized_delivery:
                    if sanitized_delivery not in delivery_material_qty:
                        delivery_material_qty[sanitized_delivery] = {}
                    
                    delivery_material_qty[sanitized_delivery][material] = qty
            
            # Create a similar mapping for df_delivery
            delivery_material_delivery_qty = {}
            
            # Group by delivery and material, and get the delivery quantities
            for _, row in df_delivery.iterrows():
                delivery = row[delivery_col_delivery]
                material = row[delivery_material_col]
                delivery_qty = row[delivery_qty_col]
                
                sanitized_delivery = sanitize_delivery_number(delivery)
                if sanitized_delivery:
                    if sanitized_delivery not in delivery_material_delivery_qty:
                        delivery_material_delivery_qty[sanitized_delivery] = {}
                    
                    delivery_material_delivery_qty[sanitized_delivery][material] = delivery_qty
            
            # Compare quantities for each delivery-material combination
            for idx, row in agg.iterrows():
                delivery_num = sanitize_delivery_number(row[delivery_col])
                if delivery_num:
                    # Get all records for this delivery from serial_df
                    delivery_serials = df_serial[df_serial[delivery_col] == row[delivery_col]]
                    
                    # Skip if no serials found
                    if delivery_serials.empty:
                        continue
                    
                    # Get unique materials for this delivery
                    materials = delivery_serials[material_col].unique()
                    
                    # Track if any material exceeds expected
                    exceeds_expected = False
                    
                    # Track if all materials have been scanned
                    all_materials_scanned = True
                    
                    # Get all materials from delivery_df for this delivery
                    delivery_materials = set()
                    for d_num, mat_dict in delivery_material_delivery_qty.items():
                        if d_num == delivery_num:
                            delivery_materials.update(mat_dict.keys())
                    
                    # Calculate total qty for all materials in serial_df and delivery_df
                    total_serial_qty = 0
                    total_delivery_qty = 0
                    
                    # Check if all materials in delivery_df have been scanned
                    for material in delivery_materials:
                        if pd.isna(material):
                            continue
                            
                        # Get serial qty for this material
                        serial_qty = delivery_material_qty.get(delivery_num, {}).get(material, 0)
                        
                        # Get delivery qty for this material
                        delivery_qty = delivery_material_delivery_qty.get(delivery_num, {}).get(material, 0)
                        
                        # Add to totals
                        total_serial_qty += serial_qty
                        total_delivery_qty += delivery_qty
                        
                        # If serial qty exceeds delivery qty, mark it
                        if serial_qty > delivery_qty and delivery_qty > 0:
                            exceeds_expected = True
                    
                    # If total serial qty equals or exceeds total delivery qty, consider all materials scanned
                    if total_serial_qty >= total_delivery_qty and total_delivery_qty > 0:
                        all_materials_scanned = True
                    else:
                        all_materials_scanned = False
                    
                    # Mark the row if any material exceeds expected
                    if exceeds_expected:
                        agg.at[idx, 'scanned_exceeds_expected'] = True
                    
                    # If all materials have been scanned, set scanned_packages to delivery_total_packages
                    if all_materials_scanned and not pd.isna(row['delivery_total_packages']):
                        agg.at[idx, 'scanned_packages'] = row['delivery_total_packages']
    else:
        logging.warning(f"Missing required columns for detailed quantity comparison. Using simpler approach.")
        
        # Fallback to simpler approach if we don't have all required columns
        delivery_counts = {}
        
        # Group by delivery to get total counts from serial_df
        if delivery_col in df_serial.columns:
            serial_delivery_counts = df_serial.groupby(delivery_col).size().to_dict()
            
            # Store sanitized delivery numbers for comparison
            for delivery, count in serial_delivery_counts.items():
                sanitized_delivery = sanitize_delivery_number(delivery)
                if sanitized_delivery:
                    delivery_counts[sanitized_delivery] = count
        
        # Compare counts for each delivery
        for idx, row in agg.iterrows():
            delivery_num = sanitize_delivery_number(row[delivery_col])
            if delivery_num and delivery_num in delivery_counts:
                serial_count = delivery_counts[delivery_num]
                delivery_count = row['delivery_total_packages'] if not pd.isna(row['delivery_total_packages']) else 0
                
                # If serial count exceeds delivery count, mark it
                if serial_count > delivery_count and delivery_count > 0:
                    agg.at[idx, 'scanned_exceeds_expected'] = True
    
    # For rows where scanned exceeds expected, calculate percentage based on serial counts
    exceeds_mask = agg['scanned_exceeds_expected'] & mask
    
    # For normal cases, calculate progress as before
    agg.loc[mask & ~exceeds_mask, 'progress_percentage'] = np.where(
        agg.loc[mask & ~exceeds_mask, 'delivery_total_packages'] > 0,
        (agg.loc[mask & ~exceeds_mask, 'scanned_packages'] / agg.loc[mask & ~exceeds_mask, 'delivery_total_packages']) * 100,
        0
    )
    
    # For cases where scanned_packages equals delivery_total_packages, set progress to 100%
    equal_mask = (agg['scanned_packages'] == agg['delivery_total_packages']) & (agg['delivery_total_packages'] > 0)
    agg.loc[equal_mask, 'progress_percentage'] = 100.0
    
    # For cases where scanned exceeds expected, calculate based on status and qty in serial_df
    for idx, row in agg[exceeds_mask].iterrows():
        delivery_num = sanitize_delivery_number(row[delivery_col])
        if delivery_num:
            # Get all records for this delivery from serial_df
            delivery_serials = df_serial[df_serial[delivery_col] == row[delivery_col]]
            
            # Skip if no serials found
            if delivery_serials.empty:
                continue
            
            # If we have qty column, use it for weighted calculation
            if qty_col in delivery_serials.columns:
                # Group by status and sum the quantities
                status_qty = delivery_serials.groupby(status_col)[qty_col].sum()
                
                # Calculate total qty (both ASH and SHP)
                total_qty = status_qty.sum()
                
                # Calculate shipped and picked quantities
                shipped_qty = status_qty.get('SHP', 0)
                ash_qty = status_qty.get('ASH', 0)
                
                # Calculate progress based on status and qty
                if total_qty > 0:
                    # If we have both ASH and SHP, calculate percentage based on both
                    progress = min(100, ((ash_qty + shipped_qty) / total_qty) * 100)
                    agg.at[idx, 'progress_percentage'] = progress
            else:
                # Fallback to count-based calculation if qty column not available
                status_counts = delivery_serials[status_col].value_counts()
                
                # Calculate total scanned (both ASH and SHP)
                total_scanned = status_counts.sum()
                
                # Calculate shipped and picked counts
                shipped_count = status_counts.get('SHP', 0)
                ash_count = status_counts.get('ASH', 0)
                
                # Calculate progress based on status
                if total_scanned > 0:
                    # If we have both ASH and SHP, calculate percentage based on both
                    progress = min(100, ((ash_count + shipped_count) / total_scanned) * 100)
                    agg.at[idx, 'progress_percentage'] = progress
    
    # If all items are shipped, set progress to 100%
    all_shipped_mask = (agg['shipped_count'] > 0) & (agg['assigned to shipper_count'] == 0)
    agg.loc[all_shipped_mask, 'progress_percentage'] = 100.0
    
    # Clean up temporary column
    if 'scanned_exceeds_expected' in agg.columns:
        agg = agg.drop('scanned_exceeds_expected', axis=1)
    
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
        
        # Add median time between scans for better outlier handling
        if not time_metrics_df.empty:
            time_metrics_df['median_time_between_scans_minutes'] = time_metrics_df['time_diff_list'].apply(
                lambda x: np.median(x) / 60 if x else 0
            ).round(2)
        
        logging.info(f"Calculated time metrics using original dataframe with {len(df_serial_original)} records")
        logging.info(f"Time metrics calculated for {len(time_metrics_df)} users")
        
    except Exception as e:
        logging.error(f"Error calculating time metrics: {e}")
        time_metrics_df = pd.DataFrame()
    
    # Add the is_ash_user flag to the aggregated data
    agg['is_ash_user'] = agg['created_by'].isin(ash_users)
    
    # 8. Rename columns for final output
    agg = agg.rename(columns={'created_by': 'user', 'delivery': 'delivery'})
    
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
    
    # 10. Generate material completion data for visualization
    material_completion_df = None
    try:
        # Check if we have all required columns for material completion
        if all([qty_col, material_col, delivery_col, status_col]):
            logging.info("Generating material completion data for visualization")
            
            # First, check serial counts against package counts
            serial_counts = {}
            package_counts = {}
            
            # Get serial counts (ASH status only)
            df_serial_ash = df_serial[df_serial[status_col] == 'ASH']
            df_serial_ash['delivery_clean'] = df_serial_ash[delivery_col].apply(sanitize_delivery_number)
            
            # Count unique serials per delivery
            serial_counts = df_serial_ash.groupby('delivery_clean').size().to_dict()
            
            # Get package counts from delivery data
            if all([delivery_col_delivery, packages_col]):
                df_delivery['delivery_clean'] = df_delivery[delivery_col_delivery].apply(sanitize_delivery_number)
                package_counts = df_delivery.groupby('delivery_clean')[packages_col].sum().to_dict()
            
            # Find common deliveries
            common_deliveries = set(serial_counts.keys()) & set(package_counts.keys())
            logging.info(f"Found {len(common_deliveries)} common deliveries for serial vs package comparison")
            
            # Check if serial counts make sense compared to package counts
            serial_package_matches = []
            for delivery in common_deliveries:
                serial_count = serial_counts[delivery]
                package_count = package_counts[delivery]
                
                # If serial count is close to or exceeds package count, use serial count
                if serial_count >= package_count * 0.9:  # Allow 10% margin
                    serial_package_matches.append({
                        'delivery': delivery,
                        'count_type': 'serial',
                        'count': serial_count,
                        'package_count': package_count
                    })
                else:
                    # If counts don't match, we'll use material-level completion
                    serial_package_matches.append({
                        'delivery': delivery,
                        'count_type': 'material',
                        'count': serial_count,
                        'package_count': package_count
                    })
            
            # Convert to DataFrame for analysis
            serial_package_df = pd.DataFrame(serial_package_matches)
            logging.info(f"Serial counts used for {len(serial_package_df[serial_package_df['count_type'] == 'serial'])} deliveries")
            logging.info(f"Material completion used for {len(serial_package_df[serial_package_df['count_type'] == 'material'])} deliveries")
            
            # Now proceed with material completion for deliveries where serial counts don't make sense
            material_deliveries = serial_package_df[serial_package_df['count_type'] == 'material']['delivery'].tolist()
            
            # Create a mapping of delivery to material quantities from delivery data
            delivery_material_qty = {}
            
            # Group by delivery and material in delivery data
            if all([delivery_col_delivery, delivery_material_col, delivery_qty_col]):
                # Sanitize delivery numbers in delivery data
                df_delivery['delivery_clean'] = df_delivery[delivery_col_delivery].apply(sanitize_delivery_number)
                
                # Filter for material-level deliveries only
                df_delivery_material = df_delivery[df_delivery['delivery_clean'].isin(material_deliveries)]
                
                # Group by material and delivery, sum the quantities
                material_delivery_qty = df_delivery_material.groupby([delivery_material_col, 'delivery_clean'])[delivery_qty_col].sum().reset_index()
                
                # Store in dictionary for easy lookup
                for _, row in material_delivery_qty.iterrows():
                    delivery = row['delivery_clean']
                    material = str(row[delivery_material_col]).split('.')[0]  # Remove decimal part
                    qty = row[delivery_qty_col]
                    
                    if delivery not in delivery_material_qty:
                        delivery_material_qty[delivery] = {}
                    
                    delivery_material_qty[delivery][material] = qty
                
                logging.info(f"Processed delivery quantities for {len(delivery_material_qty)} deliveries")
            
            # Process serial data to get ASH quantities for material-level deliveries
            serial_material_qty = {}
            
            # Filter serial data for material-level deliveries
            df_serial_material = df_serial_ash[df_serial_ash['delivery_clean'].isin(material_deliveries)]
            
            # Group by delivery, material and sum quantities
            serial_grouped = df_serial_material.groupby(['delivery_clean', material_col])[qty_col].sum().reset_index()
            
            # Store in dictionary for easy lookup
            for _, row in serial_grouped.iterrows():
                delivery = row['delivery_clean']
                material = str(row[material_col]).split('.')[0]  # Remove decimal part
                qty = row[qty_col]
                
                if delivery not in serial_material_qty:
                    serial_material_qty[delivery] = {}
                
                serial_material_qty[delivery][material] = qty
            
            logging.info(f"Processed serial quantities for {len(serial_material_qty)} deliveries")
            
            # For each delivery, match materials and calculate completion
            material_matches = []
            
            for delivery in material_deliveries:
                if delivery in delivery_material_qty and delivery in serial_material_qty:
                    delivery_materials = delivery_material_qty[delivery]
                    serial_materials = serial_material_qty[delivery]
                    
                    # Try to match materials by checking if one is a substring of the other
                    for delivery_material, delivery_qty in delivery_materials.items():
                        for serial_material, serial_qty in serial_materials.items():
                            # Check if the material numbers are similar (one is a substring of the other)
                            if delivery_material in serial_material or serial_material in delivery_material:
                                material_matches.append({
                                    'delivery': delivery,
                                    'material': delivery_material,
                                    'expected_qty': delivery_qty,
                                    'scanned_qty': serial_qty,
                                    'count_type': 'material'
                                })
            
            # Add serial-level completions
            for delivery in serial_package_df[serial_package_df['count_type'] == 'serial']['delivery']:
                material_matches.append({
                    'delivery': delivery,
                    'material': 'ALL',
                    'expected_qty': package_counts[delivery],
                    'scanned_qty': serial_counts[delivery],
                    'count_type': 'serial'
                })
            
            # Convert to DataFrame
            if material_matches:
                material_completion_df = pd.DataFrame(material_matches)
                
                # Calculate completion percentage with validation for negative quantities
                material_completion_df['completion_percentage'] = np.where(
                    material_completion_df['expected_qty'] > 0,
                    (material_completion_df['scanned_qty'] / material_completion_df['expected_qty'] * 100).clip(0, 100),
                    0
                )
                
                # Add additional metrics
                material_completion_df['remaining_qty'] = material_completion_df['expected_qty'] - material_completion_df['scanned_qty']
                material_completion_df['remaining_qty'] = material_completion_df['remaining_qty'].clip(lower=0)  # Ensure non-negative
                
                logging.info(f"Generated completion data with {len(material_completion_df)} records")
                logging.info(f"Average completion percentage: {material_completion_df['completion_percentage'].mean():.2f}%")
                logging.info(f"Serial-level completions: {len(material_completion_df[material_completion_df['count_type'] == 'serial'])}")
                logging.info(f"Material-level completions: {len(material_completion_df[material_completion_df['count_type'] == 'material'])}")
            else:
                logging.warning("No matching materials found for completion visualization")
        else:
            logging.warning("Missing required columns for completion calculation")
    except Exception as e:
        logging.error(f"Error generating completion data: {e}")
    
    # 11. Write new output if changes detected
    if changed:
        # Ensure output directory exists
        Path(config.OUT_DIR).mkdir(exist_ok=True)
        
        # Generate timestamped filename
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path(config.OUT_DIR) / f"output_{timestamp}.parquet"
        time_metrics_path = Path(config.OUT_DIR) / f"time_metrics_{timestamp}.parquet"
        material_completion_path = Path(config.OUT_DIR) / f"material_completion_{timestamp}.parquet"
        
        # Write to Parquet
        agg.to_parquet(out_path)
        logging.info(f"Wrote output file: {out_path}")
        
        # Write time metrics to a separate Parquet file if we have data
        if not time_metrics_df.empty:
            time_metrics_df.to_parquet(time_metrics_path)
            logging.info(f"Wrote time metrics file: {time_metrics_path}")
        
        # Write material completion data to a separate Parquet file if we have data
        if material_completion_df is not None and not material_completion_df.empty:
            material_completion_df.to_parquet(material_completion_path)
            logging.info(f"Wrote material completion file: {material_completion_path}")
        
        # 11. Cleanup old outputs (keep last 5)
        cleanup_old_files(config.OUT_DIR, "output_*.parquet", 2)
        cleanup_old_files(config.OUT_DIR, "time_metrics_*.parquet", 2)
        cleanup_old_files(config.OUT_DIR, "material_*.parquet", 2)
        
        # 12. Cleanup old exports
        cleanup_old_files(config.DELIVERY_INFO_DIR, "*_VL06O.xlsx", 5)
        cleanup_old_files(config.SERIAL_NUMBERS_DIR, "*_ZMDESNR.xlsx", 5)
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
    Main function to run the shipment tracking process.
    Handles command line arguments for run-once and no-wait options.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Shipment Tracking Tool')
    parser.add_argument('--run-once', action='store_true', help='Run the process once and exit')
    parser.add_argument('--no-wait', action='store_true', help='Disable the 10-second wait before visualization')
    args = parser.parse_args()

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
    
    # Run the process in a loop or once based on arguments
    while True:
        try:
            process_snapshot()
        except Exception as e:
            logging.exception("Unexpected error in processing cycle")
        
        # Wait before visualization if not disabled
        if not args.no_wait:
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
        
        # Exit if run-once is specified
        if args.run_once:
            logging.info("Run-once specified, exiting...")
            break
        
        # Calculate remaining sleep time
        remaining_sleep = config.INTERVAL_SECONDS - (0 if args.no_wait else 10)
        if remaining_sleep > 0:
            logging.info(f"Sleeping for remaining {remaining_sleep} seconds...")
            time.sleep(remaining_sleep)

if __name__ == "__main__":
    main()
