"""
Debug Script for Shipment Tracker

This script runs a single processing cycle of the shipment tracker
with additional debugging information to help identify issues.
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
import config
from shipment_tracker import (
    sanitize_delivery_number, 
    ensure_timestamp_column,
    filter_by_warehouse,
    filter_by_parent_serial,
    filter_by_time_window,
    deduplicate_serial_status,
    process_ash_shp_statuses,
    apply_shp_ceiling_timestamp,
    extract_timestamp_from_filename
)

def get_latest_file(directory, pattern="*.xlsx"):
    """
    Get the latest file in a directory matching the given pattern.
    Skips temporary Excel files (those starting with ~$).
    
    Args:
        directory (str): Directory path to search
        pattern (str): Glob pattern to match files
        
    Returns:
        Path: Path object of the latest file, or None if no files found
    """
    files = list(Path(directory).glob(pattern))
    # Filter out temporary Excel files (those starting with ~$)
    files = [f for f in files if not f.name.startswith('~$')]
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)

def debug_process_snapshot():
    """
    Process one cycle of snapshot data with additional debugging information.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,  # Set to DEBUG for more detailed logging
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Log to console
            logging.FileHandler('debug_shipment_tracker.log')  # Log to file
        ]
    )
    
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
        
        # DEBUG: Print initial dataframe shapes
        logging.debug(f"Initial df_serial shape: {df_serial.shape}")
        logging.debug(f"Initial df_delivery shape: {df_delivery.shape}")
        
        # DEBUG: Print sample of each dataframe
        logging.debug("Sample of serial data:")
        logging.debug(df_serial.head(5).to_string())
        
        logging.debug("Sample of delivery data:")
        logging.debug(df_delivery.head(5).to_string())
        
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
    
    # DEBUG: Print dataframe shape after processing
    logging.debug(f"df_serial shape after processing: {df_serial.shape}")
    logging.debug("Sample of processed serial data:")
    logging.debug(df_serial.head(5).to_string())
    
    # DEBUG: Count unique values
    logging.debug(f"Unique users: {df_serial['Created by'].nunique()}")
    logging.debug(f"Unique deliveries: {df_serial['Delivery'].nunique()}")
    logging.debug(f"Unique serials: {df_serial['Serial #'].nunique()}")
    logging.debug(f"Status counts: {df_serial['Status'].value_counts().to_dict()}")

    # 4. Create a base aggregation by user and delivery
    agg = df_serial.groupby(['Created by', 'Delivery']).size().reset_index(name='total_records')
    
    # DEBUG: Print aggregation shape
    logging.debug(f"Initial aggregation shape: {agg.shape}")
    logging.debug("Sample of initial aggregation:")
    logging.debug(agg.head(5).to_string())
    
    # Get status breakdown (shipped vs picked counts)
    # This gives us counts by status for each user-delivery combination
    status_counts = df_serial.groupby(['Created by', 'Delivery', 'StatusText']).size().unstack(fill_value=0)
    
    # DEBUG: Print status counts shape
    logging.debug(f"Status counts shape: {status_counts.shape}")
    logging.debug("Sample of status counts:")
    logging.debug(status_counts.head(5).to_string())
    
    # Ensure both status columns exist, even if no data
    for status in config.STATUS_MAPPING.values():
        if status not in status_counts.columns:
            status_counts[status] = 0
    
    # Rename columns for clarity
    status_counts.columns = [f"{col.replace(' / ', '_')}_count" for col in status_counts.columns]
    status_counts = status_counts.reset_index()
    
    # DEBUG: Print status counts after renaming
    logging.debug("Status counts after renaming:")
    logging.debug(status_counts.head(5).to_string())
    
    # Merge status counts with the aggregated data
    agg = agg.merge(status_counts, on=['Created by', 'Delivery'], how='left')
    agg = agg.fillna(0)  # Fill NaN values with 0 for status counts
    
    # DEBUG: Print aggregation after merging status counts
    logging.debug(f"Aggregation shape after merging status counts: {agg.shape}")
    logging.debug("Sample of aggregation after merging status counts:")
    logging.debug(agg.head(5).to_string())
    
    # Drop the total_records column as it's not needed
    agg = agg.drop('total_records', axis=1)
    
    # 5. Add total delivery packages from VL06O
    # Sanitize delivery numbers in both dataframes to ensure consistent format
    agg['delivery_clean'] = agg['Delivery'].apply(sanitize_delivery_number)
    df_delivery['delivery_clean'] = df_delivery['Delivery'].apply(sanitize_delivery_number)
    
    # DEBUG: Check for duplicate delivery_clean values in df_delivery
    delivery_clean_counts = df_delivery['delivery_clean'].value_counts()
    duplicate_deliveries = delivery_clean_counts[delivery_clean_counts > 1].index.tolist()
    
    if duplicate_deliveries:
        logging.warning(f"Found {len(duplicate_deliveries)} duplicate delivery_clean values in df_delivery: {duplicate_deliveries}")
        logging.warning("This could cause the merge to create duplicate rows!")
        
        # DEBUG: Print the duplicate rows
        for delivery in duplicate_deliveries:
            logging.warning(f"Duplicate rows for delivery_clean={delivery}:")
            logging.warning(df_delivery[df_delivery['delivery_clean'] == delivery].to_string())
            
        # For each delivery, keep only one row with the total package count
        # Group by delivery_clean and take the first row for each group
        df_delivery_deduped = df_delivery.groupby('delivery_clean').first().reset_index()
        df_delivery = df_delivery_deduped
        
        logging.warning(f"Deduplicated delivery data. Shape before: {len(delivery_clean_counts)}, after: {df_delivery.shape[0]}")
    
    # DEBUG: Print delivery data before merge
    logging.debug("Delivery data before merge:")
    logging.debug(df_delivery[['delivery_clean', 'Number of packages']].head(10).to_string())
    
    # DEBUG: Print aggregation before merge
    logging.debug(f"Aggregation shape before delivery merge: {agg.shape}")
    logging.debug("Sample of aggregation before delivery merge:")
    logging.debug(agg.head(5).to_string())
    
    # Merge delivery totals using the sanitized delivery numbers
    agg_before_merge = agg.copy()
    agg = agg.merge(df_delivery[['delivery_clean', 'Number of packages']], 
                   on='delivery_clean', how='left')
    agg = agg.rename(columns={'Number of packages': 'delivery_total_packages'})
    
    # DEBUG: Check if the merge created duplicate rows
    if len(agg) > len(agg_before_merge):
        logging.warning(f"Merge created duplicate rows! Before: {len(agg_before_merge)}, After: {len(agg)}")
        
        # DEBUG: Find which rows were duplicated
        agg_with_counts = agg.groupby(['Created by', 'Delivery']).size().reset_index(name='count')
        duplicated_rows = agg_with_counts[agg_with_counts['count'] > 1]
        
        if not duplicated_rows.empty:
            logging.warning(f"Found {len(duplicated_rows)} duplicated user-delivery combinations:")
            logging.warning(duplicated_rows.to_string())
            
            # DEBUG: Print the duplicated rows in detail
            for _, row in duplicated_rows.iterrows():
                user = row['Created by']
                delivery = row['Delivery']
                logging.warning(f"Duplicated rows for user={user}, delivery={delivery}:")
                logging.warning(agg[(agg['Created by'] == user) & (agg['Delivery'] == delivery)].to_string())
    
    # DEBUG: Print aggregation after merge
    logging.debug(f"Aggregation shape after delivery merge: {agg.shape}")
    logging.debug("Sample of aggregation after delivery merge:")
    logging.debug(agg.head(5).to_string())
    
    # Drop the temporary column used for merging
    agg = agg.drop('delivery_clean', axis=1)
    
    # 6. Calculate progress metrics
    # For progress calculation, consider ASH (assigned to shipper) as "picked"
    # If all items are shipped, progress should be 100%
    agg['scanned_packages'] = agg['assigned to shipper_count']
    logging.info("Using 'assigned to shipper_count' as the picked count")
    
    # DEBUG: Print scanned packages
    logging.debug("Scanned packages:")
    logging.debug(agg[['Created by', 'Delivery', 'scanned_packages', 'delivery_total_packages']].head(10).to_string())
    
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
    
    # DEBUG: Check for unusually high progress percentages
    high_progress = agg[agg['progress_percentage'] > 100]
    if not high_progress.empty:
        logging.warning(f"Found {len(high_progress)} rows with progress > 100%:")
        logging.warning(high_progress.to_string())
    
    # If all items are shipped, set progress to 100%
    all_shipped_mask = (agg['shipped_count'] > 0) & (agg['assigned to shipper_count'] == 0)
    agg.loc[all_shipped_mask, 'progress_percentage'] = 100.0
    
    # Round percentage to 2 decimal places
    agg['progress_percentage'] = agg['progress_percentage'].round(2)
    
    # DEBUG: Print final aggregation
    logging.debug("Final aggregation:")
    logging.debug(agg.head(10).to_string())
    
    # DEBUG: Print summary statistics
    logging.debug("Summary statistics for progress_percentage:")
    logging.debug(agg['progress_percentage'].describe().to_string())
    
    logging.debug("Summary statistics for scanned_packages:")
    logging.debug(agg['scanned_packages'].describe().to_string())
    
    logging.debug("Summary statistics for delivery_total_packages:")
    logging.debug(agg['delivery_total_packages'].describe().to_string())
    
    # DEBUG: Check for cases where scanned_packages > delivery_total_packages
    problem_cases = agg[agg['scanned_packages'] > agg['delivery_total_packages']]
    if not problem_cases.empty:
        logging.warning(f"Found {len(problem_cases)} cases where scanned_packages > delivery_total_packages:")
        logging.warning(problem_cases.to_string())
    
    # Add the is_ash_user flag to the aggregated data
    agg['is_ash_user'] = agg['Created by'].isin(ash_users)
    
    # 8. Rename columns for final output
    agg = agg.rename(columns={'Created by': 'user', 'Delivery': 'delivery'})
    
    # Return the aggregated data for further analysis
    return agg

if __name__ == "__main__":
    print("Running debug analysis of shipment tracker...")
    result = debug_process_snapshot()
    
    if result is not None:
        print("\nAnalysis complete. Check debug_shipment_tracker.log for detailed logs.")
        
        # Print summary of potential issues
        problem_cases = result[result['scanned_packages'] > result['delivery_total_packages']]
        if not problem_cases.empty:
            print(f"\nWARNING: Found {len(problem_cases)} cases where scanned_packages > delivery_total_packages")
            print("Sample of problem cases:")
            print(problem_cases.head(5).to_string())
        
        high_progress = result[result['progress_percentage'] > 100]
        if not high_progress.empty:
            print(f"\nWARNING: Found {len(high_progress)} rows with progress > 100%")
            print("Sample of high progress cases:")
            print(high_progress.head(5).to_string())
    else:
        print("Analysis failed. Check debug_shipment_tracker.log for error details.")
