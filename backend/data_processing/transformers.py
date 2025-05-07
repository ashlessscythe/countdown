"""
Data transformation functions to extract required metrics.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
import logging

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config import WAREHOUSE_FILTER, WINDOW_MINUTES, STATUS_MAPPING
from backend.storage.cache import dashboard_cache

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def calculate_progress_metrics(combined_df):
    """
    Calculate progress metrics for each delivery.
    
    Args:
        combined_df (pandas.DataFrame): Combined data from ZMDESNR and VL06O
        
    Returns:
        pandas.DataFrame: DataFrame with progress metrics
    """
    if combined_df.empty:
        return pd.DataFrame()
    
    # Group by delivery and created_by to get counts per user per delivery
    user_delivery_counts = combined_df.groupby(['delivery', 'created_by']).size().reset_index(name='scanned_count')
    
    # Get the total package count for each delivery
    delivery_totals = combined_df[['delivery', 'number_of_packages']].drop_duplicates()
    
    # Merge the counts with the totals
    progress_df = pd.merge(
        user_delivery_counts,
        delivery_totals,
        on='delivery',
        how='left'
    )
    
    # Calculate progress percentage
    progress_df['progress_percentage'] = (progress_df['scanned_count'] / progress_df['number_of_packages'] * 100).round(2)
    
    return progress_df

def get_scan_time_metrics(combined_df):
    """
    Extract scan time metrics for each user.
    
    Args:
        combined_df (pandas.DataFrame): Combined data from ZMDESNR and VL06O
        
    Returns:
        pandas.DataFrame: DataFrame with scan time metrics
    """
    if combined_df.empty:
        logger.warning("Empty dataframe, skipping scan time metrics")
        return pd.DataFrame()
    
    # Check if we have the necessary columns
    required_columns = ['created_by', 'time', 'created_on']
    missing_columns = [col for col in required_columns if col not in combined_df.columns]
    
    if missing_columns:
        logger.warning(f"Missing required columns for scan time metrics: {missing_columns}")
        # Try to create scan_timestamp from time and created_on if they exist
        if 'time' in combined_df.columns and 'created_on' in combined_df.columns:
            try:
                # Convert created_on to datetime if it's not already
                if not pd.api.types.is_datetime64_any_dtype(combined_df['created_on']):
                    combined_df['created_on'] = pd.to_datetime(combined_df['created_on'], errors='coerce')
                
                # Create scan_timestamp by combining date from created_on and time
                combined_df['scan_timestamp'] = pd.to_datetime(
                    combined_df['created_on'].dt.strftime('%Y-%m-%d') + ' ' + combined_df['time'], 
                    errors='coerce'
                )
                logger.info("Created scan_timestamp from time and created_on columns")
            except Exception as e:
                logger.error(f"Error creating scan_timestamp: {str(e)}")
                return pd.DataFrame()
        else:
            return pd.DataFrame()
    
    # Use scan_timestamp if it exists, otherwise try to create it
    if 'scan_timestamp' not in combined_df.columns:
        try:
            # Convert created_on to datetime if it's not already
            if not pd.api.types.is_datetime64_any_dtype(combined_df['created_on']):
                combined_df['created_on'] = pd.to_datetime(combined_df['created_on'], errors='coerce')
            
            # Check if time_str exists (created in readers.py)
            if 'time_str' in combined_df.columns:
                # Create scan_timestamp by combining date from created_on and time_str
                combined_df['scan_timestamp'] = pd.to_datetime(
                    combined_df['created_on'].dt.strftime('%Y-%m-%d') + ' ' + combined_df['time_str'], 
                    errors='coerce'
                )
                logger.info("Created scan_timestamp from time_str and created_on columns")
            elif 'time' in combined_df.columns:
                # Convert time to string if it's not already
                if not pd.api.types.is_object_dtype(combined_df['time']):
                    # If time is a datetime.time object, convert to string
                    combined_df['time_str'] = combined_df['time'].apply(lambda x: x.strftime('%H:%M:%S') if hasattr(x, 'strftime') else str(x))
                else:
                    combined_df['time_str'] = combined_df['time']
                
                # Create scan_timestamp by combining date from created_on and time_str
                combined_df['scan_timestamp'] = pd.to_datetime(
                    combined_df['created_on'].dt.strftime('%Y-%m-%d') + ' ' + combined_df['time_str'], 
                    errors='coerce'
                )
                logger.info("Created scan_timestamp from time and created_on columns")
            else:
                # Use created_on as fallback
                combined_df['scan_timestamp'] = combined_df['created_on']
                logger.info("Using created_on as scan_timestamp (no time column available)")
        except Exception as e:
            logger.error(f"Error creating scan_timestamp: {str(e)}")
            # Use current time as a fallback
            combined_df['scan_timestamp'] = datetime.now()
            logger.warning("Using current time for all scan_timestamp values due to error")
    
    # Ensure scan_timestamp is datetime
    combined_df['scan_timestamp'] = pd.to_datetime(combined_df['scan_timestamp'], errors='coerce')
    
    # Drop rows with missing scan_timestamp or created_by
    valid_df = combined_df.dropna(subset=['scan_timestamp', 'created_by'])
    
    if valid_df.empty:
        logger.warning("No valid scan data after filtering")
        return pd.DataFrame()
    
    # Sort by user and timestamp
    sorted_df = valid_df.sort_values(['created_by', 'scan_timestamp'])
    
    # Group by user
    user_groups = sorted_df.groupby('created_by')
    
    # Initialize lists to store results
    users = []
    current_scans = []
    previous_scans = []
    time_differences = []
    serials = []
    statuses = []
    
    # Current time for reference
    now = datetime.now()
    
    for user, group in user_groups:
        # Get the latest scan for this user
        latest_scan = group['scan_timestamp'].max()
        latest_row = group[group['scan_timestamp'] == latest_scan].iloc[0]
        
        # Get serial number and status if available
        serial = str(latest_row.get('serial_number', '')) if 'serial_number' in latest_row else str(latest_row.get('Serial #', ''))
        status = str(latest_row.get('status', '')) if 'status' in latest_row else ''
        
        # If we have at least two scans, get the previous one
        if len(group) > 1:
            # Get all timestamps except the latest
            previous_timestamps = group[group['scan_timestamp'] < latest_scan]['scan_timestamp']
            if not previous_timestamps.empty:
                previous_scan = previous_timestamps.max()
                previous_row = group[group['scan_timestamp'] == previous_scan].iloc[0]
                time_diff = (latest_scan - previous_scan).total_seconds() / 60  # in minutes
            else:
                previous_scan = pd.NaT
                time_diff = np.nan
        else:
            previous_scan = pd.NaT
            time_diff = np.nan
        
        users.append(user)
        current_scans.append(latest_scan)
        previous_scans.append(previous_scan)
        time_differences.append(time_diff)
        serials.append(serial)
        statuses.append(status)
    
    # Create DataFrame with results
    scan_metrics_df = pd.DataFrame({
        'user_id': users,
        'current_scan_time': current_scans,
        'previous_scan_time': previous_scans,
        'time_between_scans_minutes': time_differences,
        'serial': serials,
        'status': statuses
    })
    
    # Calculate time since last scan
    scan_metrics_df['minutes_since_last_scan'] = (
        (now - scan_metrics_df['current_scan_time']).dt.total_seconds() / 60
    ).round(2)
    
    return scan_metrics_df

def get_user_activity_metrics(combined_df, window_minutes=WINDOW_MINUTES):
    """
    Calculate user activity metrics within a time window.
    
    Args:
        combined_df (pandas.DataFrame): Combined data from ZMDESNR and VL06O
        window_minutes (int): Time window in minutes
        
    Returns:
        pandas.DataFrame: DataFrame with user activity metrics
    """
    if combined_df.empty:
        logger.warning("Empty dataframe, skipping user activity metrics")
        return pd.DataFrame()
    
    # Check if we have the necessary columns
    if 'created_by' not in combined_df.columns:
        logger.warning("created_by column not found, skipping user activity metrics")
        return pd.DataFrame()
    
    # Use scan_timestamp if it exists, otherwise try to create it
    if 'scan_timestamp' not in combined_df.columns:
        try:
            # Check if we have time_str and created_on columns
            if 'time_str' in combined_df.columns and 'created_on' in combined_df.columns:
                # Convert created_on to datetime if it's not already
                if not pd.api.types.is_datetime64_any_dtype(combined_df['created_on']):
                    combined_df['created_on'] = pd.to_datetime(combined_df['created_on'], errors='coerce')
                
                # Create scan_timestamp by combining date from created_on and time_str
                combined_df['scan_timestamp'] = pd.to_datetime(
                    combined_df['created_on'].dt.strftime('%Y-%m-%d') + ' ' + combined_df['time_str'], 
                    errors='coerce'
                )
                logger.info("Created scan_timestamp from time_str and created_on columns")
            elif 'time' in combined_df.columns and 'created_on' in combined_df.columns:
                # Convert time to string if it's not already
                if not pd.api.types.is_object_dtype(combined_df['time']):
                    # If time is a datetime.time object, convert to string
                    combined_df['time_str'] = combined_df['time'].apply(lambda x: x.strftime('%H:%M:%S') if hasattr(x, 'strftime') else str(x))
                else:
                    combined_df['time_str'] = combined_df['time']
                
                # Create scan_timestamp by combining date from created_on and time_str
                combined_df['scan_timestamp'] = pd.to_datetime(
                    combined_df['created_on'].dt.strftime('%Y-%m-%d') + ' ' + combined_df['time_str'], 
                    errors='coerce'
                )
                logger.info("Created scan_timestamp from time and created_on columns")
            else:
                # If we don't have time and created_on, use created_on as scan_timestamp
                if 'created_on' in combined_df.columns:
                    combined_df['scan_timestamp'] = pd.to_datetime(combined_df['created_on'], errors='coerce')
                    logger.info("Using created_on as scan_timestamp")
                else:
                    logger.warning("Cannot create scan_timestamp, no suitable columns found")
                    # Use current time for all rows as a fallback
                    combined_df['scan_timestamp'] = datetime.now()
                    logger.warning("Using current time for all scan_timestamp values")
        except Exception as e:
            logger.error(f"Error creating scan_timestamp: {str(e)}")
            # Use current time for all rows as a fallback
            combined_df['scan_timestamp'] = datetime.now()
            logger.warning("Using current time for all scan_timestamp values due to error")
    
    # Ensure scan_timestamp is datetime
    combined_df['scan_timestamp'] = pd.to_datetime(combined_df['scan_timestamp'], errors='coerce')
    
    # Drop rows with missing scan_timestamp or created_by
    valid_df = combined_df.dropna(subset=['scan_timestamp', 'created_by'])
    
    if valid_df.empty:
        logger.warning("No valid user data after filtering")
        return pd.DataFrame()
    
    # Calculate the cutoff time for the window
    now = datetime.now()
    cutoff_time = now - timedelta(minutes=window_minutes)
    
    # Filter for scans within the window
    recent_scans = valid_df[valid_df['scan_timestamp'] >= cutoff_time]
    
    if recent_scans.empty:
        logger.warning("No recent scans within the time window")
        # Return all users with minimal data instead of empty DataFrame
        all_users = valid_df['created_by'].unique()
        user_df = pd.DataFrame({
            'created_by': all_users,
            'scan_count': 0,
            'first_scan': pd.NaT,
            'last_scan': pd.NaT,
            'unique_deliveries': 0,
            'scans_per_hour': 0.0,
            'minutes_since_last_scan': np.nan
        })
        return user_df
    
    # Group by user and calculate metrics
    user_metrics = recent_scans.groupby('created_by').agg({
        'scan_timestamp': ['count', 'min', 'max'],
        'delivery': 'nunique'
    })
    
    # Flatten the column hierarchy
    user_metrics.columns = ['scan_count', 'first_scan', 'last_scan', 'unique_deliveries']
    user_metrics = user_metrics.reset_index()
    
    # Calculate scans per hour
    user_metrics['scans_per_hour'] = (
        user_metrics['scan_count'] / 
        ((user_metrics['last_scan'] - user_metrics['first_scan']).dt.total_seconds() / 3600)
    ).round(2)
    
    # Replace infinity values (when time difference is very small)
    user_metrics['scans_per_hour'] = user_metrics['scans_per_hour'].replace([np.inf, -np.inf], np.nan)
    
    # Calculate time since last scan
    user_metrics['minutes_since_last_scan'] = (
        (now - user_metrics['last_scan']).dt.total_seconds() / 60
    ).round(2)
    
    # Add name column if available
    if 'name' in combined_df.columns:
        # Create a mapping of user IDs to names
        name_map = {}
        for user_id in user_metrics['created_by']:
            user_rows = combined_df[combined_df['created_by'] == user_id]
            if not user_rows.empty and not pd.isna(user_rows['name'].iloc[0]):
                name_map[user_id] = user_rows['name'].iloc[0]
            else:
                name_map[user_id] = f"User {user_id}"
        
        # Add the name column
        user_metrics['name'] = user_metrics['created_by'].map(name_map)
    
    return user_metrics

def track_serial_status_changes(combined_df):
    """
    Track changes in serial status (ASH to SHP) and identify new serials.
    
    Args:
        combined_df (pandas.DataFrame): Combined data from ZMDESNR and VL06O
        
    Returns:
        tuple: (status_changes_df, new_serials_df, completed_deliveries_df)
    """
    if combined_df.empty or 'status' not in combined_df.columns or 'serial_number' not in combined_df.columns:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Get previous dashboard data from cache
    previous_data = dashboard_cache.get('previous_dashboard_data', {})
    previous_serials = previous_data.get('serials', [])
    
    # Log the status distribution in the current data
    if not combined_df.empty and 'status' in combined_df.columns:
        status_counts = combined_df['status'].value_counts()
        logger.info(f"Current status distribution: {status_counts.to_dict()}")
    
    # If no previous data, all serials are new
    if not previous_serials:
        # Mark all ASH status serials as newly picked
        new_serials_df = combined_df[combined_df['status'] == 'ASH'].copy()
        if not new_serials_df.empty:
            new_serials_df['event'] = 'newly_picked'
            logger.info(f"Found {len(new_serials_df)} new serials (first run)")
        else:
            new_serials_df = pd.DataFrame()
        
        # No status changes or completed deliveries on first run
        return pd.DataFrame(), new_serials_df, pd.DataFrame()
    
    # Convert previous serials list to DataFrame
    previous_df = pd.DataFrame(previous_serials)
    
    # Ensure serial_number is of the same type in both dataframes
    combined_df['serial_number'] = combined_df['serial_number'].astype(str)
    if 'serial_number' in previous_df.columns:
        previous_df['serial_number'] = previous_df['serial_number'].astype(str)
    
    # Find new serials (in current but not in previous)
    if 'serial_number' in previous_df.columns:
        new_serials = set(combined_df['serial_number']) - set(previous_df['serial_number'])
        new_serials_df = combined_df[combined_df['serial_number'].isin(new_serials)].copy()
        
        # Filter for only ASH status (newly picked)
        new_serials_df = new_serials_df[new_serials_df['status'] == 'ASH'].copy()
        if not new_serials_df.empty:
            new_serials_df['event'] = 'newly_picked'
            logger.info(f"Found {len(new_serials_df)} newly picked serials")
    else:
        new_serials_df = pd.DataFrame()
    
    # Find status changes (serials in both current and previous with different status)
    status_changes_df = pd.DataFrame()
    if 'serial_number' in previous_df.columns and 'status' in previous_df.columns:
        # Create a mapping of previous serial numbers to their status
        prev_status_map = dict(zip(previous_df['serial_number'], previous_df['status']))
        
        # Filter for serials that exist in both datasets
        common_serials = set(combined_df['serial_number']) & set(previous_df['serial_number'])
        common_serials_df = combined_df[combined_df['serial_number'].isin(common_serials)].copy()
        
        # Add previous status column
        common_serials_df['previous_status'] = common_serials_df['serial_number'].map(prev_status_map)
        
        # Filter for status changes from ASH to SHP
        status_changes_df = common_serials_df[
            (common_serials_df['previous_status'] == 'ASH') & 
            (common_serials_df['status'] == 'SHP')
        ].copy()
        
        if not status_changes_df.empty:
            status_changes_df['event'] = 'shipped'
            logger.info(f"Found {len(status_changes_df)} serials that changed from ASH to SHP")
    
    # Find completed deliveries (all serials for a delivery are SHP)
    completed_deliveries_df = pd.DataFrame()
    if not combined_df.empty:
        # Group by delivery and count total serials and shipped serials
        delivery_status = combined_df.groupby('delivery').agg({
            'serial_number': 'count',
            'status': lambda x: (x == 'SHP').sum()
        }).reset_index()
        
        # Rename columns for clarity
        delivery_status.columns = ['delivery', 'total_serials', 'shipped_serials']
        
        # Filter for deliveries where all serials are shipped
        completed_deliveries_df = delivery_status[
            delivery_status['total_serials'] == delivery_status['shipped_serials']
        ].copy()
        
        if not completed_deliveries_df.empty:
            completed_deliveries_df['event'] = 'completed'
            logger.info(f"Found {len(completed_deliveries_df)} completed deliveries")
    
    return status_changes_df, new_serials_df, completed_deliveries_df

def preprocess_serial_data(combined_df):
    """
    Preprocess serial data to handle cumulative snapshots.
    
    For each serial number, if both ASH and SHP statuses exist, keep only the SHP status.
    This is more efficient than processing each duplicate serial one by one.
    
    Args:
        combined_df (pandas.DataFrame): Combined data from ZMDESNR and VL06O
        
    Returns:
        pandas.DataFrame: Preprocessed DataFrame with duplicate serials removed
    """
    if combined_df.empty or 'serial_number' not in combined_df.columns or 'status' not in combined_df.columns:
        return combined_df
    
    # Make a copy to avoid modifying the original
    df = combined_df.copy()
    
    # Get count of rows before preprocessing
    initial_count = len(df)
    
    # Create a flag for SHP status
    df['is_shp'] = df['status'] == 'SHP'
    
    # Sort by serial_number and is_shp (True values first)
    # This ensures SHP records come before ASH records for the same serial
    df = df.sort_values(['serial_number', 'is_shp'], ascending=[True, False])
    
    # Keep only the first occurrence of each serial_number (which will be SHP if it exists)
    df = df.drop_duplicates('serial_number', keep='first')
    
    # Remove the temporary flag
    df = df.drop('is_shp', axis=1)
    
    # Get count of rows after preprocessing
    final_count = len(df)
    removed_count = initial_count - final_count
    
    if removed_count > 0:
        logger.info(f"Removed {removed_count} duplicate serial entries, keeping SHP status where available")
    
    return df

def prepare_dashboard_data(combined_df):
    """
    Prepare data for the dashboard display.
    
    Args:
        combined_df (pandas.DataFrame): Combined data from ZMDESNR and VL06O
        
    Returns:
        dict: Dictionary with dashboard data
    """
    if combined_df.empty:
        return {
            'users': [],
            'deliveries': [],
            'progress': [],
            'scan_times': [],
            'serials': [],
            'status_changes': [],
            'new_serials': [],
            'completed_deliveries': []
        }
    
    # Make a copy to avoid modifying the original
    df = combined_df.copy()
    
    # Standardize column names from Excel files
    # Map common Excel column names to our expected column names
    column_mapping = {
        'Serial #': 'serial_number',
        'Created by': 'created_by',
        'Created on': 'created_on',
        'Delivery': 'delivery',
        'Status': 'status',
        'Number of packages': 'number_of_packages'
    }
    
    # Apply the mapping for columns that exist
    for excel_col, std_col in column_mapping.items():
        if excel_col in df.columns and std_col not in df.columns:
            df[std_col] = df[excel_col]
    
    # Preprocess serial data to handle cumulative snapshots
    df = preprocess_serial_data(df)
    
    # Map status codes to their descriptions
    if 'status' in df.columns:
        df['status_description'] = df['status'].map(STATUS_MAPPING)
    
    # Calculate progress metrics
    progress_df = calculate_progress_metrics(df)
    
    # Get scan time metrics
    scan_metrics_df = get_scan_time_metrics(df)
    
    # Get user activity metrics
    user_metrics_df = get_user_activity_metrics(df)
    
    # Track serial status changes
    status_changes_df, new_serials_df, completed_deliveries_df = track_serial_status_changes(df)
    
    # Prepare serials data - ensure we only select columns that exist
    required_columns = ['serial_number', 'delivery', 'status', 'created_by']
    optional_columns = ['scan_timestamp', 'time', 'created_on']
    
    # Add optional columns if they exist
    columns_to_select = [col for col in required_columns if col in df.columns] + \
                        [col for col in optional_columns if col in df.columns]
    
    if columns_to_select:
        serials_df = df[columns_to_select].copy()
        
        # Add status description if available
        if 'status_description' in df.columns:
            serials_df['status_description'] = df['status_description']
    else:
        serials_df = pd.DataFrame()
    
    # Ensure user_metrics_df has user_id column (renamed from created_by)
    if not user_metrics_df.empty and 'created_by' in user_metrics_df.columns:
        user_metrics_df = user_metrics_df.rename(columns={'created_by': 'user_id'})
    
    # Combine user metrics with progress and scan times
    dashboard_data = {
        'users': user_metrics_df.to_dict(orient='records') if not user_metrics_df.empty else [],
        'deliveries': df[['delivery', 'number_of_packages']].drop_duplicates().to_dict(orient='records') if 'delivery' in df.columns and 'number_of_packages' in df.columns else [],
        'progress': progress_df.to_dict(orient='records') if not progress_df.empty else [],
        'scan_times': scan_metrics_df.to_dict(orient='records') if not scan_metrics_df.empty else [],
        'serials': serials_df.to_dict(orient='records') if not serials_df.empty else [],
        'status_changes': status_changes_df.to_dict(orient='records') if not status_changes_df.empty else [],
        'new_serials': new_serials_df.to_dict(orient='records') if not new_serials_df.empty else [],
        'completed_deliveries': completed_deliveries_df.to_dict(orient='records') if not completed_deliveries_df.empty else []
    }
    
    # Log the counts for each section
    for section, data in dashboard_data.items():
        logger.info(f"{section}: {len(data)} records")
    
    return dashboard_data
