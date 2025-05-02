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
from config import WAREHOUSE_FILTER, WINDOW_MINUTES

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
    if combined_df.empty or 'scan_timestamp' not in combined_df.columns:
        return pd.DataFrame()
    
    # Ensure scan_timestamp is datetime
    combined_df['scan_timestamp'] = pd.to_datetime(combined_df['scan_timestamp'], errors='coerce')
    
    # Sort by user and timestamp
    sorted_df = combined_df.sort_values(['created_by', 'scan_timestamp'])
    
    # Group by user
    user_groups = sorted_df.groupby('created_by')
    
    # Initialize lists to store results
    users = []
    current_scans = []
    previous_scans = []
    time_differences = []
    
    # Current time for reference
    now = datetime.now()
    
    for user, group in user_groups:
        # Get the latest scan for this user
        latest_scan = group['scan_timestamp'].max()
        
        # If we have at least two scans, get the previous one
        if len(group) > 1:
            # Get all timestamps except the latest
            previous_timestamps = group[group['scan_timestamp'] < latest_scan]['scan_timestamp']
            if not previous_timestamps.empty:
                previous_scan = previous_timestamps.max()
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
    
    # Create DataFrame with results
    scan_metrics_df = pd.DataFrame({
        'user_id': users,
        'current_scan_time': current_scans,
        'previous_scan_time': previous_scans,
        'time_between_scans_minutes': time_differences
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
    if combined_df.empty or 'scan_timestamp' not in combined_df.columns:
        return pd.DataFrame()
    
    # Ensure scan_timestamp is datetime
    combined_df['scan_timestamp'] = pd.to_datetime(combined_df['scan_timestamp'], errors='coerce')
    
    # Calculate the cutoff time for the window
    now = datetime.now()
    cutoff_time = now - timedelta(minutes=window_minutes)
    
    # Filter for scans within the window
    recent_scans = combined_df[combined_df['scan_timestamp'] >= cutoff_time]
    
    if recent_scans.empty:
        return pd.DataFrame()
    
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
    
    return user_metrics

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
            'scan_times': []
        }
    
    # Calculate progress metrics
    progress_df = calculate_progress_metrics(combined_df)
    
    # Get scan time metrics
    scan_metrics_df = get_scan_time_metrics(combined_df)
    
    # Get user activity metrics
    user_metrics_df = get_user_activity_metrics(combined_df)
    
    # Combine user metrics with progress and scan times
    dashboard_data = {
        'users': user_metrics_df.to_dict(orient='records') if not user_metrics_df.empty else [],
        'deliveries': combined_df[['delivery', 'number_of_packages']].drop_duplicates().to_dict(orient='records'),
        'progress': progress_df.to_dict(orient='records') if not progress_df.empty else [],
        'scan_times': scan_metrics_df.to_dict(orient='records') if not scan_metrics_df.empty else []
    }
    
    return dashboard_data
