"""
Diff Utilities for Compare Snapshots

This module provides functions for data processing and metrics generation:
- Building DataFrames for analysis
- Generating metrics from snapshots
- Creating summary and hierarchical views
"""

import pandas as pd
import numpy as np
from datetime import datetime


def build_status_summary(df_latest):
    """
    Build status summary DataFrame
    
    Args:
        df_latest (pandas.DataFrame): Latest snapshot DataFrame
        
    Returns:
        pandas.DataFrame: Status summary DataFrame
    """
    # Group by status and count
    df_status_summary = df_latest.groupby('status').size().reset_index(name='count')
    
    return df_status_summary


def build_shipment_tree(df_latest):
    """
    Build hierarchical shipment tree DataFrame
    
    Args:
        df_latest (pandas.DataFrame): Latest snapshot DataFrame
        
    Returns:
        pandas.DataFrame: Shipment tree DataFrame (pivot table)
    """
    # Group by shipment and delivery, count serials
    shipment_counts = df_latest.groupby(['shipment', 'delivery']).size().unstack(fill_value=0)
    
    # Add totals
    shipment_counts['total'] = shipment_counts.sum(axis=1)
    
    return shipment_counts


def build_user_activity(df_window):
    """
    Build user activity DataFrame
    
    Args:
        df_window (pandas.DataFrame): Window DataFrame containing multiple snapshots
        
    Returns:
        pandas.DataFrame: User activity DataFrame with shipment progress
    """
    # Current time for calculating time since last scan
    now = datetime.now()
    
    # Get the latest snapshot data
    latest_time = df_window['snapshot_time'].max()
    if pd.isna(latest_time):
        latest_df = df_window  # Use all data if no valid time
    else:
        latest_df = df_window[df_window['snapshot_time'] == latest_time]
    
    # Get the second latest snapshot time for previous scan time
    time_sorted = sorted(df_window['snapshot_time'].dropna().unique())
    prev_time = time_sorted[-2] if len(time_sorted) >= 2 else None
    
    # Group by user and aggregate
    user_activity = (
        df_window.groupby('user')
        .agg(
            num_scans=('serial', 'count')
        )
    )
    
    # Add columns for timestamps
    user_activity['last_scan_ts'] = None
    user_activity['prev_scan_time'] = None
    user_activity['secs_since_last_scan'] = None
    
    # Add shipment information and progress
    user_activity['current_shipment'] = None
    user_activity['total_items'] = None
    user_activity['completed_items'] = None
    user_activity['ash_count'] = None
    user_activity['shp_count'] = None
    
    # Process each user to get their current shipment and progress
    for user in user_activity.index:
        # Get the user's latest data
        user_latest = latest_df[latest_df['user'] == user]
        
        if not user_latest.empty:
            # Get the shipment the user is working on (most common shipment)
            shipment_counts = user_latest['shipment'].value_counts()
            current_shipment = shipment_counts.index[0] if not shipment_counts.empty else None
            user_activity.at[user, 'current_shipment'] = current_shipment
            
            # Get the last scan timestamp from the 'time' column
            if 'time' in user_latest.columns:
                # Get the latest time for this user
                time_values = user_latest['time'].dropna()
                if not time_values.empty:
                    # Get the latest time value
                    last_scan = time_values.max()
                    if not pd.isna(last_scan):
                        user_activity.at[user, 'last_scan_ts'] = last_scan
                        # Calculate seconds since last scan
                        if isinstance(last_scan, datetime):
                            user_activity.at[user, 'secs_since_last_scan'] = (now - last_scan).total_seconds()
                        else:
                            print(f"Warning: last_scan is not a datetime for user {user}: {last_scan}")
            
            if current_shipment:
                # Get all items for this shipment
                shipment_items = df_window[df_window['shipment'] == current_shipment]
                total_items = len(shipment_items)
                user_activity.at[user, 'total_items'] = total_items
                
                # Count completed items (assuming SHP status means completed)
                completed_items = len(shipment_items[shipment_items['status'] == 'SHP'])
                user_activity.at[user, 'completed_items'] = completed_items
                
                # Count ASH vs SHP items
                ash_count = len(shipment_items[shipment_items['status'] == 'ASH'])
                shp_count = len(shipment_items[shipment_items['status'] == 'SHP'])
                user_activity.at[user, 'ash_count'] = ash_count
                user_activity.at[user, 'shp_count'] = shp_count
        
        # Get previous scan time for this user
        if prev_time is not None:
            prev_scans = df_window[(df_window['user'] == user) & (df_window['snapshot_time'] == prev_time)]
            if not prev_scans.empty and 'time' in prev_scans.columns:
                prev_time_values = prev_scans['time'].dropna()
                if not prev_time_values.empty:
                    prev_scan_time = prev_time_values.max()
                    if not pd.isna(prev_scan_time):
                        user_activity.at[user, 'prev_scan_time'] = prev_scan_time
    
    # Convert NaN values to None for JSON serialization
    for col in ['last_scan_ts', 'prev_scan_time', 'secs_since_last_scan', 'total_items', 'completed_items', 'ash_count', 'shp_count']:
        user_activity[col] = user_activity[col].apply(lambda x: None if pd.isna(x) else x)
    
    return user_activity


def build_status_by_delivery(df_latest):
    """
    Build status by delivery DataFrame
    
    Args:
        df_latest (pandas.DataFrame): Latest snapshot DataFrame
        
    Returns:
        pandas.DataFrame: Status by delivery DataFrame
    """
    # Group by delivery and status, count serials
    status_by_delivery = df_latest.groupby(['delivery', 'status']).size().unstack(fill_value=0)
    
    # Add totals
    status_by_delivery['total'] = status_by_delivery.sum(axis=1)
    
    return status_by_delivery


def build_metrics(df_latest, df_window):
    """
    Build all metrics DataFrames
    
    Args:
        df_latest (pandas.DataFrame): Latest snapshot DataFrame
        df_window (pandas.DataFrame): Window DataFrame containing multiple snapshots
        
    Returns:
        dict: Dictionary of metrics DataFrames
    """
    metrics = {
        'status_summary': build_status_summary(df_latest),
        'shipment_tree': build_shipment_tree(df_latest),
        'user_activity': build_user_activity(df_window),
        'status_by_delivery': build_status_by_delivery(df_latest)
    }
    
    return metrics
