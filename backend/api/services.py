"""
Business logic for API endpoints.
"""
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
import json

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from backend.storage.cache import dashboard_cache
from backend.data_processing.transformers import (
    calculate_progress_metrics,
    get_scan_time_metrics,
    get_user_activity_metrics,
    track_serial_status_changes
)
from config import WINDOW_MINUTES

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# User activity tracking
user_activity_log = {}

def get_dashboard_data() -> Dict[str, Any]:
    """
    Get all dashboard data from cache.
    
    Returns:
        dict: Complete dashboard data
    """
    # Get data from cache
    data = dashboard_cache.get_dashboard_data()
    
    if not data:
        logger.warning("No dashboard data found in cache")
        return {
            "users": [],
            "deliveries": [],
            "progress": [],
            "scan_times": [],
            "serials": [],
            "status_changes": [],
            "new_serials": [],
            "completed_deliveries": []
        }
    
    # Add timestamp
    data["timestamp"] = datetime.now().isoformat()
    
    return data

def get_user_activity(active_only: bool = False) -> List[Dict[str, Any]]:
    """
    Get user activity data.
    
    Args:
        active_only (bool): If True, return only active users
        
    Returns:
        list: User activity data
    """
    # Get user data from cache
    users_data = dashboard_cache.get_section_data('users')
    
    if not users_data:
        logger.warning("No user data found in cache")
        return []
    
    # Filter for active users if requested
    if active_only:
        # Consider a user active if their last scan was within the window
        cutoff_time = datetime.now() - timedelta(minutes=WINDOW_MINUTES)
        
        # Convert string timestamps to datetime for comparison
        active_users = []
        for user in users_data:
            if 'last_scan' in user:
                try:
                    last_scan = datetime.fromisoformat(user['last_scan'])
                    if last_scan >= cutoff_time:
                        active_users.append(user)
                except (ValueError, TypeError):
                    # If we can't parse the timestamp, skip this user
                    pass
        
        return active_users
    
    return users_data

def get_delivery_progress(delivery_id: Optional[str] = None, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get delivery progress data.
    
    Args:
        delivery_id (str, optional): Filter by delivery ID
        user_id (str, optional): Filter by user ID
        
    Returns:
        list: Delivery progress data
    """
    # Get progress data from cache
    progress_data = dashboard_cache.get_section_data('progress')
    
    if not progress_data:
        logger.warning("No progress data found in cache")
        return []
    
    # Apply filters
    filtered_data = progress_data
    
    if delivery_id:
        filtered_data = [item for item in filtered_data if str(item.get('delivery', '')) == delivery_id]
    
    if user_id:
        filtered_data = [item for item in filtered_data if str(item.get('created_by', '')) == user_id]
    
    return filtered_data

def get_scan_times(user_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get scan time data.
    
    Args:
        user_id (str, optional): Filter by user ID
        
    Returns:
        list: Scan time data
    """
    # Get scan time data from cache
    scan_times_data = dashboard_cache.get_section_data('scan_times')
    
    if not scan_times_data:
        logger.warning("No scan time data found in cache")
        return []
    
    # Apply filter
    if user_id:
        filtered_data = [item for item in scan_times_data if str(item.get('user_id', '')) == user_id]
        return filtered_data
    
    return scan_times_data

def track_user_activity(user_id: str, activity_type: str) -> None:
    """
    Track user activity.
    
    Args:
        user_id (str): User ID
        activity_type (str): Type of activity (e.g., 'scan', 'view')
    """
    timestamp = datetime.now().isoformat()
    
    # Initialize user's activity log if not exists
    if user_id not in user_activity_log:
        user_activity_log[user_id] = []
    
    # Add activity to log
    user_activity_log[user_id].append({
        'timestamp': timestamp,
        'activity_type': activity_type
    })
    
    # Trim log to keep only recent activities (last 100)
    if len(user_activity_log[user_id]) > 100:
        user_activity_log[user_id] = user_activity_log[user_id][-100:]
    
    logger.info(f"Tracked {activity_type} activity for user {user_id}")
    
    # Update user's last activity in cache
    users_data = dashboard_cache.get_section_data('users')
    
    if users_data:
        for user in users_data:
            if str(user.get('user_id', '')) == user_id:
                user['last_activity'] = timestamp
                user['last_activity_type'] = activity_type
                break
        
        # Update the cache
        dashboard_cache.set_section_data('users', users_data)

def get_real_time_updates() -> Dict[str, Any]:
    """
    Get real-time updates for the dashboard.
    
    Returns:
        dict: Updates since the last check
    """
    # Get the latest diff from cache
    diff = dashboard_cache.get('latest_diff', {})
    
    if not diff:
        # If no diff is available, return empty updates
        return {}
    
    # Add timestamp
    updates = {
        'timestamp': datetime.now().isoformat(),
        'updates': diff
    }
    
    return updates

def get_user_activity_history(user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Get activity history for a specific user.
    
    Args:
        user_id (str): User ID
        limit (int): Maximum number of activities to return
        
    Returns:
        list: User activity history
    """
    if user_id not in user_activity_log:
        return []
    
    # Get the user's activity log
    activities = user_activity_log[user_id]
    
    # Return the most recent activities up to the limit
    return activities[-limit:]

def calculate_user_metrics() -> Dict[str, Any]:
    """
    Calculate metrics for all users.
    
    Returns:
        dict: User metrics
    """
    metrics = {}
    
    # Get user data from cache
    users_data = dashboard_cache.get_section_data('users')
    
    if not users_data:
        return metrics
    
    # Calculate metrics for each user
    for user_id, activities in user_activity_log.items():
        # Skip users with no activities
        if not activities:
            continue
        
        # Count activities by type
        activity_counts = {}
        for activity in activities:
            activity_type = activity.get('activity_type', 'unknown')
            activity_counts[activity_type] = activity_counts.get(activity_type, 0) + 1
        
        # Calculate time since last activity
        last_activity_time = datetime.fromisoformat(activities[-1]['timestamp'])
        time_since_last = (datetime.now() - last_activity_time).total_seconds() / 60  # in minutes
        
        # Store metrics
        metrics[user_id] = {
            'activity_counts': activity_counts,
            'total_activities': len(activities),
            'last_activity_time': last_activity_time.isoformat(),
            'minutes_since_last_activity': round(time_since_last, 2)
        }
    
    return metrics
