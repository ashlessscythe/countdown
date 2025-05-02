"""
Parquet file management for storing processed data.
"""
import os
import pandas as pd
import json
from datetime import datetime
import logging
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config import OUT_DIR

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def save_to_parquet(data, data_type, timestamp=None):
    """
    Save data to a Parquet file.
    
    Args:
        data (pandas.DataFrame or dict): Data to save
        data_type (str): Type of data (e.g., 'deliveries', 'users', 'progress')
        timestamp (str, optional): Timestamp to use in the filename. If None, current time is used.
        
    Returns:
        str: Path to the saved Parquet file
    """
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Create the output directory if it doesn't exist
    os.makedirs(OUT_DIR, exist_ok=True)
    
    # Convert dict to DataFrame if needed
    if isinstance(data, dict):
        # Handle dashboard data with multiple sections
        if data_type == 'dashboard':
            # Save each section to a separate Parquet file
            file_paths = {}
            for section, section_data in data.items():
                if section_data:  # Only save non-empty sections
                    try:
                        df = pd.DataFrame(section_data)
                        file_path = os.path.join(OUT_DIR, f"{section}_{timestamp}.parquet")
                        df.to_parquet(file_path, index=False)
                        logger.info(f"Saved {section} data to {file_path}")
                        file_paths[section] = file_path
                    except Exception as e:
                        logger.error(f"Error saving {section} data to Parquet: {str(e)}")
            return file_paths
        else:
            # Convert single section to DataFrame
            try:
                df = pd.DataFrame(data)
            except Exception as e:
                logger.error(f"Error converting data to DataFrame: {str(e)}")
                return None
    else:
        df = data
    
    # Save the DataFrame to a Parquet file
    file_path = os.path.join(OUT_DIR, f"{data_type}_{timestamp}.parquet")
    try:
        df.to_parquet(file_path, index=False)
        logger.info(f"Saved {data_type} data to {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Error saving {data_type} data to Parquet: {str(e)}")
        return None

def load_from_parquet(file_path):
    """
    Load data from a Parquet file.
    
    Args:
        file_path (str): Path to the Parquet file
        
    Returns:
        pandas.DataFrame: Loaded data
    """
    try:
        df = pd.read_parquet(file_path)
        logger.info(f"Loaded data from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading data from {file_path}: {str(e)}")
        return pd.DataFrame()

def get_latest_parquet(data_type):
    """
    Get the most recent Parquet file for a specific data type.
    
    Args:
        data_type (str): Type of data (e.g., 'deliveries', 'users', 'progress')
        
    Returns:
        str: Path to the most recent Parquet file, or None if no files found
    """
    pattern = f"{data_type}_*.parquet"
    files = [os.path.join(OUT_DIR, f) for f in os.listdir(OUT_DIR) if f.startswith(data_type) and f.endswith('.parquet')]
    
    if not files:
        return None
    
    # Sort files by modification time (most recent first)
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]

def save_dashboard_data_to_parquet(dashboard_data, timestamp=None):
    """
    Save dashboard data to Parquet files.
    
    Args:
        dashboard_data (dict): Dashboard data with sections
        timestamp (str, optional): Timestamp to use in the filename. If None, current time is used.
        
    Returns:
        dict: Paths to the saved Parquet files
    """
    return save_to_parquet(dashboard_data, 'dashboard', timestamp)

def diff_dashboard_data(current_data, previous_data):
    """
    Calculate the difference between current and previous dashboard data.
    
    Args:
        current_data (dict): Current dashboard data
        previous_data (dict): Previous dashboard data
        
    Returns:
        dict: Differences between the two datasets
    """
    if not previous_data:
        return {'added': current_data, 'removed': {}, 'changed': {}}
    
    diff = {'added': {}, 'removed': {}, 'changed': {}}
    
    # Process each section
    for section in current_data.keys():
        if section not in previous_data:
            diff['added'][section] = current_data[section]
            continue
        
        # Convert to DataFrames for easier comparison
        try:
            current_df = pd.DataFrame(current_data[section])
            previous_df = pd.DataFrame(previous_data[section])
            
            # Identify primary key columns based on section
            if section == 'users':
                key_cols = ['user_id']
            elif section == 'deliveries':
                key_cols = ['delivery']
            elif section == 'progress':
                key_cols = ['delivery', 'created_by']
            elif section == 'scan_times':
                key_cols = ['user_id']
            else:
                # Default to using all columns as keys
                key_cols = current_df.columns.tolist()
            
            # Find added records
            if not current_df.empty and not previous_df.empty:
                # Create sets of tuples for comparison
                current_keys = set(tuple(row) for row in current_df[key_cols].values)
                previous_keys = set(tuple(row) for row in previous_df[key_cols].values)
                
                # Added records
                added_keys = current_keys - previous_keys
                if added_keys:
                    added_mask = current_df[key_cols].apply(lambda row: tuple(row) in added_keys, axis=1)
                    diff['added'][section] = current_df[added_mask].to_dict('records')
                
                # Removed records
                removed_keys = previous_keys - current_keys
                if removed_keys:
                    removed_mask = previous_df[key_cols].apply(lambda row: tuple(row) in removed_keys, axis=1)
                    diff['removed'][section] = previous_df[removed_mask].to_dict('records')
                
                # Changed records
                common_keys = current_keys.intersection(previous_keys)
                changed_records = []
                
                for key in common_keys:
                    current_record = current_df[current_df[key_cols].apply(lambda row: tuple(row) == key, axis=1)]
                    previous_record = previous_df[previous_df[key_cols].apply(lambda row: tuple(row) == key, axis=1)]
                    
                    # Compare non-key columns
                    non_key_cols = [col for col in current_df.columns if col not in key_cols]
                    
                    for col in non_key_cols:
                        if current_record[col].values[0] != previous_record[col].values[0]:
                            changed_record = current_record.copy()
                            changed_record['_changed_column'] = col
                            changed_record['_previous_value'] = previous_record[col].values[0]
                            changed_records.append(changed_record.to_dict('records')[0])
                            break
                
                if changed_records:
                    diff['changed'][section] = changed_records
            
            elif not previous_df.empty and current_df.empty:
                # All records removed
                diff['removed'][section] = previous_df.to_dict('records')
            
            elif not current_df.empty and previous_df.empty:
                # All records added
                diff['added'][section] = current_df.to_dict('records')
        
        except Exception as e:
            logger.error(f"Error calculating diff for {section}: {str(e)}")
    
    return diff
