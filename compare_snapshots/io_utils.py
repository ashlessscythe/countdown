"""
I/O Utilities for Compare Snapshots

This module provides functions for file operations:
- Loading Excel snapshots
- Listing and filtering files by timestamp
- Extracting timestamp from filenames
"""

import os
import re
import pandas as pd
from datetime import datetime, timedelta
import config


def extract_timestamp_from_filename(filename):
    """
    Extract timestamp from filename format: YYYYMMDDHHMMSS-*.xlsx
    
    Args:
        filename (str): Filename to extract timestamp from
        
    Returns:
        datetime: Extracted timestamp or None if not found
    """
    # Get just the filename without path
    basename = os.path.basename(filename)
    
    # Try different regex patterns for timestamp extraction
    patterns = [
        # Standard format: YYYYMMDDHHMMSS-*.xlsx
        r'^(\d{14})-.*\.xlsx$',
        # Alternative format: *-YYYYMMDDHHMMSS.xlsx
        r'.*-(\d{14})\.xlsx$',
        # Just digits: YYYYMMDDHHMMSS.xlsx
        r'^(\d{14})\.xlsx$'
    ]
    
    for pattern in patterns:
        match = re.match(pattern, basename)
        if match:
            timestamp_str = match.group(1)
            try:
                # Parse YYYYMMDDHHMMSS format
                return datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
            except ValueError:
                continue
    
    # If no pattern matched, return None
    return None


def list_files(directory, pattern='*.xlsx'):
    """
    List all Excel files in the directory, sorted by timestamp in filename
    
    Args:
        directory (str): Directory to search for files
        pattern (str): File pattern to match (default: *.xlsx)
        
    Returns:
        list: Sorted list of file paths
    """
    import glob
    
    # Get all matching files
    file_paths = glob.glob(os.path.join(directory, pattern))
    
    # Custom sort key function that handles None values
    def sort_key(path):
        timestamp = extract_timestamp_from_filename(path)
        # If timestamp is None, use the file's modification time as fallback
        if timestamp is None:
            return datetime.fromtimestamp(os.path.getmtime(path))
        return timestamp
    
    # Sort files by timestamp in filename
    return sorted(file_paths, key=sort_key)


def filter_by_age(file_paths, window_minutes):
    """
    Filter files by age based on timestamp in filename
    
    Args:
        file_paths (list): List of file paths
        window_minutes (int): Window in minutes to include files
        
    Returns:
        list: Filtered list of file paths
    """
    cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
    print(f"Cutoff time: {cutoff_time}")
    
    # Filter files by timestamp
    filtered_files = []
    for path in file_paths:
        filename = os.path.basename(path)
        timestamp = extract_timestamp_from_filename(path)
        
        # Debug output
        print(f"File: {filename}")
        print(f"  Timestamp from filename: {timestamp}")
        
        # If timestamp is None, use file modification time as fallback
        if timestamp is None:
            timestamp = datetime.fromtimestamp(os.path.getmtime(path))
            print(f"  Using file modification time instead: {timestamp}")
        
        # Check if file is within window
        if timestamp >= cutoff_time:
            print(f"  INCLUDED: Within {window_minutes}-minute window")
            filtered_files.append(path)
        else:
            print(f"  EXCLUDED: Outside {window_minutes}-minute window")
    
    return filtered_files


def load_snapshot(file_path):
    """
    Load an Excel snapshot file and return a standardized DataFrame
    
    Args:
        file_path (str): Path to Excel file
        
    Returns:
        pandas.DataFrame: Standardized DataFrame with consistent column names
    """
    print(f"Loading snapshot: {os.path.basename(file_path)}")
    
    # Determine Excel engine based on file extension
    engine = "pyxlsb" if file_path.lower().endswith(".xlsb") else "openpyxl"
    
    # Read Excel file
    try:
        df = pd.read_excel(file_path, engine=engine)
        print(f"  Excel loaded successfully with {len(df)} rows")
    except Exception as e:
        print(f"  Error loading Excel file: {str(e)}")
        raise
    
    # Print the actual column names for debugging
    print(f"  Actual columns in file: {df.columns.tolist()}")
    
    # Extract timestamp from filename
    snapshot_time = extract_timestamp_from_filename(file_path)
    
    # If timestamp is None, use file modification time as fallback
    if snapshot_time is None:
        snapshot_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        print(f"  Using file modification time for snapshot_time: {snapshot_time}")
    else:
        print(f"  Using timestamp from filename: {snapshot_time}")
    
    # Check if all required columns exist in the DataFrame
    missing_cols = [col for col in config.COLUMNS.keys() if col not in df.columns]
    if missing_cols:
        print(f"  Warning: Missing columns in Excel file: {missing_cols}")
        print(f"  Available columns: {df.columns.tolist()}")
        
        # If columns are missing, try case-insensitive matching
        col_map = {}
        for required_col in missing_cols:
            for df_col in df.columns:
                if df_col.lower() == required_col.lower():
                    col_map[df_col] = required_col
                    print(f"  Found case-insensitive match: '{df_col}' -> '{required_col}'")
        
        # Rename columns based on case-insensitive matching
        if col_map:
            df = df.rename(columns=col_map)
    
    # Rename columns according to config mapping
    df = df.rename(columns=config.COLUMNS)
    
    # Check if all required columns exist after renaming
    missing_cols = [col for col in config.COLUMNS.values() if col not in df.columns]
    if missing_cols:
        print(f"  Error: Still missing required columns after renaming: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Select only the columns we need
    required_cols = list(config.COLUMNS.values())
    df = df[required_cols].copy()
    
    # Sanitize the user column (strip and uppercase)
    df['user'] = df['user'].str.strip().str.upper()
    
    # Add snapshot_time column
    df['snapshot_time'] = snapshot_time
    
    # Convert columns to appropriate types
    for col in required_cols:
        if col != 'time' and col != 'created_on':  # Skip time and date columns for string conversion
            df[col] = df[col].astype('string')
    
    # Convert status and user to category for efficiency
    df['status'] = df['status'].astype('category')
    df['user'] = df['user'].astype('category')
    
    # Ensure time column is datetime type
    # If 'time' column is in the config but not in the data, create it with snapshot_time
    if 'time' in config.COLUMNS.values() and 'time' not in df.columns:
        print(f"  'time' column not found in data, using snapshot_time instead")
        df['time'] = df['snapshot_time']
    elif 'time' in df.columns:
        # Convert existing time column to datetime
        try:
            # Print the data type and first few values of the 'time' column
            print(f"  'time' column dtype: {df['time'].dtype}")
            print(f"  'time' column first few values: {df['time'].head().tolist()}")
            
            # Check if 'time' column is of object type (which could be strings or datetime.time objects)
            if df['time'].dtype == 'object':
                print(f"  'time' column is of object type")
                
                # If 'created_on' column exists, combine it with 'time'
                if 'created_on' in df.columns:
                    print(f"  'created_on' column exists")
                    # Print the data type and first few values of the 'created_on' column
                    print(f"  'created_on' column dtype: {df['created_on'].dtype}")
                    print(f"  'created_on' column first few values: {df['created_on'].head().tolist()}")
                    
                    # Convert 'created_on' to datetime if it's not already
                    if df['created_on'].dtype != 'datetime64[ns]':
                        df['created_on'] = pd.to_datetime(df['created_on'], errors='coerce')
                        print(f"  Converted 'created_on' column to datetime")
                    
                    # Create a new column with combined date and time
                    print(f"  Creating datetime column by combining date and time")
                    
                    # Import datetime module
                    import datetime as dt
                    
                    # Create a function to combine date and time
                    def combine_date_time(row):
                        if pd.notna(row['created_on']) and pd.notna(row['time']):
                            try:
                                # Get the date part from created_on
                                if isinstance(row['created_on'], pd.Timestamp):
                                    date_part = row['created_on'].date()
                                    
                                    # Handle different time formats
                                    if isinstance(row['time'], dt.time):
                                        # If it's a datetime.time object, combine directly
                                        return pd.Timestamp(dt.datetime.combine(date_part, row['time']))
                                    elif isinstance(row['time'], str):
                                        # If it's a string, parse it and combine
                                        time_obj = dt.datetime.strptime(row['time'], '%H:%M:%S').time()
                                        return pd.Timestamp(dt.datetime.combine(date_part, time_obj))
                            except Exception as e:
                                print(f"  Error combining date and time: {e}")
                        return pd.NaT
                    
                    # Apply the function to create the datetime column
                    df['time'] = df.apply(combine_date_time, axis=1)
                    print(f"  Combined 'created_on' and 'time' columns into 'time' column")
                    print(f"  'time' column dtype after combination: {df['time'].dtype}")
                    print(f"  'time' column first few values after combination: {df['time'].head().tolist()}")
                else:
                    print(f"  'created_on' column does not exist")
                    # If no 'created_on' column, try to convert 'time' to datetime directly
                    df['time'] = pd.to_datetime(df['time'], errors='coerce')
                    print(f"  Converted 'time' directly to datetime")
            else:
                print(f"  'time' column is not of object type")
                # If 'time' is not an object, try to convert it to datetime directly
                df['time'] = pd.to_datetime(df['time'], errors='coerce')
                print(f"  Converted 'time' directly to datetime")
            
            # Print the data type and first few values of the 'time' column after conversion
            print(f"  'time' column dtype after conversion: {df['time'].dtype}")
            print(f"  'time' column first few values after conversion: {df['time'].head().tolist()}")
        except Exception as e:
            print(f"  Error converting 'time' column to datetime: {str(e)}")
            # If conversion fails, use snapshot_time as fallback
            df['time'] = df['snapshot_time']
            print(f"  Using snapshot_time as fallback for 'time' column")
    
    print(f"  DataFrame processed successfully")
    return df


def save_parquet(dataframes, output_dir):
    """
    Save DataFrames to Parquet files
    
    Args:
        dataframes (dict): Dictionary of DataFrames to save
        output_dir (str): Directory to save Parquet files
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Save each DataFrame to a Parquet file
    for name, df in dataframes.items():
        file_path = os.path.join(output_dir, f"{name}.parquet")
        df.to_parquet(file_path, engine="pyarrow", index=True)
