"""
Utility Functions for Shipment Tracker

This module contains common utility functions used across the shipment tracker project.
"""

import re
import logging
import pandas as pd
from pathlib import Path

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

def find_column_by_variants(df, variants):
    """
    Find a column in a DataFrame by checking multiple possible names.
    
    Args:
        df (DataFrame): DataFrame to search in
        variants (list): List of possible column names to check
        
    Returns:
        str or None: The first matching column name found, or None if no match
    """
    return next((col for col in variants if col in df.columns), None)

def safe_convert_column_type(df, column, target_type, errors='coerce'):
    """
    Safely convert a DataFrame column to a specified type.
    
    Args:
        df (DataFrame): DataFrame containing the column
        column (str): Name of the column to convert
        target_type (str): Type to convert to ('str', 'int', 'float', etc.)
        errors (str): How to handle errors ('raise', 'coerce', 'ignore')
        
    Returns:
        DataFrame: DataFrame with the column converted to the specified type
    """
    if column not in df.columns:
        return df
    
    try:
        if target_type == 'str':
            df[column] = df[column].astype(str)
        elif target_type == 'int':
            df[column] = pd.to_numeric(df[column], errors=errors).astype('Int64')  # Int64 allows NaN values
        elif target_type == 'float':
            df[column] = pd.to_numeric(df[column], errors=errors)
        elif target_type == 'datetime':
            df[column] = pd.to_datetime(df[column], errors=errors)
        
        logging.info(f"Converted column '{column}' to {target_type}")
    except Exception as e:
        logging.error(f"Error converting column '{column}' to {target_type}: {e}")
    
    return df

def merge_dataframes_safely(df1, df2, left_on, right_on, how='inner'):
    """
    Safely merge two DataFrames, handling type mismatches by converting to string.
    
    Args:
        df1 (DataFrame): First DataFrame
        df2 (DataFrame): Second DataFrame
        left_on (list): Columns from df1 to join on
        right_on (list): Columns from df2 to join on
        how (str): Type of merge to perform ('left', 'right', 'outer', 'inner')
        
    Returns:
        DataFrame: Merged DataFrame
    """
    # Create copies to avoid modifying the original DataFrames
    df1_copy = df1.copy()
    df2_copy = df2.copy()
    
    # Convert join columns to string in both DataFrames
    for col in left_on:
        if col in df1_copy.columns:
            df1_copy[col] = df1_copy[col].astype(str)
    
    for col in right_on:
        if col in df2_copy.columns:
            df2_copy[col] = df2_copy[col].astype(str)
    
    # Perform the merge
    try:
        result = df1_copy.merge(df2_copy, left_on=left_on, right_on=right_on, how=how)
        logging.info(f"Successfully merged DataFrames. Result has {len(result)} rows.")
        return result
    except Exception as e:
        logging.error(f"Error merging DataFrames: {e}")
        
        # Try an alternative approach using pd.concat
        logging.info("Trying alternative approach with pd.concat...")
        
        # Create a common key for joining
        for i, (left_col, right_col) in enumerate(zip(left_on, right_on)):
            key_name = f"join_key_{i}"
            df1_copy[key_name] = df1_copy[left_col].astype(str)
            df2_copy[key_name] = df2_copy[right_col].astype(str)
        
        join_keys = [f"join_key_{i}" for i in range(len(left_on))]
        
        # Set index for both DataFrames
        df1_copy = df1_copy.set_index(join_keys)
        df2_copy = df2_copy.set_index(join_keys)
        
        # Drop the columns used for joining from df2 to avoid duplicates
        for col in right_on:
            if col in df2_copy.columns and col in df1_copy.columns:
                df2_copy = df2_copy.drop(col, axis=1)
        
        # Concatenate the DataFrames
        result = pd.concat([df1_copy, df2_copy], axis=1, join=how)
        
        # Reset index
        result = result.reset_index()
        
        # Drop the temporary join key columns
        for key in join_keys:
            if key in result.columns:
                result = result.drop(key, axis=1)
        
        logging.info(f"Alternative merge approach resulted in {len(result)} rows.")
        return result
