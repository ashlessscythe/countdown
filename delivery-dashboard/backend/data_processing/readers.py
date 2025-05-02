"""
File readers for ZMDESNR and VL06O Excel files.
"""
import os
import pandas as pd
import glob
from datetime import datetime
import sys
import logging

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config import SERIAL_NUMBERS_DIR, DELIVERY_INFO_DIR, WAREHOUSE_FILTER

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def sanitize_headers(df):
    """
    Sanitize DataFrame headers to ensure consistency.
    
    Args:
        df (pandas.DataFrame): DataFrame to sanitize
        
    Returns:
        pandas.DataFrame: DataFrame with sanitized headers
    """
    # Convert headers to lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('#', 'number').str.replace('/', '_')
    return df

def get_latest_file(directory, pattern="*.xlsx"):
    """
    Get the most recent file in a directory matching the pattern.
    
    Args:
        directory (str): Directory to search
        pattern (str): File pattern to match
        
    Returns:
        str: Path to the most recent file, or None if no files found
    """
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        return None
    
    # Sort files by modification time (most recent first)
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]

def read_zmdesnr_file(file_path=None):
    """
    Read and process a ZMDESNR Excel file.
    
    Args:
        file_path (str, optional): Path to the file. If None, uses the most recent file.
        
    Returns:
        pandas.DataFrame: Processed DataFrame with serial numbers
    """
    if file_path is None:
        file_path = get_latest_file(SERIAL_NUMBERS_DIR)
        if file_path is None:
            logger.error(f"No ZMDESNR files found in {SERIAL_NUMBERS_DIR}")
            return pd.DataFrame()
    
    logger.info(f"Reading ZMDESNR file: {file_path}")
    
    try:
        # Read the Excel file
        df = pd.read_excel(file_path)
        
        # Sanitize headers
        df = sanitize_headers(df)
        
        # Filter for warehouse
        if 'warehouse_number' in df.columns:
            df = df[df['warehouse_number'] == WAREHOUSE_FILTER]
        
        # Filter for pallets (where pallet column is 1)
        if 'pallet' in df.columns:
            df = df[df['pallet'] == 1]
        
        # Convert timestamp columns if needed
        if 'created_on' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['created_on']):
            df['created_on'] = pd.to_datetime(df['created_on'], errors='coerce')
        
        if 'time' in df.columns and isinstance(df['time'].iloc[0] if not df.empty else None, str):
            df['scan_timestamp'] = pd.to_datetime(
                df['created_on'].dt.strftime('%Y-%m-%d') + ' ' + df['time'], 
                errors='coerce'
            )
        
        return df
    
    except Exception as e:
        logger.error(f"Error reading ZMDESNR file {file_path}: {str(e)}")
        return pd.DataFrame()

def read_vl06o_file(file_path=None):
    """
    Read and process a VL06O Excel file.
    
    Args:
        file_path (str, optional): Path to the file. If None, uses the most recent file.
        
    Returns:
        pandas.DataFrame: Processed DataFrame with delivery information
    """
    if file_path is None:
        file_path = get_latest_file(DELIVERY_INFO_DIR)
        if file_path is None:
            logger.error(f"No VL06O files found in {DELIVERY_INFO_DIR}")
            return pd.DataFrame()
    
    logger.info(f"Reading VL06O file: {file_path}")
    
    try:
        # Read the Excel file
        df = pd.read_excel(file_path)
        
        # Sanitize headers
        df = sanitize_headers(df)
        
        return df
    
    except Exception as e:
        logger.error(f"Error reading VL06O file {file_path}: {str(e)}")
        return pd.DataFrame()

def get_combined_data():
    """
    Combine data from ZMDESNR and VL06O files.
    
    Returns:
        tuple: (serials_df, deliveries_df, combined_df)
    """
    # Read the latest files
    serials_df = read_zmdesnr_file()
    deliveries_df = read_vl06o_file()
    
    if serials_df.empty or deliveries_df.empty:
        logger.warning("One or both dataframes are empty")
        return serials_df, deliveries_df, pd.DataFrame()
    
    # Ensure delivery column is of the same type in both dataframes
    if 'delivery' in serials_df.columns and 'delivery' in deliveries_df.columns:
        serials_df['delivery'] = serials_df['delivery'].astype(str)
        deliveries_df['delivery'] = deliveries_df['delivery'].astype(str)
        
        # Merge the dataframes on delivery
        combined_df = pd.merge(
            serials_df,
            deliveries_df,
            on='delivery',
            how='inner',
            suffixes=('_serial', '_delivery')
        )
        
        return serials_df, deliveries_df, combined_df
    else:
        logger.error("Delivery column missing in one or both dataframes")
        return serials_df, deliveries_df, pd.DataFrame()

if __name__ == "__main__":
    # Test the functions
    serials_df, deliveries_df, combined_df = get_combined_data()
    
    print(f"Serial numbers: {len(serials_df)} rows")
    print(f"Deliveries: {len(deliveries_df)} rows")
    print(f"Combined data: {len(combined_df)} rows")
