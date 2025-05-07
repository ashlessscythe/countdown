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
        
        # Log the original columns for debugging
        logger.info(f"Original columns in ZMDESNR file: {df.columns.tolist()}")
        
        # Rename columns directly instead of creating new ones
        column_mapping = {
            'Serial #': 'serial_number',
            'Created by': 'created_by',
            'Created on': 'created_on',
            'Delivery': 'delivery',
            'Status': 'status',
            'Warehouse Number': 'warehouse_number'
        }
        
        # Rename columns that exist in the DataFrame
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # Sanitize headers
        df = sanitize_headers(df)
        
        # Log the columns after mapping and sanitizing
        logger.info(f"Columns after mapping and sanitizing: {df.columns.tolist()}")
        
        # Filter for warehouse
        if 'warehouse_number' in df.columns:
            df = df[df['warehouse_number'] == WAREHOUSE_FILTER]
        
        # Filter for pallets (where pallet column is 1)
        if 'pallet' in df.columns:
            df = df[df['pallet'] == 1]
        
        # Convert timestamp columns if needed
        if 'created_on' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['created_on']):
            df['created_on'] = pd.to_datetime(df['created_on'], errors='coerce')
        
        # Create scan_timestamp from time and created_on if they exist
        if 'time' in df.columns and 'created_on' in df.columns:
            try:
                # Convert time to string if it's not already
                if not pd.api.types.is_object_dtype(df['time']):
                    # If time is a datetime.time object, convert to string
                    df['time_str'] = df['time'].apply(lambda x: x.strftime('%H:%M:%S') if hasattr(x, 'strftime') else str(x))
                else:
                    df['time_str'] = df['time']
                
                # Create scan_timestamp by combining date from created_on and time_str
                df['scan_timestamp'] = pd.to_datetime(
                    df['created_on'].dt.strftime('%Y-%m-%d') + ' ' + df['time_str'], 
                    errors='coerce'
                )
                
                logger.info(f"Created scan_timestamp column from time and created_on")
            except Exception as e:
                logger.error(f"Error creating scan_timestamp: {str(e)}")
                # Create a fallback scan_timestamp using just created_on
                try:
                    df['scan_timestamp'] = df['created_on']
                    logger.info("Using created_on as fallback for scan_timestamp")
                except Exception as e2:
                    logger.error(f"Error creating fallback scan_timestamp: {str(e2)}")
        
        # Drop the first row if it's all NaN (header row)
        if not df.empty and df.iloc[0].isna().all():
            df = df.iloc[1:].reset_index(drop=True)
            logger.info("Dropped header row with all NaN values")
        
        # Log the number of rows after processing
        logger.info(f"ZMDESNR file processed: {len(df)} rows")
        
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
        
        # Log the original columns for debugging
        logger.info(f"Original columns in VL06O file: {df.columns.tolist()}")
        
        # Rename columns directly instead of creating new ones
        column_mapping = {
            'Delivery': 'delivery',
            'Number of packages': 'number_of_packages',
            'Shipping Point/Receiving Pt': 'shipping_point'
        }
        
        # Rename columns that exist in the DataFrame
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # Sanitize headers
        df = sanitize_headers(df)
        
        # Log the columns after mapping and sanitizing
        logger.info(f"Columns after mapping and sanitizing: {df.columns.tolist()}")
        
        # Drop the first row if it's all NaN (header row)
        if not df.empty and df.iloc[0].isna().all():
            df = df.iloc[1:].reset_index(drop=True)
            logger.info("Dropped header row with all NaN values")
        
        # Log the number of rows after processing
        logger.info(f"VL06O file processed: {len(df)} rows")
        
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
    
    # Log the columns in each dataframe
    logger.info(f"Serials DataFrame columns: {serials_df.columns.tolist()}")
    logger.info(f"Deliveries DataFrame columns: {deliveries_df.columns.tolist()}")
    
    if serials_df.empty or deliveries_df.empty:
        logger.warning("One or both dataframes are empty")
        return serials_df, deliveries_df, pd.DataFrame()
    
    # Check for delivery column in both dataframes
    delivery_col_serials = 'delivery' if 'delivery' in serials_df.columns else None
    delivery_col_deliveries = 'delivery' if 'delivery' in deliveries_df.columns else None
    
    # If delivery column is missing in serials_df but we have 'Delivery', use that
    if delivery_col_serials is None and 'Delivery' in serials_df.columns:
        serials_df['delivery'] = serials_df['Delivery']
        delivery_col_serials = 'delivery'
        logger.info("Using 'Delivery' column from serials_df as 'delivery'")
    
    # If delivery column is missing in deliveries_df but we have 'Delivery', use that
    if delivery_col_deliveries is None and 'Delivery' in deliveries_df.columns:
        deliveries_df['delivery'] = deliveries_df['Delivery']
        delivery_col_deliveries = 'delivery'
        logger.info("Using 'Delivery' column from deliveries_df as 'delivery'")
    
    # Ensure delivery column is of the same type in both dataframes
    if delivery_col_serials and delivery_col_deliveries:
        # Convert to integers first to remove decimal points, then to strings
        serials_df['delivery'] = serials_df[delivery_col_serials].fillna(0).astype(int).astype(str)
        deliveries_df['delivery'] = deliveries_df[delivery_col_deliveries].fillna(0).astype(int).astype(str)
        
        # Ensure number_of_packages column exists in deliveries_df
        if 'number_of_packages' not in deliveries_df.columns and 'Number of packages' in deliveries_df.columns:
            deliveries_df['number_of_packages'] = deliveries_df['Number of packages']
            logger.info("Using 'Number of packages' column as 'number_of_packages'")
        
        # Merge the dataframes on delivery
        combined_df = pd.merge(
            serials_df,
            deliveries_df,
            on='delivery',
            how='inner',
            suffixes=('_serial', '_delivery')
        )
        
        # Log the number of rows in the combined dataframe
        logger.info(f"Combined DataFrame: {len(combined_df)} rows")
        
        return serials_df, deliveries_df, combined_df
    else:
        logger.error(f"Delivery column missing in one or both dataframes. Serials columns: {serials_df.columns.tolist()}, Deliveries columns: {deliveries_df.columns.tolist()}")
        return serials_df, deliveries_df, pd.DataFrame()

if __name__ == "__main__":
    # Test the functions
    serials_df, deliveries_df, combined_df = get_combined_data()
    
    print(f"Serial numbers: {len(serials_df)} rows")
    print(f"Deliveries: {len(deliveries_df)} rows")
    print(f"Combined data: {len(combined_df)} rows")
