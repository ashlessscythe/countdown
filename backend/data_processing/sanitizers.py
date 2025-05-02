"""
Header sanitization and data cleaning functions.
"""
import pandas as pd
import re
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def standardize_column_names(df):
    """
    Standardize column names to a consistent format.
    
    Args:
        df (pandas.DataFrame): DataFrame with columns to standardize
        
    Returns:
        pandas.DataFrame: DataFrame with standardized column names
    """
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Convert to lowercase, replace spaces with underscores, and remove special characters
    df.columns = [
        re.sub(r'[^a-zA-Z0-9_]', '', col.lower().replace(' ', '_').replace('#', 'number').replace('/', '_'))
        for col in df.columns
    ]
    
    # Map common variations to standard names
    column_mapping = {
        'serialnumber': 'serial_number',
        'serial_': 'serial_number',
        'serialno': 'serial_number',
        'serial': 'serial_number',
        'deliverynumber': 'delivery',
        'delivery_number': 'delivery',
        'deliveryno': 'delivery',
        'warehouse': 'warehouse_number',
        'warehouseno': 'warehouse_number',
        'warehouseid': 'warehouse_number',
        'warehouse_id': 'warehouse_number',
        'createdon': 'created_on',
        'created_date': 'created_on',
        'createdby': 'created_by',
        'user': 'created_by',
        'userid': 'created_by',
        'user_id': 'created_by',
        'numberofpackages': 'number_of_packages',
        'package_count': 'number_of_packages',
        'packages': 'number_of_packages',
        'shippingpoint': 'shipping_point',
        'shipping_pointreceiving_pt': 'shipping_point',
    }
    
    # Apply the mapping where matches are found
    df.columns = [column_mapping.get(col, col) for col in df.columns]
    
    return df

def clean_numeric_columns(df, columns):
    """
    Clean numeric columns by converting to appropriate numeric types.
    
    Args:
        df (pandas.DataFrame): DataFrame with columns to clean
        columns (list): List of column names to clean
        
    Returns:
        pandas.DataFrame: DataFrame with cleaned numeric columns
    """
    df = df.copy()
    
    for col in columns:
        if col in df.columns:
            try:
                # Convert to numeric, coercing errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception as e:
                logger.warning(f"Error converting column {col} to numeric: {str(e)}")
    
    return df

def clean_date_columns(df, columns):
    """
    Clean date columns by converting to datetime.
    
    Args:
        df (pandas.DataFrame): DataFrame with columns to clean
        columns (list): List of column names to clean
        
    Returns:
        pandas.DataFrame: DataFrame with cleaned date columns
    """
    df = df.copy()
    
    for col in columns:
        if col in df.columns:
            try:
                # Convert to datetime, coercing errors to NaT
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                logger.warning(f"Error converting column {col} to datetime: {str(e)}")
    
    return df

def sanitize_dataframe(df, numeric_columns=None, date_columns=None):
    """
    Perform complete sanitization of a DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame to sanitize
        numeric_columns (list, optional): List of columns to convert to numeric
        date_columns (list, optional): List of columns to convert to datetime
        
    Returns:
        pandas.DataFrame: Sanitized DataFrame
    """
    if df.empty:
        return df
    
    # Standardize column names
    df = standardize_column_names(df)
    
    # Clean numeric columns if specified
    if numeric_columns:
        df = clean_numeric_columns(df, numeric_columns)
    
    # Clean date columns if specified
    if date_columns:
        df = clean_date_columns(df, date_columns)
    
    return df

def sanitize_zmdesnr_dataframe(df):
    """
    Sanitize a ZMDESNR DataFrame with specific column handling.
    
    Args:
        df (pandas.DataFrame): ZMDESNR DataFrame to sanitize
        
    Returns:
        pandas.DataFrame: Sanitized DataFrame
    """
    numeric_columns = ['serial_number', 'pallet', 'delivery', 'qty']
    date_columns = ['created_on']
    
    return sanitize_dataframe(df, numeric_columns, date_columns)

def sanitize_vl06o_dataframe(df):
    """
    Sanitize a VL06O DataFrame with specific column handling.
    
    Args:
        df (pandas.DataFrame): VL06O DataFrame to sanitize
        
    Returns:
        pandas.DataFrame: Sanitized DataFrame
    """
    numeric_columns = ['delivery', 'number_of_packages']
    
    return sanitize_dataframe(df, numeric_columns)
