"""
Convert Excel files to JSON snapshots.
This script can be used to manually convert Excel files to JSON snapshots.
"""
import os
import pandas as pd
import json
import re
import sys
import time
from datetime import datetime, timezone
import config

def ensure_directory_exists(directory):
    """Ensure that a directory exists, creating it if necessary"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def setup_directory_structure():
    """Set up the directory structure for snapshots and changes"""
    # Create base directories if they don't exist
    data_dir = os.path.join(config.BASE_DIR, "data")
    snapshots_dir = os.path.join(data_dir, "snapshots")
    changes_dir = os.path.join(data_dir, "changes")
    
    ensure_directory_exists(data_dir)
    ensure_directory_exists(snapshots_dir)
    ensure_directory_exists(changes_dir)
    
    return {
        "data_dir": data_dir,
        "snapshots_dir": snapshots_dir,
        "changes_dir": changes_dir
    }

def to_snake_case(name):
    """Convert a column name to snake_case"""
    # Replace special characters with spaces
    s = re.sub(r'[^\w\s]', ' ', name)
    # Replace consecutive spaces with a single space
    s = re.sub(r'\s+', ' ', s)
    # Convert to lowercase and replace spaces with underscores
    return s.strip().lower().replace(' ', '_')

def sanitize_dataframe(df):
    """Convert all column names to snake_case"""
    column_mapping = {col: to_snake_case(col) for col in df.columns}
    return df.rename(columns=column_mapping), column_mapping

def generate_timestamp_filename(extension="json", source_file=None):
    """Generate a filename with the current timestamp"""
    # Use microseconds to ensure uniqueness
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    
    # If source file is provided, extract its timestamp and use it
    if source_file:
        try:
            source_timestamp = os.path.basename(source_file).split('_')[0]
            if len(source_timestamp) >= 14:  # Ensure it's a valid timestamp
                timestamp = f"{source_timestamp[:8]}-{source_timestamp[8:10]}{source_timestamp[10:12]}{source_timestamp[12:14]}"
        except (IndexError, ValueError):
            # If there's an error extracting the timestamp, use the current time
            pass
    
    # Add a random suffix to ensure uniqueness
    suffix = str(int(time.time() * 1000) % 1000)
    return f"{timestamp}_{suffix}.{extension}"

def excel_to_snapshot(excel_path, output_dir):
    """Convert an Excel file to a JSON snapshot and save it in the snapshots directory"""
    print(f"Converting Excel to snapshot: {excel_path}")
    
    # Read the Excel file
    df = pd.read_excel(excel_path, dtype=str)
    
    # Sanitize column names
    df, _ = sanitize_dataframe(df)
    
    # Fill NaN values with empty strings
    df.fillna("", inplace=True)
    
    # Apply warehouse filter if configured
    warehouse_col = "warehouse_number"
    if hasattr(config, 'FILTER_WHSE') and config.FILTER_WHSE and warehouse_col in df.columns:
        filter_whse = config.FILTER_WHSE
        print(f"Applying warehouse filter: {filter_whse}")
        df = df[df[warehouse_col] == filter_whse]
        print(f"After filtering: {len(df)} rows")
    
    # Filter by Status in ['ASH', 'SHP'] as suggested
    status_col = "status"
    if status_col in df.columns:
        df = df[df[status_col].str.upper().isin(['ASH', 'SHP'])]
        print(f"After status filtering: {len(df)} rows")
    
    # Define the key columns we need
    key_col = "serial"
    status_col = "status"
    
    # Additional columns to include in the output
    additional_cols = [
        "delivery",
        "customer_name",
        "shipment_number",
        "created_by",
        "time",
        "scan_time",
        "timestamp",
        "scan_timestamp",
        "created_at",
        "updated_at"
    ]
    
    # Select only the columns that exist in the dataframe
    cols = [col for col in [key_col, status_col] + additional_cols if col in df.columns]
    
    # Create a list of records
    records = []
    snapshot_time = datetime.now(timezone.utc).isoformat()
    
    for _, row in df[cols].iterrows():
        record = row.to_dict()
        # Add snapshot_time to each record
        record["snapshot_time"] = snapshot_time
        records.append(record)
    
    # Generate a filename with timestamp based on the source file
    filename = generate_timestamp_filename(extension="json", source_file=excel_path)
    output_path = os.path.join(output_dir, filename)
    
    # Save the snapshot as JSON
    with open(output_path, 'w') as f:
        json.dump({
            "metadata": {
                "source_file": os.path.basename(excel_path),
                "created_at": snapshot_time,
                "record_count": len(records)
            },
            "records": records
        }, f, indent=2)
    
    print(f"Snapshot saved to: {output_path}")
    return output_path

def main():
    """Main function to process command line arguments and run the script"""
    # Set up directory structure
    dirs = setup_directory_structure()
    snapshots_dir = dirs["snapshots_dir"]
    
    # Check if an Excel file was provided
    if len(sys.argv) > 1:
        excel_path = sys.argv[1]
        if not os.path.exists(excel_path):
            print(f"Error: Excel file '{excel_path}' does not exist")
            return
        
        excel_to_snapshot(excel_path, snapshots_dir)
    else:
        # Process the latest Excel file in the input directory
        input_dir = config.INPUT_DIR
        if not os.path.exists(input_dir):
            print(f"Error: Input directory '{input_dir}' does not exist")
            return
        
        # Get all Excel files
        excel_files = [f for f in os.listdir(input_dir) if f.endswith('.xlsx')]
        
        if not excel_files:
            print("No Excel files found")
            return
        
        # Sort Excel files by modification time (newest first)
        excel_files.sort(key=lambda f: os.path.getmtime(os.path.join(input_dir, f)), reverse=True)
        
        # Process the latest Excel file
        latest_excel = excel_files[0]
        excel_path = os.path.join(input_dir, latest_excel)
        excel_to_snapshot(excel_path, snapshots_dir)

if __name__ == "__main__":
    main()
