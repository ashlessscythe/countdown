"""
Compare Excel snapshots and calculate time differences between scans for each user.
"""
import os
import pandas as pd
import json
from datetime import datetime
import numpy as np

def load_excel_file(file_path):
    """Load an Excel file and return a DataFrame with standardized column names"""
    print(f"Loading file: {file_path}")
    
    # Read the Excel file
    df = pd.read_excel(file_path, dtype=str)
    
    # Standardize column names (convert to lowercase and replace spaces with underscores)
    df.columns = [col.lower().replace(' ', '_').replace('#', 'num') for col in df.columns]
    
    # Convert timestamp columns
    if 'created_on' in df.columns and 'time' in df.columns:
        # Combine date and time columns to create a timestamp, handling NaN values
        def create_timestamp(row):
            # Check if either created_on or time is NaN or empty
            if pd.isna(row['created_on']) or pd.isna(row['time']) or row['created_on'] == '' or row['time'] == '':
                return pd.NaT  # Return Not a Time for invalid entries
            try:
                return pd.to_datetime(f"{row['created_on']} {row['time']}")
            except:
                print(f"Warning: Could not parse datetime from '{row['created_on']} {row['time']}'")
                return pd.NaT
                
        df['timestamp'] = df.apply(create_timestamp, axis=1)
    
    # Drop rows with NaT timestamps
    if 'timestamp' in df.columns:
        valid_rows = ~df['timestamp'].isna()
        invalid_count = (~valid_rows).sum()
        if invalid_count > 0:
            print(f"Dropping {invalid_count} rows with invalid timestamps")
            df = df[valid_rows]
    
    return df

def compare_snapshots(file1, file2):
    """Compare two snapshots and calculate time differences"""
    # Load the Excel files
    df1 = load_excel_file(file1)
    df2 = load_excel_file(file2)
    
    # Identify common serial numbers
    common_serials = set(df1['serial_num']).intersection(set(df2['serial_num']))
    print(f"Found {len(common_serials)} common serial numbers")
    
    # Filter dataframes to only include common serial numbers
    df1_common = df1[df1['serial_num'].isin(common_serials)]
    df2_common = df2[df2['serial_num'].isin(common_serials)]
    
    # Create a dictionary to store results
    results = {
        "metadata": {
            "file1": os.path.basename(file1),
            "file2": os.path.basename(file2),
            "comparison_time": datetime.now().isoformat(),
            "common_serials_count": len(common_serials)
        },
        "user_scan_times": {},
        "serial_deltas": []
    }
    
    # Calculate time differences for each serial number
    for serial in common_serials:
        row1 = df1_common[df1_common['serial_num'] == serial].iloc[0]
        row2 = df2_common[df2_common['serial_num'] == serial].iloc[0]
        
        # Determine which snapshot is earlier
        if row1['timestamp'] < row2['timestamp']:
            earlier_row = row1
            later_row = row2
            earlier_file = "file1"
            later_file = "file2"
        else:
            earlier_row = row2
            later_row = row1
            earlier_file = "file2"
            later_file = "file1"
        
        # Calculate time difference in seconds
        time_diff = (later_row['timestamp'] - earlier_row['timestamp']).total_seconds()
        
        # Get user information
        earlier_user = earlier_row['created_by']
        later_user = later_row['created_by']
        
        # Add to user scan times dictionary
        if earlier_user not in results["user_scan_times"]:
            results["user_scan_times"][earlier_user] = {
                "total_scans": 0,
                "total_time": 0,
                "avg_time": 0,
                "scans": []
            }
        
        if later_user not in results["user_scan_times"]:
            results["user_scan_times"][later_user] = {
                "total_scans": 0,
                "total_time": 0,
                "avg_time": 0,
                "scans": []
            }
        
        # Update user scan information
        results["user_scan_times"][earlier_user]["total_scans"] += 1
        results["user_scan_times"][later_user]["total_scans"] += 1
        
        if earlier_user == later_user:
            # If same user, add to their total time
            results["user_scan_times"][earlier_user]["total_time"] += time_diff
            results["user_scan_times"][earlier_user]["scans"].append({
                "serial": serial,
                "time_diff": time_diff,
                "earlier_timestamp": earlier_row['timestamp'].isoformat(),
                "later_timestamp": later_row['timestamp'].isoformat()
            })
        
        # Add to serial deltas
        results["serial_deltas"].append({
            "serial": serial,
            "time_diff": time_diff,
            "earlier_file": earlier_file,
            "later_file": later_file,
            "earlier_user": earlier_user,
            "later_user": later_user,
            "earlier_timestamp": earlier_row['timestamp'].isoformat(),
            "later_timestamp": later_row['timestamp'].isoformat(),
            "status": earlier_row['status'],
            "customer_name": earlier_row['customer_name'],
            "material_number": earlier_row['material_number'],
            "delivery": earlier_row['delivery']
        })
    
    # Calculate average times for each user
    for user, data in results["user_scan_times"].items():
        if data["total_scans"] > 0 and data["total_time"] > 0:
            data["avg_time"] = data["total_time"] / data["total_scans"]
    
    return results

def get_newest_excel_files(directory):
    """Get the two newest Excel files by modified date"""
    # Get all Excel files
    excel_files = [f for f in os.listdir(directory) if f.endswith('.xlsx')]
    
    if len(excel_files) < 2:
        print("Need at least two Excel files for comparison")
        return None, None
    
    # Get full paths and sort by modified time (newest first)
    file_paths = [os.path.join(directory, f) for f in excel_files]
    file_paths.sort(key=os.path.getmtime, reverse=True)
    
    # Return the two newest files
    return file_paths[0], file_paths[1]

def main():
    """Main function to compare Excel snapshots"""
    import config
    input_dir = config.INPUT_DIR
    
    # Get the two newest Excel files
    file1, file2 = get_newest_excel_files(input_dir)
    
    if file1 is None or file2 is None:
        return
    
    print(f"Comparing files: {file1} and {file2}")
    results = compare_snapshots(file1, file2)
    
    # Save the results to a JSON file
    output_file = config.OUTPUT_JSON
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"Comparison results saved to: {output_file}")
    
    # Print a summary
    print("\nSummary:")
    print(f"Common serial numbers: {results['metadata']['common_serials_count']}")
    print("\nUser scan times:")
    for user, data in results["user_scan_times"].items():
        avg_time = data["avg_time"]
        if avg_time > 0:
            print(f"  {user}: {data['total_scans']} scans, avg time: {avg_time:.2f} seconds")

if __name__ == "__main__":
    main()
