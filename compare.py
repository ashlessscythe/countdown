import os
import pandas as pd
import json
import re
from datetime import datetime, timezone
import config

def get_sorted_files_by_type(directory):
    files = os.listdir(directory)
    categorized = {}
    
    for f in files:
        if not f.endswith(".xlsx"):
            continue
        try:
            timestamp, suffix = f.split("_")
            file_type = suffix.split(".")[0]
            categorized.setdefault(file_type, []).append((timestamp, f))
        except ValueError:
            continue  # Skip malformed names

    for file_type in categorized:
        categorized[file_type] = sorted(categorized[file_type], reverse=True)
    return categorized

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

def compare_excel_files(path1, path2):
    print(f"Reading files:\n  - {path1}\n  - {path2}")
    
    df1 = pd.read_excel(path1, dtype=str)
    df2 = pd.read_excel(path2, dtype=str)

    print(f"Original columns in file 1: {list(df1.columns)}")
    print(f"Original columns in file 2: {list(df2.columns)}")

    # Sanitize column names
    df1, mapping1 = sanitize_dataframe(df1)
    df2, mapping2 = sanitize_dataframe(df2)
    
    print(f"Sanitized columns in file 1: {list(df1.columns)}")
    print(f"Sanitized columns in file 2: {list(df2.columns)}")
    
    df1.fillna("", inplace=True)
    df2.fillna("", inplace=True)
    
    # Apply warehouse filter if configured
    warehouse_col = "warehouse_number"
    if hasattr(config, 'FILTER_WHSE') and config.FILTER_WHSE:
        filter_whse = config.FILTER_WHSE
        
        # Apply filter to first dataframe if column exists
        if warehouse_col in df1.columns:
            print(f"Applying warehouse filter to first file: {filter_whse}")
            df1 = df1[df1[warehouse_col] == filter_whse]
            print(f"After filtering first file: {len(df1)} rows")
            
        # Apply filter to second dataframe if column exists
        if warehouse_col in df2.columns:
            print(f"Applying warehouse filter to second file: {filter_whse}")
            df2 = df2[df2[warehouse_col] == filter_whse]
            print(f"After filtering second file: {len(df2)} rows")

    # Define the key columns we need for comparison
    key_col = "serial"        # "Serial #" becomes "serial"
    status_col = "status"     # "Status" becomes "status"
    
    # Additional columns to include in the output
    additional_cols = [
        "delivery",           # "Delivery" becomes "delivery"
        "customer_name",      # "Customer Name" becomes "customer_name"
        "shipment_number",    # "Shipment" becomes "shipment" (if available)
        "created_by"          # "Created by" becomes "created_by" (if available)
    ]
    
    # Check if the required columns exist
    if key_col not in df1.columns:
        raise KeyError(f"Could not find '{key_col}' column in first file. Available columns: {list(df1.columns)}")
    if status_col not in df1.columns:
        raise KeyError(f"Could not find '{status_col}' column in first file. Available columns: {list(df1.columns)}")
    if key_col not in df2.columns:
        raise KeyError(f"Could not find '{key_col}' column in second file. Available columns: {list(df2.columns)}")
    if status_col not in df2.columns:
        raise KeyError(f"Could not find '{status_col}' column in second file. Available columns: {list(df2.columns)}")

    # Create lists of columns to select from each dataframe
    cols1 = [key_col, status_col] + [col for col in additional_cols if col in df1.columns]
    cols2 = [key_col, status_col] + [col for col in additional_cols if col in df2.columns]
    
    # Merge dataframes on the key column
    merged = df1[cols1].merge(
        df2[cols2], on=key_col, how="outer", suffixes=("_old", "_new"), indicator=True
    )

    changes = []
    # Track serials that have already been processed to avoid duplicates
    processed_serials = set()
    
    for _, row in merged.iterrows():
        serial = row[key_col]
        old_status = "" if pd.isna(row.get(f"{status_col}_old", "")) else row.get(f"{status_col}_old", "")
        new_status = "" if pd.isna(row.get(f"{status_col}_new", "")) else row.get(f"{status_col}_new", "")
        change_type = row["_merge"]
        
        # Get the current status
        current_status = ""
        if change_type == "both":
            current_status = new_status
            # Only record a change if the status actually changed
            if old_status == new_status:
                # Skip recording changes where status didn't change
                # This helps reduce false positives
                continue
        elif change_type == "right_only":
            current_status = new_status
        elif change_type == "left_only":
            # For items that disappeared, mark them as "SHP" (shipped) only if they were ASH before
            if old_status == "ASH":
                current_status = "SHP"
            else:
                current_status = old_status
        
        # Skip if this serial has already reached SHP status
        # This helps prevent duplicate entries for serials that have already been shipped
        if serial in processed_serials:
            continue
        
        # If the serial has reached SHP status, mark it as processed
        if current_status == "SHP":
            processed_serials.add(serial)
        
        change = {
            "serial": serial,
            "from": old_status,
            "to": current_status,
            "change_type": change_type,
            "timestamp": datetime.now(timezone.utc).isoformat()  # Add timestamp to each change
        }
        
        # Add additional fields from the newer file (if available)
        for col in additional_cols:
            new_col = f"{col}_new" if f"{col}_new" in row.index else None
            old_col = f"{col}_old" if f"{col}_old" in row.index else None
            
            # Try to get value from new file first, then old file if not available
            value = ""
            if new_col and not pd.isna(row.get(new_col, "")):
                value = row[new_col]
            elif old_col and not pd.isna(row.get(old_col, "")):
                value = row[old_col]
            
            change[col] = value
        
        changes.append(change)
    return changes

def calculate_changes_statistics(changes):
    """Calculate summary statistics from the changes data"""
    # Count by status
    status_counts = {}
    for change in changes:
        status = change.get('to', '')
        if status:
            status_counts[status] = status_counts.get(status, 0) + 1
    
    # Count by customer
    customer_stats = {}
    for change in changes:
        customer = change.get('customer_name', 'Unknown')
        if customer not in customer_stats:
            customer_stats[customer] = {
                'serial_count': 0,
                'change_count': 0
            }
        customer_stats[customer]['serial_count'] += 1
        customer_stats[customer]['change_count'] += 1
    
    # Count by delivery
    delivery_stats = {}
    for change in changes:
        delivery = change.get('delivery', 'Unknown')
        if delivery not in delivery_stats:
            delivery_stats[delivery] = {
                'serial_count': 0,
                'customer': change.get('customer_name', 'Unknown')
            }
        delivery_stats[delivery]['serial_count'] += 1
    
    # Count by shipment
    shipment_stats = {}
    for change in changes:
        shipment = change.get('shipment_number', '')
        if shipment:
            if shipment not in shipment_stats:
                shipment_stats[shipment] = {
                    'serial_count': 0,
                    'customer': change.get('customer_name', 'Unknown')
                }
            shipment_stats[shipment]['serial_count'] += 1
    
    # Count by user
    user_stats = {}
    for change in changes:
        user = change.get('created_by', 'Unknown')
        if user not in user_stats:
            user_stats[user] = {
                'serial_count': 0,
                'change_count': 0
            }
        user_stats[user]['serial_count'] += 1
        user_stats[user]['change_count'] += 1
    
    # Count by change type
    change_type_counts = {}
    for change in changes:
        change_type = change.get('change_type', 'Unknown')
        change_type_counts[change_type] = change_type_counts.get(change_type, 0) + 1
    
    return {
        'status_distribution': status_counts,
        'customer_stats': customer_stats,
        'delivery_stats': delivery_stats,
        'shipment_stats': shipment_stats,
        'user_stats': user_stats,
        'change_type_counts': change_type_counts,
        'total_customers': len(customer_stats),
        'total_deliveries': len(delivery_stats),
        'total_shipments': len(shipment_stats),
        'total_users': len(user_stats)
    }

def calculate_full_dataset_statistics(excel_path):
    """Calculate summary statistics from the entire dataset"""
    print(f"Calculating full dataset statistics from: {excel_path}")
    
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
    
    # Total number of rows
    total_rows = len(df)
    
    # Count by status
    status_col = "status"
    if status_col in df.columns:
        status_counts = df[status_col].value_counts().to_dict()
    else:
        status_counts = {}
    
    # Count by customer
    customer_col = "customer_name"
    customer_stats = {}
    if customer_col in df.columns:
        for customer, group in df.groupby(customer_col):
            if not customer:
                customer = "Unknown"
            customer_stats[customer] = {
                'serial_count': len(group),
                'total_count': len(group)
            }
    
    # Count by delivery
    delivery_col = "delivery"
    delivery_stats = {}
    if delivery_col in df.columns and customer_col in df.columns:
        for delivery, group in df.groupby(delivery_col):
            if not delivery:
                delivery = "Unknown"
            # Get the most common customer for this delivery
            customer = "Unknown"
            if len(group) > 0 and customer_col in group.columns:
                customer_counts = group[customer_col].value_counts()
                if len(customer_counts) > 0:
                    customer = customer_counts.index[0]
                    if not customer:
                        customer = "Unknown"
            
            delivery_stats[delivery] = {
                'serial_count': len(group),
                'customer': customer
            }
    
    # Count by shipment
    shipment_col = "shipment_number"
    shipment_stats = {}
    if shipment_col in df.columns and customer_col in df.columns:
        # Filter out empty shipment numbers
        shipment_df = df[df[shipment_col] != ""]
        for shipment, group in shipment_df.groupby(shipment_col):
            if not shipment:
                continue
            
            # Get the most common customer for this shipment
            customer = "Unknown"
            if len(group) > 0 and customer_col in group.columns:
                customer_counts = group[customer_col].value_counts()
                if len(customer_counts) > 0:
                    customer = customer_counts.index[0]
                    if not customer:
                        customer = "Unknown"
            
            shipment_stats[shipment] = {
                'serial_count': len(group),
                'customer': customer
            }
    
    # Count by user
    user_col = "created_by"
    user_stats = {}
    if user_col in df.columns:
        for user, group in df.groupby(user_col):
            if not user:
                user = "Unknown"
            user_stats[user] = {
                'serial_count': len(group),
                'total_count': len(group)
            }
    
    return {
        'total_rows': total_rows,
        'status_distribution': status_counts,
        'customer_stats': customer_stats,
        'delivery_stats': delivery_stats,
        'shipment_stats': shipment_stats,
        'user_stats': user_stats,
        'total_customers': len(customer_stats),
        'total_deliveries': len(delivery_stats),
        'total_shipments': len(shipment_stats),
        'total_users': len(user_stats)
    }

def process_directory(directory, output_json_path):
    print(f"Processing directory: {directory}")
    if not os.path.exists(directory):
        print(f"Error: Directory '{directory}' does not exist")
        return
        
    files = os.listdir(directory)
    print(f"Found {len(files)} files in directory")
    
    data = {}
    categorized = get_sorted_files_by_type(directory)
    print(f"Categorized files: {categorized}")

    all_changes = []
    
    for file_type, entries in categorized.items():
        if len(entries) < 2:
            print(f"Skipping {file_type} - need at least 2 files to compare")
            continue
        newest_file = os.path.join(directory, entries[0][1])
        previous_file = os.path.join(directory, entries[1][1])
        
        print(f"\nComparing files for type '{file_type}':")
        print(f"  Latest: {entries[0][1]}")
        print(f"  Previous: {entries[1][1]}")

        try:
            changes = compare_excel_files(previous_file, newest_file)
            all_changes.extend(changes)
            
            # Calculate statistics for changes in this file type
            changes_stats = calculate_changes_statistics(changes)
            
            # Calculate statistics for the entire dataset in the newest file
            full_stats = calculate_full_dataset_statistics(newest_file)
            
            data[file_type] = {
                "latest": entries[0][1],
                "previous": entries[1][1],
                "change_count": len(changes),
                "changes": changes,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "changes_statistics": changes_stats,
                "full_statistics": full_stats
            }
            print(f"Found {len(changes)} changes for {file_type}")
            print(f"Full dataset has {full_stats['total_rows']} rows")
        except Exception as e:
            print(f"Error processing {file_type}: {str(e)}")

    # Calculate overall statistics from all changes
    if all_changes:
        overall_changes_stats = calculate_changes_statistics(all_changes)
        
        # Get the full statistics from the latest file of the first file type
        overall_full_stats = {}
        if categorized and len(next(iter(categorized.values()))) > 0:
            first_file_type = next(iter(categorized.keys()))
            newest_file = os.path.join(directory, categorized[first_file_type][0][1])
            try:
                overall_full_stats = calculate_full_dataset_statistics(newest_file)
            except Exception as e:
                print(f"Error calculating full statistics: {str(e)}")
        
        # Add overall statistics to the data
        data["summary"] = {
            "total_changes": len(all_changes),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "changes_statistics": overall_changes_stats,
            "full_statistics": overall_full_stats
        }

        # Add a current_status section to the output.json
        # This will contain the current status of all serials
        current_status = {}
        for change in all_changes:
            serial = change.get('serial', '')
            if serial:
                # If this serial is not in current_status or has a newer timestamp, update it
                if serial not in current_status or change.get('timestamp', '') > current_status[serial].get('timestamp', ''):
                    current_status[serial] = {
                        'status': change.get('to', ''),
                        'delivery': change.get('delivery', ''),
                        'customer_name': change.get('customer_name', ''),
                        'shipment_number': change.get('shipment_number', ''),
                        'created_by': change.get('created_by', ''),
                        'timestamp': change.get('timestamp', '')
                    }
        
        # Add current_status to the data
        data["current_status"] = current_status

    if data:
        # Implement memory optimization by processing data in chunks
        # For large datasets, we'll write the output in a streaming manner
        try:
            # First write the basic structure without the changes array
            with open(output_json_path, "w") as f:
                # Create a copy of the data without the changes arrays
                streamable_data = {}
                for key, value in data.items():
                    if key != "current_status" and isinstance(value, dict) and "changes" in value:
                        streamable_data[key] = {k: v for k, v in value.items() if k != "changes"}
                    else:
                        streamable_data[key] = value
                
                json.dump(streamable_data, f, indent=2)
            
            print(f"[✓] JSON base structure saved to: {output_json_path}")
            print(f"Total changes: {len(all_changes)}")
        except Exception as e:
            print(f"Error writing JSON: {str(e)}")
    else:
        print("No data to save - no valid comparisons were made")

# Example usage
if __name__ == "__main__":
    consume_dir = config.INPUT_DIR
    output_json = config.OUTPUT_JSON
    process_directory(consume_dir, output_json)
