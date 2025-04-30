import os
import pandas as pd
import json
import re
from datetime import datetime, timezone
import config
import shutil
import time

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

def get_sorted_files_by_type(directory):
    """Get files sorted by timestamp and categorized by type"""
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

def get_latest_snapshots(snapshots_dir, count=2):
    """Get the latest snapshot files from the snapshots directory"""
    if not os.path.exists(snapshots_dir):
        return []
    
    files = [f for f in os.listdir(snapshots_dir) if f.endswith('.json')]
    
    # Sort files by modification time (newest first)
    files.sort(key=lambda f: os.path.getmtime(os.path.join(snapshots_dir, f)), reverse=True)
    
    # Return the latest 'count' files
    return [os.path.join(snapshots_dir, f) for f in files[:count]]

def compare_snapshots(snapshot1_path, snapshot2_path, output_dir):
    """Compare two snapshots and save the delta to the changes directory"""
    print(f"Comparing snapshots:\n  - {snapshot1_path}\n  - {snapshot2_path}")
    
    # Load the snapshots
    with open(snapshot1_path, 'r') as f:
        snapshot1 = json.load(f)
    
    with open(snapshot2_path, 'r') as f:
        snapshot2 = json.load(f)
    
    # Extract records
    records1 = {record["serial"]: record for record in snapshot1["records"] if "serial" in record}
    records2 = {record["serial"]: record for record in snapshot2["records"] if "serial" in record}
    
    # Find serials in each snapshot
    serials1 = set(records1.keys())
    serials2 = set(records2.keys())
    
    # Calculate differences
    added_serials = serials2 - serials1
    removed_serials = serials1 - serials2
    common_serials = serials1.intersection(serials2)
    
    # Initialize changes lists
    added = []
    removed = []
    updated = []
    
    # Process added serials
    for serial in added_serials:
        added.append({
            "serial": serial,
            "record": records2[serial],
            "change_type": "added"
        })
    
    # Process removed serials
    for serial in removed_serials:
        removed.append({
            "serial": serial,
            "record": records1[serial],
            "change_type": "removed"
        })
    
    # Process updated serials
    for serial in common_serials:
        record1 = records1[serial]
        record2 = records2[serial]
        
        # Check if any fields have changed
        changes = {}
        for key in set(record1.keys()).union(record2.keys()):
            if key in record1 and key in record2 and record1[key] != record2[key]:
                changes[key] = {
                    "from": record1[key],
                    "to": record2[key]
                }
        
        if changes:
            updated.append({
                "serial": serial,
                "changes": changes,
                "record": record2,
                "change_type": "updated"
            })
    
    # Create delta object
    delta = {
        "metadata": {
            "snapshot1": os.path.basename(snapshot1_path),
            "snapshot2": os.path.basename(snapshot2_path),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "added_count": len(added),
            "removed_count": len(removed),
            "updated_count": len(updated)
        },
        "added": added,
        "removed": removed,
        "updated": updated
    }
    
    # Generate a filename with timestamp
    snapshot2_basename = os.path.basename(snapshot2_path)
    snapshot2_timestamp = snapshot2_basename.split('.')[0].split('_')[0]  # Extract timestamp part
    filename = f"{snapshot2_timestamp}_delta.json"
    output_path = os.path.join(output_dir, filename)
    
    # Save the delta as JSON
    with open(output_path, 'w') as f:
        json.dump(delta, f, indent=2)
    
    print(f"Delta saved to: {output_path}")
    return delta, output_path

def append_to_master_history(delta, master_history_path):
    """Append delta to the master history file"""
    print(f"Appending to master history: {master_history_path}")
    
    # Initialize history data
    history = {
        "metadata": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "delta_count": 0,
            "total_changes": 0
        },
        "deltas": []
    }
    
    # Load existing history if it exists
    if os.path.exists(master_history_path):
        try:
            with open(master_history_path, 'r') as f:
                history = json.load(f)
        except Exception as e:
            print(f"Error loading master history: {str(e)}")
            # Continue with a new history file
    
    # Add the new delta
    history["deltas"].append(delta)
    
    # Update metadata
    history["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
    history["metadata"]["delta_count"] = len(history["deltas"])
    history["metadata"]["total_changes"] = sum([
        d["metadata"]["added_count"] + 
        d["metadata"]["removed_count"] + 
        d["metadata"]["updated_count"] 
        for d in history["deltas"]
    ])
    
    # Save the updated history
    with open(master_history_path, 'w') as f:
        json.dump(history, f, indent=2)
    
    print(f"Master history updated: {master_history_path}")
    return history

def generate_current_status(delta, current_status_path):
    """Generate a current status file based on the latest delta"""
    print(f"Generating current status: {current_status_path}")
    
    # Initialize current status
    current_status = {
        "metadata": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "record_count": 0
        },
        "records": {}
    }
    
    # Load existing current status if it exists
    if os.path.exists(current_status_path):
        try:
            with open(current_status_path, 'r') as f:
                current_status = json.load(f)
        except Exception as e:
            print(f"Error loading current status: {str(e)}")
            # Continue with a new current status file
    
    # Update with added records
    for item in delta["added"]:
        serial = item["serial"]
        current_status["records"][serial] = item["record"]
    
    # Update with updated records
    for item in delta["updated"]:
        serial = item["serial"]
        current_status["records"][serial] = item["record"]
    
    # Remove removed records
    for item in delta["removed"]:
        serial = item["serial"]
        if serial in current_status["records"]:
            del current_status["records"][serial]
    
    # Update metadata
    current_status["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
    current_status["metadata"]["record_count"] = len(current_status["records"])
    
    # Save the updated current status
    with open(current_status_path, 'w') as f:
        json.dump(current_status, f, indent=2)
    
    print(f"Current status updated: {current_status_path}")
    return current_status

def calculate_statistics(current_status):
    """Calculate statistics from the current status"""
    records = list(current_status["records"].values())
    
    # Count by status
    status_counts = {}
    for record in records:
        status = record.get('status', '').upper()
        if status:
            status_counts[status] = status_counts.get(status, 0) + 1
    
    # Count by customer
    customer_stats = {}
    for record in records:
        customer = record.get('customer_name', 'Unknown')
        if customer not in customer_stats:
            customer_stats[customer] = {
                'serial_count': 0,
                'ash_count': 0,
                'shp_count': 0
            }
        
        customer_stats[customer]['serial_count'] += 1
        
        status = record.get('status', '').upper()
        if status == 'ASH':
            customer_stats[customer]['ash_count'] += 1
        elif status == 'SHP':
            customer_stats[customer]['shp_count'] += 1
    
    # Count by delivery
    delivery_stats = {}
    for record in records:
        delivery = record.get('delivery', 'Unknown')
        if delivery not in delivery_stats:
            delivery_stats[delivery] = {
                'serial_count': 0,
                'customer': record.get('customer_name', 'Unknown'),
                'ash_count': 0,
                'shp_count': 0
            }
        
        delivery_stats[delivery]['serial_count'] += 1
        
        status = record.get('status', '').upper()
        if status == 'ASH':
            delivery_stats[delivery]['ash_count'] += 1
        elif status == 'SHP':
            delivery_stats[delivery]['shp_count'] += 1
    
    # Count by user
    user_stats = {}
    for record in records:
        user = record.get('created_by', 'Unknown')
        if user not in user_stats:
            user_stats[user] = {
                'serial_count': 0,
                'ash_count': 0,
                'shp_count': 0
            }
        
        user_stats[user]['serial_count'] += 1
        
        status = record.get('status', '').upper()
        if status == 'ASH':
            user_stats[user]['ash_count'] += 1
        elif status == 'SHP':
            user_stats[user]['shp_count'] += 1
    
    return {
        'status_distribution': status_counts,
        'customer_stats': customer_stats,
        'delivery_stats': delivery_stats,
        'user_stats': user_stats,
        'total_customers': len(customer_stats),
        'total_deliveries': len(delivery_stats),
        'total_users': len(user_stats),
        'total_records': len(records)
    }

def process_directory(input_dir):
    """Process the input directory and generate snapshots, deltas, and statistics"""
    print(f"Processing directory: {input_dir}")
    if not os.path.exists(input_dir):
        print(f"Error: Directory '{input_dir}' does not exist")
        return
    
    # Set up directory structure
    dirs = setup_directory_structure()
    data_dir = dirs["data_dir"]
    snapshots_dir = dirs["snapshots_dir"]
    changes_dir = dirs["changes_dir"]
    
    # Define paths for master history and current status
    master_history_path = os.path.join(data_dir, "master_history.json")
    current_status_path = os.path.join(data_dir, "current_status.json")
    
    # Get categorized files
    categorized = get_sorted_files_by_type(input_dir)
    print(f"Categorized files: {categorized}")
    
    # Process each file type
    for file_type, entries in categorized.items():
        if not entries:
            print(f"No files found for type '{file_type}'")
            continue
        
        # Get the latest file
        latest_file = os.path.join(input_dir, entries[0][1])
        print(f"\nProcessing latest file for type '{file_type}': {entries[0][1]}")
        
        # Convert to snapshot
        snapshot_path = excel_to_snapshot(latest_file, snapshots_dir)
        
        # Get the latest snapshots
        latest_snapshots = get_latest_snapshots(snapshots_dir, 2)
        
        # If we have at least 2 snapshots, compare them
        if len(latest_snapshots) >= 2:
            print(f"Found {len(latest_snapshots)} snapshots, comparing the latest two")
            # Compare the latest two snapshots
            delta, delta_path = compare_snapshots(latest_snapshots[1], latest_snapshots[0], changes_dir)
            
            # Append to master history
            append_to_master_history(delta, master_history_path)
            
            # Generate current status
            current_status = generate_current_status(delta, current_status_path)
            
            # Calculate statistics
            stats = calculate_statistics(current_status)
            
            # Save statistics to a separate file
            stats_path = os.path.join(data_dir, "statistics.json")
            with open(stats_path, 'w') as f:
                json.dump({
                    "metadata": {
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "source": os.path.basename(current_status_path)
                    },
                    "statistics": stats
                }, f, indent=2)
            
            print(f"Statistics saved to: {stats_path}")
            
            # Also save to the original output.json for backward compatibility
            with open(config.OUTPUT_JSON, 'w') as f:
                json.dump({
                    "metadata": {
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "source": os.path.basename(current_status_path)
                    },
                    "statistics": stats,
                    "current_status": current_status["records"]
                }, f, indent=2)
            
            print(f"Output saved to: {config.OUTPUT_JSON}")
        else:
            print(f"Need at least 2 snapshots to compare. Only created the first snapshot.")
            print(f"Run this script again with a new Excel file to create a second snapshot and generate a delta.")

# Example usage
if __name__ == "__main__":
    process_directory(config.INPUT_DIR)
