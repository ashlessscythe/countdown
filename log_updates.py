"""
Log updates to the master history file.
This script can be used to manually append delta files to the master history.
"""
import os
import json
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

def append_to_master_history(delta_path, master_history_path):
    """Append delta to the master history file"""
    print(f"Appending delta {delta_path} to master history: {master_history_path}")
    
    # Load the delta file
    try:
        with open(delta_path, 'r') as f:
            delta = json.load(f)
    except Exception as e:
        print(f"Error loading delta file: {str(e)}")
        return None
    
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

def generate_current_status(delta_path, current_status_path):
    """Generate a current status file based on the delta file"""
    print(f"Generating current status from delta {delta_path}: {current_status_path}")
    
    # Load the delta file
    try:
        with open(delta_path, 'r') as f:
            delta = json.load(f)
    except Exception as e:
        print(f"Error loading delta file: {str(e)}")
        return None
    
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

def process_delta_directory(changes_dir, master_history_path, current_status_path):
    """Process all delta files in the changes directory and append them to the master history"""
    print(f"Processing delta files in: {changes_dir}")
    
    if not os.path.exists(changes_dir):
        print(f"Error: Directory '{changes_dir}' does not exist")
        return
    
    # Get all delta files
    delta_files = [f for f in os.listdir(changes_dir) if f.endswith('_delta.json')]
    
    if not delta_files:
        print("No delta files found")
        return
    
    # Sort delta files by name (which includes timestamp)
    delta_files.sort()
    
    # Process each delta file
    for delta_file in delta_files:
        delta_path = os.path.join(changes_dir, delta_file)
        append_to_master_history(delta_path, master_history_path)
        generate_current_status(delta_path, current_status_path)

def main():
    """Main function to process command line arguments and run the script"""
    # Set up directory structure
    dirs = setup_directory_structure()
    data_dir = dirs["data_dir"]
    changes_dir = dirs["changes_dir"]
    
    # Define paths for master history and current status
    master_history_path = os.path.join(data_dir, "master_history.json")
    current_status_path = os.path.join(data_dir, "current_status.json")
    
    # Check if a specific delta file was provided
    if len(sys.argv) > 1:
        delta_path = sys.argv[1]
        if not os.path.exists(delta_path):
            print(f"Error: Delta file '{delta_path}' does not exist")
            return
        
        append_to_master_history(delta_path, master_history_path)
        generate_current_status(delta_path, current_status_path)
    else:
        # Process all delta files in the changes directory
        process_delta_directory(changes_dir, master_history_path, current_status_path)

if __name__ == "__main__":
    main()
