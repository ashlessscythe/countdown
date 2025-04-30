import threading
import time
from datetime import datetime
from compare_snapshots import compare_snapshots
import config
import os

# Flag to control the background thread
keep_running = True

def run_scheduled_compare():
    """Run the comparison process in a background thread"""
    global keep_running
    print(f"Starting scheduled comparison thread with interval of {config.UPDATE_INTERVAL} seconds")
    
    while keep_running:
        try:
            print(f"Running comparison at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Get all Excel files in the input directory defined in config
            input_dir = config.INPUT_DIR
            excel_files = [f for f in os.listdir(input_dir) if f.endswith('.xlsx')]
            
            if len(excel_files) < 2:
                print("Need at least two Excel files for comparison")
                continue
            
            # Sort Excel files by name
            excel_files.sort()
            
            # Compare the first two files
            file1 = os.path.join(input_dir, excel_files[0])
            file2 = os.path.join(input_dir, excel_files[1])
            
            print(f"Comparing files: {file1} and {file2}")
            results = compare_snapshots(file1, file2)
            
            # Save the results to a JSON file
            with open('comparison_results.json', 'w') as f:
                import json
                json.dump(results, f, indent=2)
            
            print(f"Comparison completed successfully")
        except Exception as e:
            print(f"Error during comparison process: {str(e)}")
        
        # Wait for the next interval
        time.sleep(config.UPDATE_INTERVAL)

def start_background_thread():
    """Start the background thread for scheduled comparisons"""
    compare_thread = threading.Thread(target=run_scheduled_compare, daemon=True)
    compare_thread.start()
    return compare_thread

def stop_background_thread():
    """Stop the background thread"""
    global keep_running
    keep_running = False
