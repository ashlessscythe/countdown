import threading
import time
from datetime import datetime
import config
import os
import subprocess

# Flag to control the background thread
keep_running = True

# Flag to track if a process is currently running
process_running = False

def run_scheduled_process():
    """Run the snapshot processing in a background thread"""
    global keep_running, process_running
    print(f"Starting scheduled processing thread with interval of {config.INTERVAL_SECONDS} seconds")
    
    while keep_running:
        try:
            # Only run if no process is currently running
            if not process_running:
                process_running = True
                print(f"Running snapshot processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Run the compare_snapshots.py script as a subprocess
                subprocess.run(["python", "compare_snapshots.py"], check=True)
                
                print(f"Snapshot processing completed successfully")
                process_running = False
            else:
                print(f"Skipping scheduled run as a process is already running")
        except Exception as e:
            print(f"Error during snapshot processing: {str(e)}")
            process_running = False
        
        # Wait for the next interval
        time.sleep(config.INTERVAL_SECONDS)

def start_background_thread():
    """Start the background thread for scheduled processing"""
    process_thread = threading.Thread(target=run_scheduled_process, daemon=True)
    process_thread.start()
    return process_thread

def stop_background_thread():
    """Stop the background thread"""
    global keep_running
    keep_running = False
