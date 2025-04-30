import threading
import time
from datetime import datetime
from compare import process_directory
import config

# Flag to control the background thread
keep_running = True

def run_scheduled_compare():
    """Run the comparison process in a background thread"""
    global keep_running
    print(f"Starting scheduled comparison thread with interval of {config.UPDATE_INTERVAL} seconds")
    
    while keep_running:
        try:
            print(f"Running comparison at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            process_directory(config.INPUT_DIR, config.OUTPUT_JSON)
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
