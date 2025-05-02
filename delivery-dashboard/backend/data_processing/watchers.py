"""
File watching mechanism for new files in the ZMDESNR and VL06O directories.
"""
import os
import time
import glob
from datetime import datetime
import sys
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config import SERIAL_NUMBERS_DIR, DELIVERY_INFO_DIR, INTERVAL_SECONDS

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ExcelFileHandler(FileSystemEventHandler):
    """
    Handler for Excel file events in the watched directories.
    """
    def __init__(self, callback=None):
        """
        Initialize the handler with an optional callback function.
        
        Args:
            callback (callable, optional): Function to call when a new file is detected
        """
        self.callback = callback
        self.last_processed_files = {
            'zmdesnr': None,
            'vl06o': None
        }
    
    def on_created(self, event):
        """
        Handle file creation events.
        
        Args:
            event: File system event
        """
        if not event.is_directory and event.src_path.lower().endswith('.xlsx'):
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            
            logger.info(f"New file detected: {file_path}")
            
            # Determine file type
            if 'ZMDESNR' in file_name.upper():
                file_type = 'zmdesnr'
            elif 'VL06O' in file_name.upper():
                file_type = 'vl06o'
            else:
                logger.info(f"Ignoring file of unknown type: {file_name}")
                return
            
            # Update last processed file
            self.last_processed_files[file_type] = file_path
            
            # Call the callback if provided
            if self.callback:
                self.callback(file_type, file_path)

def get_latest_files():
    """
    Get the latest files from both directories.
    
    Returns:
        dict: Dictionary with the latest file paths
    """
    # Get the latest ZMDESNR file
    zmdesnr_files = glob.glob(os.path.join(SERIAL_NUMBERS_DIR, "*ZMDESNR*.xlsx"))
    latest_zmdesnr = max(zmdesnr_files, key=os.path.getmtime) if zmdesnr_files else None
    
    # Get the latest VL06O file
    vl06o_files = glob.glob(os.path.join(DELIVERY_INFO_DIR, "*VL06O*.xlsx"))
    latest_vl06o = max(vl06o_files, key=os.path.getmtime) if vl06o_files else None
    
    return {
        'zmdesnr': latest_zmdesnr,
        'vl06o': latest_vl06o
    }

def start_file_watcher(callback=None):
    """
    Start watching for new files in the ZMDESNR and VL06O directories.
    
    Args:
        callback (callable, optional): Function to call when a new file is detected
        
    Returns:
        tuple: (observer, handler) - The watchdog observer and handler
    """
    # Create event handler
    handler = ExcelFileHandler(callback)
    
    # Create observer
    observer = Observer()
    
    # Schedule watching both directories
    observer.schedule(handler, SERIAL_NUMBERS_DIR, recursive=False)
    observer.schedule(handler, DELIVERY_INFO_DIR, recursive=False)
    
    # Start the observer
    observer.start()
    logger.info(f"Started watching directories: {SERIAL_NUMBERS_DIR}, {DELIVERY_INFO_DIR}")
    
    return observer, handler

def poll_for_new_files(callback=None, interval=INTERVAL_SECONDS):
    """
    Poll for new files at regular intervals.
    This is an alternative to using watchdog for systems where it might not work well.
    
    Args:
        callback (callable, optional): Function to call when a new file is detected
        interval (int): Polling interval in seconds
    """
    last_processed = {
        'zmdesnr': None,
        'vl06o': None
    }
    
    logger.info(f"Started polling for new files every {interval} seconds")
    
    try:
        while True:
            # Get the latest files
            latest_files = get_latest_files()
            
            # Check for new ZMDESNR file
            if latest_files['zmdesnr'] and latest_files['zmdesnr'] != last_processed['zmdesnr']:
                logger.info(f"New ZMDESNR file detected: {latest_files['zmdesnr']}")
                last_processed['zmdesnr'] = latest_files['zmdesnr']
                if callback:
                    callback('zmdesnr', latest_files['zmdesnr'])
            
            # Check for new VL06O file
            if latest_files['vl06o'] and latest_files['vl06o'] != last_processed['vl06o']:
                logger.info(f"New VL06O file detected: {latest_files['vl06o']}")
                last_processed['vl06o'] = latest_files['vl06o']
                if callback:
                    callback('vl06o', latest_files['vl06o'])
            
            # Sleep for the specified interval
            time.sleep(interval)
    
    except KeyboardInterrupt:
        logger.info("Stopped polling for new files")

def file_change_callback(file_type, file_path):
    """
    Example callback function for file changes.
    
    Args:
        file_type (str): Type of file ('zmdesnr' or 'vl06o')
        file_path (str): Path to the new file
    """
    logger.info(f"Processing new {file_type.upper()} file: {file_path}")
    # Here you would typically:
    # 1. Read the new file
    # 2. Process the data
    # 3. Update any caches or storage
    # 4. Trigger updates to the frontend

if __name__ == "__main__":
    # Example usage
    try:
        # Option 1: Use watchdog observer
        # observer, handler = start_file_watcher(file_change_callback)
        # time.sleep(3600)  # Run for an hour
        # observer.stop()
        # observer.join()
        
        # Option 2: Use polling
        poll_for_new_files(file_change_callback)
    
    except KeyboardInterrupt:
        logger.info("File watching stopped by user")
