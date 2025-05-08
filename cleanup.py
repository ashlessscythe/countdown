"""
Cleanup Script for Shipment Tracking Tool

This script removes all generated files and directories, resetting the environment
to its initial state. It does not remove the configuration file.
"""

import os
import shutil
import logging
from pathlib import Path
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def remove_directory(directory):
    """Remove a directory and all its contents if it exists."""
    path = Path(directory)
    if path.exists():
        try:
            shutil.rmtree(path)
            logging.info(f"Removed directory: {path}")
            return True
        except Exception as e:
            logging.error(f"Failed to remove directory {path}: {e}")
            return False
    return True  # Directory doesn't exist, so no action needed

def remove_file(file_path):
    """Remove a file if it exists."""
    path = Path(file_path)
    if path.exists():
        try:
            path.unlink()
            logging.info(f"Removed file: {path}")
            return True
        except Exception as e:
            logging.error(f"Failed to remove file {path}: {e}")
            return False
    return True  # File doesn't exist, so no action needed

def remove_files_by_pattern(directory, pattern):
    """Remove all files matching a pattern in a directory."""
    path = Path(directory)
    if not path.exists():
        return True
    
    try:
        files = list(path.glob(pattern))
        for file in files:
            file.unlink()
            logging.info(f"Removed file: {file}")
        return True
    except Exception as e:
        logging.error(f"Failed to remove files matching {pattern} in {directory}: {e}")
        return False

def main():
    """Run the cleanup process."""
    logging.info("Starting cleanup process...")
    
    # Ask for confirmation
    print("\nWARNING: This will remove all generated files and directories.")
    print("The following will be removed:")
    print("- All Excel files in the configured directories")
    print("- All Parquet output files")
    print("- All log files")
    print("- The 'visualizations' directory")
    print("\nThe configuration file (config.py) will NOT be removed.")
    
    confirm = input("\nDo you want to continue? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Cleanup aborted.")
        return
    
    # Try to import config to get directory paths
    try:
        import config
        
        # Remove files in the configured directories
        remove_files_by_pattern(config.SERIAL_NUMBERS_DIR, "*.xlsx")
        remove_files_by_pattern(config.DELIVERY_INFO_DIR, "*.xlsx")
        remove_files_by_pattern(config.OUT_DIR, "*.parquet")
        
    except ImportError:
        logging.warning("Could not import config.py. Skipping configured directories.")
    
    # Remove log files
    remove_file("shipment_tracker.log")
    remove_file("test_shipment_tracker.log")
    remove_file("workflow.log")
    
    # Remove visualizations directory
    remove_directory("visualizations")
    
    logging.info("Cleanup completed.")
    print("\nCleanup completed. The environment has been reset to its initial state.")

if __name__ == "__main__":
    main()
