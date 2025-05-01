"""
Main Module for Compare Snapshots

This module provides the main functionality for the Compare Snapshots package:
- Main loop for continuous processing
- Orchestration of file loading and metrics generation
"""

import os
import time
import pandas as pd
from datetime import datetime
import config
from .io_utils import list_files, filter_by_age, load_snapshot, save_parquet
from .diff_utils import build_metrics


def run_loop():
    """
    Process Excel snapshots once
    
    This function processes Excel snapshots and generates metrics,
    which are saved as Parquet files. It runs once and then returns,
    allowing it to be scheduled by an external mechanism.
    """
    print(f"Starting snapshot processing...")
    print(f"Data directory: {config.DATA_DIR}")
    print(f"Output directory: {config.OUT_DIR}")
    print(f"Window: {config.WINDOW_MINUTES} minutes")
    
    try:
        # Process snapshots
        process_snapshots()
        print("Snapshot processing completed successfully")
    except Exception as e:
        print(f"Error processing snapshots: {str(e)}")
        import traceback
        traceback.print_exc()


def process_snapshots():
    """
    Process Excel snapshots and generate metrics
    
    This function:
    1. Lists all Excel files in the data directory
    2. Filters files by age based on the window
    3. Loads snapshots into DataFrames
    4. Generates metrics
    5. Saves metrics to Parquet files
    """
    print(f"Processing snapshots at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # List all Excel files in the data directory
    all_files = list_files(config.DATA_DIR)
    
    if not all_files:
        print("No Excel files found in data directory")
        return
    
    print(f"Found {len(all_files)} Excel files")
    
    # Get the newest file
    newest_file = all_files[-1]
    print(f"Newest file: {os.path.basename(newest_file)}")
    
    # Filter files by age based on the window
    window_files = filter_by_age(all_files, config.WINDOW_MINUTES)
    print(f"Files in {config.WINDOW_MINUTES}-minute window: {len(window_files)}")
    
    if not window_files:
        print("No files in window, using newest file regardless of age")
        window_files = [newest_file]
    
    # Load snapshots
    print("Loading snapshots...")
    dfs = []
    for file_path in window_files:
        try:
            df = load_snapshot(file_path)
            dfs.append(df)
            print(f"Loaded {os.path.basename(file_path)}: {len(df)} rows")
        except Exception as e:
            print(f"Error loading {os.path.basename(file_path)}: {str(e)}")
    
    if not dfs:
        print("No snapshots loaded")
        return
    
    # Concatenate all snapshots in the window
    df_window = pd.concat(dfs, ignore_index=True)
    print(f"Combined window DataFrame: {len(df_window)} rows")
    
    # Get the latest snapshot
    df_latest = dfs[-1]
    print(f"Latest snapshot: {len(df_latest)} rows")
    
    # Build metrics
    print("Building metrics...")
    metrics = build_metrics(df_latest, df_window)
    
    # Save metrics to Parquet files
    print("Saving metrics to Parquet files...")
    save_parquet(metrics, config.OUT_DIR)
    print(f"Metrics saved to {config.OUT_DIR}")
    
    # Print summary
    print("\nSummary:")
    print(f"Status counts:")
    print(metrics['status_summary'])
    print(f"\nUser activity:")
    print(metrics['user_activity'])


if __name__ == "__main__":
    run_loop()
