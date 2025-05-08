"""
Test Script for Shipment Tracker

This script runs a single processing cycle of the shipment tracker
without the continuous loop. It's useful for testing and debugging.
"""

import logging
from shipment_tracker import process_snapshot

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Log to console
            logging.FileHandler('test_shipment_tracker.log')  # Log to file
        ]
    )
    
    logging.info("Starting test run of shipment tracker...")
    
    # Run a single processing cycle
    try:
        process_snapshot()
        logging.info("Test run completed successfully")
    except Exception as e:
        logging.exception("Error during test run")
        
    logging.info("Test run finished")
