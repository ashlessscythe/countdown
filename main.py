"""
Main entry point for the Delivery Dashboard application.
"""
import os
import sys
import logging
import uvicorn
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_dashboard():
    """
    Run the dashboard application with FastAPI backend.
    """
    logger.info("Starting Delivery Dashboard application...")
    
    # Import here to avoid circular imports
    from backend.app import app
    
    # Run the FastAPI application
    uvicorn.run(app, host="0.0.0.0", port=8000)

def test_data_processing():
    """
    Test the data processing modules without starting the API server.
    """
    logger.info("Testing data processing modules...")
    
    # Import data processing modules
    from backend.data_processing.readers import get_combined_data
    from backend.data_processing.transformers import prepare_dashboard_data
    from backend.storage.parquet_manager import save_dashboard_data_to_parquet
    from backend.storage.cache import dashboard_cache
    
    # Get the combined data
    serials_df, deliveries_df, combined_df = get_combined_data()
    
    # Print some stats
    logger.info(f"ZMDESNR file: {len(serials_df)} rows")
    logger.info(f"VL06O file: {len(deliveries_df)} rows")
    logger.info(f"Combined data: {len(combined_df)} rows")
    
    # Prepare dashboard data
    dashboard_data = prepare_dashboard_data(combined_df)
    
    # Print some stats
    num_users = len(dashboard_data.get('users', []))
    num_deliveries = len(dashboard_data.get('deliveries', []))
    logger.info(f"Dashboard data: {num_users} users, {num_deliveries} deliveries")
    
    # Generate timestamp for file naming
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Save the dashboard data to Parquet files
    parquet_paths = save_dashboard_data_to_parquet(dashboard_data, timestamp)
    if parquet_paths:
        logger.info(f"Test data saved to Parquet files: {', '.join(parquet_paths.values())}")
    
    # Update the cache with the test data
    dashboard_cache.set_dashboard_data(dashboard_data)
    dashboard_cache.set('test_mode', True)
    dashboard_cache.set('test_timestamp', timestamp)
    logger.info("Test data cached for quick access")
    
    return dashboard_data

if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test":
            # Run in test mode
            test_data_processing()
        elif sys.argv[1] == "--api-only":
            # Run only the API server without file processing
            from backend.app import app
            uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        # Run the dashboard application
        run_dashboard()
