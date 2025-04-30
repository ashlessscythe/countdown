"""
Scheduled execution of the compare.py script.
Runs the comparison process at intervals defined in config.py.
"""
import time
import logging
import sys
import os
from datetime import datetime
import config
from compare import process_directory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scheduled_compare.log'))
    ]
)
logger = logging.getLogger(__name__)

def run_comparison():
    """Run the comparison process and log the results"""
    try:
        logger.info(f"Starting comparison process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        process_directory(config.INPUT_DIR, config.OUTPUT_JSON)
        logger.info(f"Comparison completed successfully")
    except Exception as e:
        logger.error(f"Error during comparison process: {str(e)}")

def scheduled_execution():
    """Run the comparison process at scheduled intervals"""
    logger.info(f"Starting scheduled execution with interval of {config.UPDATE_INTERVAL} seconds")
    
    try:
        # Run immediately on startup
        run_comparison()
        
        # Then run at specified intervals
        while True:
            sleep_time = config.UPDATE_INTERVAL
            logger.info(f"Waiting {config.UPDATE_INTERVAL} seconds until next execution")
            time.sleep(sleep_time)
            run_comparison()
    except KeyboardInterrupt:
        logger.info("Scheduled execution stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error in scheduled execution: {str(e)}")

if __name__ == "__main__":
    scheduled_execution()
