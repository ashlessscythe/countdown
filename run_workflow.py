"""
Workflow Runner Script

This script runs the entire shipment tracking workflow in sequence:
1. Generates sample data
2. Processes the data once
3. Creates visualizations

This is useful for testing the entire system in one go.
"""

import os
import time
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler('workflow.log')  # Log to file
    ]
)

def run_script(script_name, description):
    """Run a Python script and log the result."""
    logging.info(f"Starting: {description}")
    result = os.system(f"python {script_name}")
    
    if result == 0:
        logging.info(f"Completed: {description}")
        return True
    else:
        logging.error(f"Failed: {description} (exit code {result})")
        return False

def check_environment():
    """Check if the environment is properly set up."""
    logging.info("Checking environment...")
    result = os.system("python check_environment.py")
    
    if result != 0:
        logging.error("Environment check failed. Please fix the issues before running the workflow.")
        return False
    
    logging.info("Environment check passed.")
    return True

def main():
    """Run the entire workflow."""
    logging.info("Starting shipment tracking workflow")
    
    # Check environment first
    if not check_environment():
        return
    
    # Step 1: Generate sample data
    if not run_script("generate_sample_data.py", "Generate sample data"):
        logging.error("Workflow aborted: Failed to generate sample data")
        return
    
    # Wait a moment to ensure files are fully written
    time.sleep(2)
    
    # Step 2: Process the data once
    if not run_script("test_shipment_tracker.py", "Process data snapshot"):
        logging.error("Workflow aborted: Failed to process data")
        return
    
    # Wait a moment to ensure output files are fully written
    time.sleep(2)
    
    # Step 3: Generate visualizations
    if not run_script("visualize_results.py", "Generate visualizations"):
        logging.error("Workflow aborted: Failed to generate visualizations")
        return
    
    # Check if visualizations were created
    viz_dir = Path("visualizations")
    if viz_dir.exists() and any(viz_dir.iterdir()):
        logging.info(f"Visualizations created in {viz_dir.absolute()}")
    
    logging.info("Workflow completed successfully")
    print("\nWorkflow completed! You can find:")
    print("- Log file: workflow.log")
    print("- Output data: in the configured output directory")
    print("- Visualizations: in the 'visualizations' directory")

if __name__ == "__main__":
    main()
