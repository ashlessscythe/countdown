"""
Main backend application with FastAPI.
"""
import os
import sys
import logging
import asyncio
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import threading
import time
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from backend.api.routes import router as api_router, broadcast_updates
from backend.data_processing.readers import get_combined_data
from backend.data_processing.transformers import prepare_dashboard_data
from backend.data_processing.watchers import poll_for_new_files
from backend.storage.cache import dashboard_cache
from backend.storage.parquet_manager import save_dashboard_data_to_parquet, diff_dashboard_data
from config import INTERVAL_SECONDS

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Delivery Dashboard API",
    description="API for the Delivery Dashboard application",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins in development
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

# Include API routes
app.include_router(api_router)

# Background task for processing files
def process_files():
    """
    Process the latest ZMDESNR and VL06O files and update the dashboard data.
    """
    logger.info("Processing latest files...")
    
    try:
        # Get the combined data
        serials_df, deliveries_df, combined_df = get_combined_data()
        
        if serials_df.empty or deliveries_df.empty:
            logger.warning("One or both dataframes are empty")
            return
        
        # Prepare dashboard data
        dashboard_data = prepare_dashboard_data(combined_df)
        
        # Generate timestamp for file naming
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Get previous dashboard data from cache
        previous_data = dashboard_cache.get('previous_dashboard_data', {})
        
        # Calculate diff with previous data
        if previous_data:
            diff = diff_dashboard_data(dashboard_data, previous_data)
            dashboard_cache.set('latest_diff', diff)
        
        # Update the cache with the new dashboard data
        dashboard_cache.set_dashboard_data(dashboard_data)
        dashboard_cache.set('previous_dashboard_data', dashboard_data)
        dashboard_cache.set('last_update_time', datetime.now().isoformat())
        
        # Save the dashboard data to Parquet files
        save_dashboard_data_to_parquet(dashboard_data, timestamp)
        
        logger.info("Dashboard data updated successfully")
    
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")

# File change callback
def file_change_callback(file_type, file_path):
    """
    Handle updates when new files are detected.
    
    Args:
        file_type (str): Type of file ('zmdesnr' or 'vl06o')
        file_path (str): Path to the new file
    """
    logger.info(f"New {file_type.upper()} file detected: {file_path}")
    
    # Process the files and update the dashboard data
    process_files()

# Background task for file watching
def start_file_watcher():
    """
    Start watching for file changes in a background thread.
    """
    logger.info("Starting file watcher...")
    poll_for_new_files(file_change_callback, INTERVAL_SECONDS)

# Startup event
@app.on_event("startup")
async def startup_event():
    """
    Run startup tasks when the application starts.
    """
    logger.info("Starting Delivery Dashboard API...")
    
    # Process files initially
    process_files()
    
    # Start file watcher in a background thread
    watcher_thread = threading.Thread(target=start_file_watcher, daemon=True)
    watcher_thread.start()
    
    # Start the WebSocket broadcast task
    asyncio.create_task(broadcast_updates())

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """
    Run cleanup tasks when the application shuts down.
    """
    logger.info("Shutting down Delivery Dashboard API...")

# Root endpoint
@app.get("/")
async def root():
    """
    Root endpoint that returns basic API information.
    """
    return {
        "message": "Delivery Dashboard API",
        "version": "1.0.0",
        "status": "running",
        "docs_url": "/docs"
    }

# Health check endpoint
@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    """
    # Get the last update time from cache
    last_update_time = dashboard_cache.get('last_update_time')
    
    # Check if dashboard data exists
    dashboard_data = dashboard_cache.get_dashboard_data()
    has_data = bool(dashboard_data)
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "last_update": last_update_time,
        "has_data": has_data
    }

# Error handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """
    Global exception handler.
    """
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error", "detail": str(exc)}
    )

if __name__ == "__main__":
    # Run the application with uvicorn
    # Use a different default port (8001) when run directly to avoid conflicts with main.py
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the Delivery Dashboard API")
    parser.add_argument("--port", type=int, default=8001, 
                        help="Port to run the API on (default: 8001)")
    args = parser.parse_args()
    
    print(f"Starting API server on port {args.port}...")
    uvicorn.run("backend.app:app", host="0.0.0.0", port=args.port, reload=True)
