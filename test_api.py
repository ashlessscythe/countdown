"""
Test script for the Delivery Dashboard API.
"""
import os
import sys
import json
import logging
import requests
import time
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API base URL
API_BASE_URL = "http://localhost:8000/api"

def test_dashboard_endpoint():
    """
    Test the dashboard endpoint.
    """
    logger.info("Testing dashboard endpoint...")
    
    try:
        response = requests.get(f"{API_BASE_URL}/dashboard")
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Dashboard data received: {len(data)} sections")
        
        # Print some stats
        if 'users' in data:
            logger.info(f"Users: {len(data['users'])}")
        if 'deliveries' in data:
            logger.info(f"Deliveries: {len(data['deliveries'])}")
        if 'progress' in data:
            logger.info(f"Progress entries: {len(data['progress'])}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing dashboard endpoint: {str(e)}")
        return False

def test_users_endpoint():
    """
    Test the users endpoint.
    """
    logger.info("Testing users endpoint...")
    
    try:
        # Test with active_only=False
        response = requests.get(f"{API_BASE_URL}/users", params={"active_only": False})
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"All users: {len(data)}")
        
        # Test with active_only=True
        response = requests.get(f"{API_BASE_URL}/users", params={"active_only": True})
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Active users: {len(data)}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing users endpoint: {str(e)}")
        return False

def test_progress_endpoint():
    """
    Test the progress endpoint.
    """
    logger.info("Testing progress endpoint...")
    
    try:
        # Test without filters
        response = requests.get(f"{API_BASE_URL}/progress")
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"All progress entries: {len(data)}")
        
        # If we have progress data, test with filters
        if data:
            # Get the first delivery ID
            delivery_id = str(data[0].get('delivery', ''))
            
            # Test with delivery_id filter
            response = requests.get(f"{API_BASE_URL}/progress", params={"delivery_id": delivery_id})
            response.raise_for_status()
            
            filtered_data = response.json()
            logger.info(f"Progress entries for delivery {delivery_id}: {len(filtered_data)}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing progress endpoint: {str(e)}")
        return False

def test_scan_times_endpoint():
    """
    Test the scan times endpoint.
    """
    logger.info("Testing scan times endpoint...")
    
    try:
        # Test without filters
        response = requests.get(f"{API_BASE_URL}/scan-times")
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"All scan time entries: {len(data)}")
        
        # If we have scan time data, test with filters
        if data:
            # Get the first user ID
            user_id = str(data[0].get('user_id', ''))
            
            # Test with user_id filter
            response = requests.get(f"{API_BASE_URL}/scan-times", params={"user_id": user_id})
            response.raise_for_status()
            
            filtered_data = response.json()
            logger.info(f"Scan time entries for user {user_id}: {len(filtered_data)}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing scan times endpoint: {str(e)}")
        return False

def test_track_activity_endpoint():
    """
    Test the track activity endpoint.
    """
    logger.info("Testing track activity endpoint...")
    
    try:
        # Create a test user ID
        user_id = f"test_user_{int(time.time())}"
        
        # Track a view activity
        response = requests.post(f"{API_BASE_URL}/track-activity", params={
            "user_id": user_id,
            "activity_type": "view"
        })
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Track activity response: {data}")
        
        # Track a scan activity
        response = requests.post(f"{API_BASE_URL}/track-activity", params={
            "user_id": user_id,
            "activity_type": "scan"
        })
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Track activity response: {data}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing track activity endpoint: {str(e)}")
        return False

def run_all_tests():
    """
    Run all API tests.
    """
    logger.info("Starting API tests...")
    
    # Wait for the API server to start
    logger.info("Waiting for API server to start...")
    time.sleep(2)
    
    # Run tests
    tests = [
        test_dashboard_endpoint,
        test_users_endpoint,
        test_progress_endpoint,
        test_scan_times_endpoint,
        test_track_activity_endpoint
    ]
    
    results = []
    for test in tests:
        result = test()
        results.append(result)
    
    # Print summary
    logger.info("Test results:")
    for i, test in enumerate(tests):
        logger.info(f"{test.__name__}: {'PASS' if results[i] else 'FAIL'}")
    
    # Return overall result
    return all(results)

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
