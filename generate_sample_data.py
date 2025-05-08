"""
Sample Data Generator for Shipment Tracker

This script generates sample Excel files that mimic the structure of
ZMDESNR and VL06O snapshots for testing the shipment tracker.
"""

import os
import random
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import config

def ensure_directories():
    """Ensure all required directories exist."""
    Path(config.SERIAL_NUMBERS_DIR).mkdir(parents=True, exist_ok=True)
    Path(config.DELIVERY_INFO_DIR).mkdir(parents=True, exist_ok=True)
    Path(config.OUT_DIR).mkdir(parents=True, exist_ok=True)

def generate_serial_data(num_records=100):
    """
    Generate sample serial-level data (ZMDESNR).
    
    Args:
        num_records: Number of records to generate
        
    Returns:
        DataFrame with sample serial data
    """
    # Define possible values
    users = ['USER001', 'USER002', 'USER003', 'USER004', 'USER005']
    statuses = list(config.STATUS_MAPPING.keys())  # 'ASH', 'SHP'
    warehouse = config.WAREHOUSE_FILTER  # 'E01'
    
    # Generate random deliveries (10-digit numbers as strings)
    deliveries = [f"{random.randint(1000000000, 9999999999)}" for _ in range(10)]
    
    # Generate random serial numbers
    serials = [f"SN{random.randint(100000, 999999)}" for _ in range(num_records)]
    
    # Generate random pallet IDs (fewer than serials, as multiple serials can be in one pallet)
    pallets = [f"P{random.randint(10000, 99999)}" for _ in range(num_records // 3)]
    
    # Base timestamp for created dates (within the last week)
    now = datetime.now()
    base_date = now - timedelta(days=7)
    
    # Generate data
    data = []
    
    # Create a dictionary to track the last scan time for each user
    # This helps create realistic time patterns between scans
    user_last_scan = {user: base_date for user in users}
    
    for i in range(num_records):
        # Select a random user
        user = random.choice(users)
        
        # Get the last scan time for this user
        last_scan = user_last_scan[user]
        
        # Generate a timestamp that's after the last scan (between 1 minute and 2 hours later)
        # This creates more realistic scanning patterns
        min_seconds = 60  # minimum 1 minute between scans
        max_seconds = 7200  # maximum 2 hours between scans
        
        # For some users, create tighter scan patterns (more frequent scans)
        if user in ['USER001', 'USER003']:
            max_seconds = 900  # 15 minutes max for these users
        
        random_seconds = random.randint(min_seconds, max_seconds)
        timestamp = last_scan + timedelta(seconds=random_seconds)
        
        # Make sure the timestamp isn't in the future
        if timestamp > now:
            timestamp = now - timedelta(minutes=random.randint(5, 30))
        
        # Update the last scan time for this user
        user_last_scan[user] = timestamp
        
        # Format date and time separately
        created_date = timestamp.strftime('%Y-%m-%d')
        created_time = timestamp.strftime('%H:%M:%S')
        
        record = {
            'Serial #': serials[i],
            'Pallet': random.choice([1, 2, 3]),  # Using numeric values for Pallet
            'Delivery': random.choice(deliveries),
            'Status': random.choice(statuses),
            'Warehouse Number': warehouse,
            'Created by': user,
            'Created on': created_date,
            'Time': created_time,
            'Timestamp': timestamp  # Add a proper timestamp column
        }
        data.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Sort by timestamp to simulate chronological snapshots
    df = df.sort_values('Timestamp')
    
    return df

def generate_delivery_data(deliveries):
    """
    Generate sample delivery-level data (VL06O).
    
    Args:
        deliveries: List of delivery numbers to include
        
    Returns:
        DataFrame with sample delivery data
    """
    data = []
    for delivery in deliveries:
        # Random number of packages between 5 and 30
        num_packages = random.randint(5, 30)
        
        record = {
            'Delivery': delivery,
            'Number of packages': num_packages,
            'Shipping Point': 'SP' + str(random.randint(100, 999)),
            'Created Date': datetime.now().strftime('%Y-%m-%d')
        }
        data.append(record)
    
    return pd.DataFrame(data)

def main():
    """Generate and save sample data files."""
    print("Generating sample data for shipment tracker...")
    
    # Ensure directories exist
    ensure_directories()
    
    # Generate timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Generate serial data
    df_serial = generate_serial_data(200)
    
    # Extract unique deliveries from serial data
    deliveries = df_serial['Delivery'].unique().tolist()
    
    # Generate delivery data
    df_delivery = generate_delivery_data(deliveries)
    
    # Save files
    serial_file = os.path.join(config.SERIAL_NUMBERS_DIR, f"{timestamp}_ZMDESNR.xlsx")
    delivery_file = os.path.join(config.DELIVERY_INFO_DIR, f"{timestamp}_VL06O.xlsx")
    
    df_serial.to_excel(serial_file, index=False)
    df_delivery.to_excel(delivery_file, index=False)
    
    print(f"Generated {len(df_serial)} serial records in {serial_file}")
    print(f"Generated {len(df_delivery)} delivery records in {delivery_file}")
    print("Sample data generation complete.")

if __name__ == "__main__":
    main()
