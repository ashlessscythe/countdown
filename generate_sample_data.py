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
    for i in range(num_records):
        # Random timestamp between base_date and now
        random_seconds = random.randint(0, int((now - base_date).total_seconds()))
        timestamp = base_date + timedelta(seconds=random_seconds)
        
        # Format date and time separately
        created_date = timestamp.strftime('%Y-%m-%d')
        created_time = timestamp.strftime('%H:%M:%S')
        
        record = {
            'Serial #': serials[i],
            'Pallet': random.choice(pallets),
            'Delivery': random.choice(deliveries),
            'Status': random.choice(statuses),
            'Warehouse Number': warehouse,
            'Created by': random.choice(users),
            'Created on': created_date,
            'Time': created_time
        }
        data.append(record)
    
    return pd.DataFrame(data)

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
