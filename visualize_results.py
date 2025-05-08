"""
Visualization Script for Shipment Tracker Results

This script reads the latest Parquet output file and generates
visualizations to help understand the data.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import config
from shipment_tracker import get_latest_file

def read_latest_output():
    """Read the latest Parquet output file."""
    latest_file = get_latest_file(config.OUT_DIR, "*.parquet")
    if not latest_file:
        print("No output files found. Run the shipment tracker first.")
        return None
    
    print(f"Reading data from {latest_file}")
    return pd.read_parquet(latest_file)

def create_visualizations(df):
    """Create visualizations from the output data."""
    if df is None or len(df) == 0:
        print("No data to visualize.")
        return
    
    # Create output directory for visualizations
    viz_dir = Path("visualizations")
    viz_dir.mkdir(exist_ok=True)
    
    # 1. Packages scanned per user
    plt.figure(figsize=(10, 6))
    user_packages = df.groupby('user')['scanned_packages'].sum().sort_values(ascending=False)
    user_packages.plot(kind='bar', color='skyblue')
    plt.title('Total Packages Scanned by User')
    plt.xlabel('User')
    plt.ylabel('Packages Scanned')
    plt.tight_layout()
    plt.savefig(viz_dir / 'packages_by_user.png')
    plt.close()
    
    # 2. Status breakdown (picked vs shipped)
    plt.figure(figsize=(10, 6))
    status_totals = pd.DataFrame({
        'Picked': df['picked_count'].sum(),
        'Shipped': df['shipped_count'].sum()
    }, index=['Status'])
    status_totals.T.plot(kind='bar', color=['orange', 'green'])
    plt.title('Status Breakdown: Picked vs Shipped Packages')
    plt.xlabel('Status')
    plt.ylabel('Number of Packages')
    plt.tight_layout()
    plt.savefig(viz_dir / 'status_breakdown.png')
    plt.close()
    
    # 3. Scanning progress by delivery
    plt.figure(figsize=(12, 8))
    # Filter to top 10 deliveries by total packages for readability
    top_deliveries = df.groupby('delivery')['delivery_total_packages'].first().sort_values(ascending=False).head(10).index
    delivery_data = df[df['delivery'].isin(top_deliveries)].groupby('delivery').agg({
        'scanned_packages': 'sum',
        'delivery_total_packages': 'first'
    })
    
    # Calculate percentage scanned
    delivery_data['percent_scanned'] = (delivery_data['scanned_packages'] / delivery_data['delivery_total_packages'] * 100).clip(upper=100)
    delivery_data = delivery_data.sort_values('percent_scanned', ascending=False)
    
    ax = delivery_data['percent_scanned'].plot(kind='bar', color='lightgreen')
    plt.title('Scanning Progress by Delivery (Top 10 Deliveries)')
    plt.xlabel('Delivery')
    plt.ylabel('Percentage Scanned')
    plt.axhline(y=100, color='red', linestyle='--', alpha=0.7)
    
    # Add percentage labels on bars
    for i, v in enumerate(delivery_data['percent_scanned']):
        ax.text(i, v + 1, f"{v:.1f}%", ha='center')
    
    plt.tight_layout()
    plt.savefig(viz_dir / 'delivery_progress.png')
    plt.close()
    
    # 4. User activity timeline (based on last scan time)
    plt.figure(figsize=(10, 6))
    # Convert to datetime if not already
    if df['last_scan_time'].dtype != 'datetime64[ns]':
        df['last_scan_time'] = pd.to_datetime(df['last_scan_time'])
    
    # Get most recent scan per user
    user_last_scan = df.groupby('user')['last_scan_time'].max().sort_values()
    
    # Plot timeline
    ax = user_last_scan.plot(kind='barh', color='purple')
    plt.title('Last Activity by User')
    plt.xlabel('Last Scan Time')
    plt.ylabel('User')
    
    # Format x-axis as time
    from matplotlib.dates import DateFormatter
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M'))
    plt.gcf().autofmt_xdate()
    
    plt.tight_layout()
    plt.savefig(viz_dir / 'user_activity.png')
    plt.close()
    
    print(f"Visualizations saved to {viz_dir.absolute()}")

def main():
    """Main function to read data and create visualizations."""
    print("Visualizing shipment tracker results...")
    df = read_latest_output()
    create_visualizations(df)
    print("Visualization complete.")

if __name__ == "__main__":
    main()
