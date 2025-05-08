"""
Visualization Script for Shipment Tracker Results

This script reads the latest Parquet output file and generates
visualizations to help understand the data.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import config
from shipment_tracker import get_latest_file

def read_latest_output():
    """Read the latest Parquet output file."""
    latest_file = get_latest_file(config.OUT_DIR, "output_*.parquet")
    if not latest_file:
        print("No output files found. Run the shipment tracker first.")
        return None
    
    print(f"Reading data from {latest_file}")
    return pd.read_parquet(latest_file)

def read_latest_time_metrics():
    """Read the latest time metrics Parquet file."""
    latest_file = get_latest_file(config.OUT_DIR, "time_metrics_*.parquet")
    if not latest_file:
        print("No time metrics files found. Run the shipment tracker first.")
        return None
    
    print(f"Reading time metrics from {latest_file}")
    return pd.read_parquet(latest_file)

def create_visualizations(df):
    """Create visualizations from the output data."""
    if df is None or len(df) == 0:
        print("No data to visualize.")
        return
    
    # Create output directory for visualizations
    viz_dir = Path(config.VIZ_DIR)
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
        'Shipped': df['shipped_closed_count'].sum()
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
    if 'last_scan_time' in df.columns and len(df['last_scan_time']) > 0:
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

def create_time_metrics_visualizations(df_time):
    """Create visualizations from the time metrics data."""
    if df_time is None or len(df_time) == 0:
        print("No time metrics data to visualize.")
        return
    
    # Create output directory for visualizations
    viz_dir = Path(config.VIZ_DIR)
    viz_dir.mkdir(exist_ok=True)
    
    # 1. Average time between scans per user (in minutes)
    plt.figure(figsize=(10, 6))
    avg_times = df_time.sort_values('avg_time_between_scans_minutes', ascending=True)
    ax = avg_times.plot(kind='barh', x='user', y='avg_time_between_scans_minutes', color='teal')
    plt.title('Average Time Between Scans by User')
    plt.xlabel('Time (minutes)')
    plt.ylabel('User')
    
    # Add time labels on bars
    for i, v in enumerate(avg_times['avg_time_between_scans_minutes']):
        ax.text(v + 0.1, i, f"{v:.1f} min", va='center')
    
    plt.tight_layout()
    plt.savefig(viz_dir / 'avg_time_between_scans.png')
    plt.close()
    
    # 2. Time since last scan per user (in minutes)
    plt.figure(figsize=(10, 6))
    last_scan_times = df_time.sort_values('time_since_last_scan_minutes', ascending=True)
    ax = last_scan_times.plot(kind='barh', x='user', y='time_since_last_scan_minutes', color='coral')
    plt.title('Time Since Last Scan by User')
    plt.xlabel('Time (minutes)')
    plt.ylabel('User')
    
    # Add time labels on bars
    for i, v in enumerate(last_scan_times['time_since_last_scan_minutes']):
        ax.text(v + 0.1, i, f"{v:.1f} min", va='center')
    
    plt.tight_layout()
    plt.savefig(viz_dir / 'time_since_last_scan.png')
    plt.close()
    
    # 3. Scan count by user
    plt.figure(figsize=(10, 6))
    scan_counts = df_time.sort_values('scan_count', ascending=False)
    ax = scan_counts.plot(kind='bar', x='user', y='scan_count', color='lightblue')
    plt.title('Total Scan Count by User')
    plt.xlabel('User')
    plt.ylabel('Number of Scans')
    
    # Add count labels on bars
    for i, v in enumerate(scan_counts['scan_count']):
        ax.text(i, v + 0.1, str(int(v)), ha='center')
    
    plt.tight_layout()
    plt.savefig(viz_dir / 'scan_count.png')
    plt.close()
    
    # 4. Distribution of scan times for top users
    # Select top 5 users by scan count
    top_users = df_time.sort_values('scan_count', ascending=False).head(5)['user'].tolist()
    
    # Create a boxplot for the distribution of time between scans
    plt.figure(figsize=(12, 8))
    
    # Prepare data for boxplot
    boxplot_data = []
    labels = []
    
    for user in top_users:
        user_data = df_time[df_time['user'] == user]
        if not user_data.empty and 'time_diff_list' in user_data.columns:
            time_diffs = user_data['time_diff_list'].iloc[0]
            # Fix: Check if time_diffs is not None and has elements without using it in a boolean context
            if isinstance(time_diffs, (list, np.ndarray)) and len(time_diffs) > 0:
                # Convert seconds to minutes
                time_diffs_minutes = [t/60 for t in time_diffs]
                boxplot_data.append(time_diffs_minutes)
                labels.append(user)
    
    if boxplot_data:
        plt.boxplot(boxplot_data, labels=labels, patch_artist=True)
        plt.title('Distribution of Time Between Scans for Top Users')
        plt.xlabel('User')
        plt.ylabel('Time Between Scans (minutes)')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(viz_dir / 'scan_time_distribution.png')
    plt.close()
    
    print(f"Time metrics visualizations saved to {viz_dir.absolute()}")

def main():
    """Main function to read data and create visualizations."""
    print("Visualizing shipment tracker results...")
    
    # Read and visualize main output data
    df = read_latest_output()
    create_visualizations(df)
    
    # Read and visualize time metrics data
    df_time = read_latest_time_metrics()
    create_time_metrics_visualizations(df_time)
    
    print("Visualization complete.")

if __name__ == "__main__":
    main()
