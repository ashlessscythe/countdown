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
    
    # Filter to only include users who do ASH (not just SHP)
    # The is_ash_user flag is added in shipment_tracker.py
    if 'is_ash_user' in df.columns:
        df_ash_users = df[df['is_ash_user'] == True]
        print(f"Filtered data to only include users who do ASH: {len(df_ash_users)} out of {len(df)} records")
    else:
        # Fallback if the flag is not present
        print("Warning: is_ash_user flag not found in data. Using all users.")
        df_ash_users = df
    
    # 1. Packages scanned per user (only ASH users)
    plt.figure(figsize=(10, 6))
    user_packages = df_ash_users.groupby('user')['scanned_packages'].sum().sort_values(ascending=False)
    user_packages.plot(kind='bar', color='skyblue')
    plt.title('Total Packages Scanned by User (ASH Users Only)')
    plt.xlabel('User')
    plt.ylabel('Packages Scanned')
    plt.tight_layout()
    plt.savefig(viz_dir / 'packages_by_user.png')
    plt.close()
    
    # 2. Status breakdown (picked vs shipped)
    plt.figure(figsize=(10, 6))
    status_totals = pd.DataFrame({
        'Picked': df_ash_users['picked_count'].sum(),
        'Shipped': df_ash_users['shipped_closed_count'].sum()
    }, index=['Status'])
    status_totals.T.plot(kind='bar', color=['orange', 'green'])
    plt.title('Status Breakdown: Picked vs Shipped Packages (ASH Users Only)')
    plt.xlabel('Status')
    plt.ylabel('Number of Packages')
    plt.tight_layout()
    plt.savefig(viz_dir / 'status_breakdown.png')
    plt.close()
    
    # 3. Scanning progress by delivery (only ASH users)
    plt.figure(figsize=(12, 8))
    # Filter to top 10 deliveries by total packages for readability
    top_deliveries = df_ash_users.groupby('delivery')['delivery_total_packages'].first().sort_values(ascending=False).head(10).index
    delivery_data = df_ash_users[df_ash_users['delivery'].isin(top_deliveries)].groupby('delivery').agg({
        'scanned_packages': 'sum',
        'delivery_total_packages': 'first'
    })
    
    # Calculate percentage scanned
    delivery_data['percent_scanned'] = (delivery_data['scanned_packages'] / delivery_data['delivery_total_packages'] * 100).clip(upper=100)
    delivery_data = delivery_data.sort_values('percent_scanned', ascending=False)
    
    ax = delivery_data['percent_scanned'].plot(kind='bar', color='lightgreen')
    plt.title('Scanning Progress by Delivery (Top 10 Deliveries, ASH Users Only)')
    plt.xlabel('Delivery')
    plt.ylabel('Percentage Scanned')
    plt.axhline(y=100, color='red', linestyle='--', alpha=0.7)
    
    # Add percentage labels on bars
    for i, v in enumerate(delivery_data['percent_scanned']):
        ax.text(i, v + 1, f"{v:.1f}%", ha='center')
    
    plt.tight_layout()
    plt.savefig(viz_dir / 'delivery_progress.png')
    plt.close()
    
    # 4. User activity timeline (based on last scan time, only ASH users)
    plt.figure(figsize=(10, 6))
    # Convert to datetime if not already
    if 'last_scan_time' in df_ash_users.columns and len(df_ash_users['last_scan_time']) > 0:
        if df_ash_users['last_scan_time'].dtype != 'datetime64[ns]':
            df_ash_users['last_scan_time'] = pd.to_datetime(df_ash_users['last_scan_time'])
        
        # Get most recent scan per user
        user_last_scan = df_ash_users.groupby('user')['last_scan_time'].max().sort_values()
        
        # Plot timeline
        ax = user_last_scan.plot(kind='barh', color='purple')
        plt.title('Last Activity by User (ASH Users Only)')
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
    
    # Note: Time metrics are already filtered to only include ASH users in shipment_tracker.py
    
    # 1. Average time between scans per user (in minutes)
    plt.figure(figsize=(10, 6))
    avg_times = df_time.sort_values('avg_time_between_scans_minutes', ascending=True)
    ax = avg_times.plot(kind='barh', x='user', y='avg_time_between_scans_minutes', color='teal')
    plt.title('Average Time Between Scans by User (ASH Users Only)')
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
    plt.title('Time Since Last Scan by User (ASH Users Only)')
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
    plt.title('Total Scan Count by User (ASH Users Only)')
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
        plt.title('Distribution of Time Between Scans for Top Users (ASH Users Only)')
        plt.xlabel('User')
        plt.ylabel('Time Between Scans (minutes)')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(viz_dir / 'scan_time_distribution.png')
    plt.close()
    
    print(f"Time metrics visualizations saved to {viz_dir.absolute()}")

def create_delivery_completion_visualization(df):
    """
    Create visualization showing delivery completion by package count.
    Shows deliveries with most recent activity and displays raw counts (e.g., "30 of 45 packages scanned").
    
    Args:
        df (DataFrame): DataFrame containing the aggregated shipment data
    """
    if df is None or len(df) == 0:
        print("No data to visualize delivery completion.")
        return
    
    # Create output directory for visualizations
    viz_dir = Path(config.VIZ_DIR)
    viz_dir.mkdir(exist_ok=True)
    
    # Filter to only include users who do ASH (not just SHP)
    if 'is_ash_user' in df.columns:
        df_ash_users = df[df['is_ash_user'] == True]
        print(f"Filtered data to only include users who do ASH: {len(df_ash_users)} out of {len(df)} records")
    else:
        # Fallback if the flag is not present
        print("Warning: is_ash_user flag not found in data. Using all users.")
        df_ash_users = df
    
    # Check if last_scan_time is available in the dataframe
    has_time_data = 'last_scan_time' in df_ash_users.columns
    
    # Prepare aggregation dictionary based on available columns
    agg_dict = {
        'scanned_packages': 'sum',
        'delivery_total_packages': 'first'
    }
    
    # Add last_scan_time to aggregation if it exists
    if has_time_data:
        agg_dict['last_scan_time'] = 'max'
    
    # Aggregate data by delivery to get total scanned packages and total packages
    delivery_data = df_ash_users.groupby('delivery').agg(agg_dict).reset_index()
    
    # Filter out rows with missing delivery_total_packages
    delivery_data = delivery_data.dropna(subset=['delivery_total_packages'])
    
    # Convert to appropriate types
    delivery_data['scanned_packages'] = delivery_data['scanned_packages'].astype(int)
    delivery_data['delivery_total_packages'] = delivery_data['delivery_total_packages'].astype(int)
    
    # Sort by most recent activity if time data is available, otherwise by completion percentage
    if has_time_data:
        delivery_data = delivery_data.sort_values('last_scan_time', ascending=False)
    else:
        # Calculate completion percentage for sorting
        delivery_data['completion_percentage'] = (delivery_data['scanned_packages'] / 
                                                delivery_data['delivery_total_packages'] * 100)
        delivery_data = delivery_data.sort_values('completion_percentage', ascending=False)
    
    # Take top 10 deliveries for readability (reduced from 15)
    delivery_data = delivery_data.head(10)
    
    # Calculate completion percentage for all deliveries
    delivery_data['completion_percentage'] = (delivery_data['scanned_packages'] / 
                                             delivery_data['delivery_total_packages'] * 100)
    
    # Sort by completion percentage for better visualization (descending order)
    delivery_data = delivery_data.sort_values('completion_percentage', ascending=False)
    
    # Create a more readable horizontal bar chart
    plt.figure(figsize=(14, 8))  # Wider figure for better readability
    
    # Create the plot with more space between bars
    ax = plt.subplot(111)
    
    # Use delivery as y-axis labels but make them more readable
    y_pos = np.arange(len(delivery_data))
    
    # Calculate the maximum package count for setting x-axis limits
    max_packages = max(delivery_data['delivery_total_packages']) * 1.2  # Add 20% margin
    
    # Plot the bars with increased height and spacing
    bars = ax.barh(y_pos, delivery_data['scanned_packages'], 
                  height=0.5,  # Reduced height for better spacing
                  color='#4CAF50',  # Green color for better visibility
                  label='Scanned Packages')
    
    # Add total package count as a line or marker
    for i, (_, row) in enumerate(delivery_data.iterrows()):
        # Add a line representing total packages
        ax.plot([0, row['delivery_total_packages']], [y_pos[i], y_pos[i]], 
               'k-', alpha=0.5, linewidth=2)
        
        # Add a marker at the end
        ax.plot(row['delivery_total_packages'], y_pos[i], 
               'ro', alpha=0.8, markersize=8)
    
    # Add text labels showing "X of Y packages" with better positioning
    for i, (_, row) in enumerate(delivery_data.iterrows()):
        # Position text at the end of the bar or at a minimum position for visibility
        text_x_pos = max(row['scanned_packages'] + (max_packages * 0.02), max_packages * 0.3)
        
        # Format text with bold for scanned packages
        ax.text(text_x_pos, y_pos[i], 
               f"{int(row['scanned_packages'])} of {int(row['delivery_total_packages'])} packages", 
               va='center', fontsize=10, fontweight='bold')
        
        # Add percentage in parentheses
        percentage = row['completion_percentage']
        ax.text(text_x_pos, y_pos[i] - 0.2, 
               f"({percentage:.1f}%)", 
               va='center', fontsize=9, color='#555555')
    
    # Set custom y-tick labels with delivery numbers
    plt.yticks(y_pos, delivery_data['delivery'])
    
    # Set x-axis limit to ensure all labels are visible
    plt.xlim(0, max_packages)
    
    # Set labels and title with improved styling
    plt.title('Delivery Completion by Package Count', fontsize=16, fontweight='bold')
    plt.xlabel('Number of Packages', fontsize=12)
    plt.ylabel('Delivery Number', fontsize=12)
    
    # Add gridlines for better readability
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    
    # Add a legend with better positioning
    from matplotlib.lines import Line2D
    custom_lines = [
        Line2D([0], [0], color='#4CAF50', lw=4),
        Line2D([0], [0], color='black', alpha=0.5, linewidth=2),
        Line2D([0], [0], marker='o', color='red', alpha=0.8, markersize=8, linestyle='None')
    ]
    ax.legend(custom_lines, ['Scanned Packages', 'Total Packages', 'Target'], 
             loc='upper right', frameon=True, framealpha=0.9)
    
    plt.tight_layout()
    plt.savefig(viz_dir / 'delivery_completion.png', dpi=120)  # Higher DPI for better quality
    plt.close()
    
    print(f"Delivery completion visualization saved to {viz_dir.absolute()}")

def main():
    """Main function to read data and create visualizations."""
    print("Visualizing shipment tracker results...")
    
    # Read and visualize main output data
    df = read_latest_output()
    create_visualizations(df)
    create_delivery_completion_visualization(df)
    
    # Read and visualize time metrics data
    df_time = read_latest_time_metrics()
    create_time_metrics_visualizations(df_time)
    
    print("Visualization complete.")

if __name__ == "__main__":
    main()
