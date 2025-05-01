"""
Flask dashboard for displaying SAP snapshot comparisons using Parquet files.
"""
import os
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from flask import Flask, render_template, jsonify
from datetime import datetime
import threading
import time
import subprocess

# Import utility modules
from utils.background_tasks import start_background_thread, stop_background_thread, process_running
import config

# Create Flask app
app = Flask(__name__)

# Global variables
last_update_time = None

def load_parquet_data():
    """Load data from Parquet files"""
    global last_update_time
    try:
        # Dictionary to store all dataframes
        dataframes = {}
        
        # List of expected parquet files
        expected_files = [
            'status_summary.parquet',
            'shipment_tree.parquet',
            'user_activity.parquet',
            'status_by_delivery.parquet'
        ]
        
        # Check if output directory exists
        if not os.path.exists(config.OUT_DIR):
            print(f"Warning: Output directory {config.OUT_DIR} does not exist")
            return {}
        
        # Count how many files were found
        files_found = 0
        
        # Load each parquet file
        for file_name in expected_files:
            file_path = os.path.join(config.OUT_DIR, file_name)
            if os.path.exists(file_path):
                try:
                    # Extract the metric name from the file name (remove .parquet extension)
                    metric_name = os.path.splitext(file_name)[0]
                    df = pd.read_parquet(file_path)
                    
                    # Check if dataframe is empty
                    if df.empty:
                        print(f"Warning: {file_name} is empty")
                        continue
                    
                    dataframes[metric_name] = df
                    files_found += 1
                    print(f"Loaded {file_name}: {len(df)} rows")
                except Exception as e:
                    print(f"Error loading {file_name}: {str(e)}")
            else:
                print(f"Warning: {file_path} not found")
        
        # Update last update time only if files were found
        if files_found > 0:
            last_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"Updated last_update_time to {last_update_time}")
        else:
            print("No valid parquet files found")
            
        return dataframes
    except Exception as e:
        print(f"Error loading parquet data: {str(e)}")
        return {}

def get_file_info():
    """Get information about the most recent files processed"""
    try:
        # Check if data directory exists
        if not os.path.exists(config.DATA_DIR):
            print(f"Warning: Data directory {config.DATA_DIR} does not exist")
            return []
        
        # Get the list of Excel files in the data directory
        excel_files = [f for f in os.listdir(config.DATA_DIR) if f.endswith('.xlsx') or f.endswith('.xlsb')]
        
        if not excel_files:
            print("No Excel files found in data directory")
            return []
        
        # Sort files by modification time (newest first)
        excel_files.sort(key=lambda x: os.path.getmtime(os.path.join(config.DATA_DIR, x)), reverse=True)
        
        # Get the two newest files
        newest_files = excel_files[:2] if len(excel_files) >= 2 else excel_files
        
        return newest_files
    except Exception as e:
        print(f"Error getting file info: {str(e)}")
        return []

@app.route('/')
def index():
    """Render the dashboard homepage"""
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    """API endpoint to get the latest data from parquet files"""
    global last_update_time
    
    # Load data from parquet files
    dataframes = load_parquet_data()
    
    # Check if dataframes is empty
    if not dataframes:
        print("No data available from parquet files")
        return jsonify({
            'data': {},
            'metadata': {
                'files_processed': get_file_info(),
                'window_minutes': config.WINDOW_MINUTES
            },
            'last_update_time': last_update_time,
            'message': 'No data available. Please check if parquet files exist and are valid.'
        })
    
    # Convert dataframes to dictionaries for JSON serialization
    data = {}
    for name, df in dataframes.items():
        # Reset index to include index in the JSON
        df_reset = df.reset_index()
        
        # Handle datetime columns for JSON serialization
        for col in df_reset.columns:
            if pd.api.types.is_datetime64_any_dtype(df_reset[col]):
                df_reset[col] = df_reset[col].apply(
                    lambda x: x.isoformat() if pd.notna(x) else None
                )
        
        # Handle NaN values for numeric columns
        for col in df_reset.columns:
            if pd.api.types.is_numeric_dtype(df_reset[col]):
                df_reset[col] = df_reset[col].apply(
                    lambda x: None if pd.isna(x) else x
                )
        
        data[name] = df_reset.to_dict(orient='records')
    
    # Get file info
    newest_files = get_file_info()
    
    # Add metadata
    metadata = {
        'files_processed': newest_files,
        'window_minutes': config.WINDOW_MINUTES
    }
    
    return jsonify({
        'data': data,
        'metadata': metadata,
        'last_update_time': last_update_time
    })

@app.route('/api/update')
def trigger_update():
    """API endpoint to trigger a manual update of the data"""
    global process_running
    
    # Check if a process is already running
    if process_running:
        return jsonify({
            'status': 'warning',
            'message': 'A snapshot processing task is already running',
            'last_update_time': last_update_time
        })
    
    try:
        # Check if compare_snapshots.py exists
        if not os.path.exists("compare_snapshots.py"):
            return jsonify({
                'status': 'error',
                'message': 'compare_snapshots.py script not found',
                'last_update_time': last_update_time
            })
        
        # Set the flag to indicate a process is running
        process_running = True
        
        # Run the compare_snapshots.py script to update the parquet files
        result = subprocess.run(["python", "compare_snapshots.py"], check=True, capture_output=True, text=True)
        
        # Print the output for debugging
        print("compare_snapshots.py output:")
        print(result.stdout)
        
        if result.stderr:
            print("compare_snapshots.py errors:")
            print(result.stderr)
        
        # Reload the parquet files
        dataframes = load_parquet_data()
        
        # Reset the flag
        process_running = False
        
        # Check if dataframes is empty
        if not dataframes:
            return jsonify({
                'status': 'warning',
                'message': 'Snapshot processing completed but no valid data was generated',
                'last_update_time': last_update_time
            })
        
        return jsonify({
            'status': 'success',
            'message': 'Snapshot processing completed and data updated',
            'last_update_time': last_update_time
        })
    except subprocess.CalledProcessError as e:
        # Reset the flag in case of error
        process_running = False
        
        error_message = f"Error running compare_snapshots.py: {str(e)}"
        if e.stderr:
            error_message += f"\nDetails: {e.stderr}"
        
        return jsonify({
            'status': 'error',
            'message': error_message,
            'last_update_time': last_update_time
        })
    except Exception as e:
        # Reset the flag in case of error
        process_running = False
        
        return jsonify({
            'status': 'error',
            'message': f'Error updating data: {str(e)}',
            'last_update_time': last_update_time
        })

@app.route('/api/charts/status_summary')
def status_summary_chart():
    """API endpoint to get the status summary chart data"""
    dataframes = load_parquet_data()
    
    if 'status_summary' not in dataframes or dataframes['status_summary'].empty:
        # Create an empty chart with a message
        fig = go.Figure()
        
        # Add annotation to show no data message
        fig.add_annotation(
            text="No status summary data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16)
        )
        
        fig.update_layout(
            title='Status Summary',
            xaxis_title='Status',
            yaxis_title='Count',
            template='plotly_white',
            xaxis=dict(showticklabels=False),
            yaxis=dict(showticklabels=False)
        )
        
        return jsonify({
            'chart': fig.to_json()
        })
    
    df = dataframes['status_summary']
    
    # Create a bar chart for status counts
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['status'].tolist(),
        y=df['count'].tolist(),
        marker_color='rgb(55, 83, 109)'
    ))
    
    fig.update_layout(
        title='Status Summary',
        xaxis_title='Status',
        yaxis_title='Count',
        template='plotly_white'
    )
    
    return jsonify({
        'chart': fig.to_json()
    })

@app.route('/api/charts/user_activity')
def user_activity_chart():
    """API endpoint to get the user activity chart data"""
    dataframes = load_parquet_data()
    
    if 'user_activity' not in dataframes or dataframes['user_activity'].empty:
        # Create an empty chart with a message
        fig = go.Figure()
        
        # Add annotation to show no data message
        fig.add_annotation(
            text="No user activity data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16)
        )
        
        fig.update_layout(
            title='User Activity',
            xaxis_title='User',
            yaxis_title='Number of Scans',
            template='plotly_white',
            xaxis=dict(showticklabels=False),
            yaxis=dict(showticklabels=False)
        )
        
        return jsonify({
            'chart': fig.to_json(),
            'users': [],
            'num_scans': [],
            'last_scan_times': []
        })
    
    df = dataframes['user_activity']
    
    # Create a bar chart for user activity
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df.index.tolist(),  # User is the index
        y=df['num_scans'].tolist(),
        name='Number of Scans',
        marker_color='rgb(55, 83, 109)'
    ))
    
    fig.update_layout(
        title='User Activity',
        xaxis_title='User',
        yaxis_title='Number of Scans',
        template='plotly_white'
    )
    
    # Convert datetime objects to ISO format strings for JSON serialization
    last_scan_times = []
    for ts in df['last_scan_ts']:
        if pd.notna(ts):
            last_scan_times.append(ts.isoformat())
        else:
            last_scan_times.append(None)
    
    return jsonify({
        'chart': fig.to_json(),
        'users': df.index.tolist(),
        'num_scans': df['num_scans'].tolist(),
        'last_scan_times': last_scan_times
    })

@app.route('/api/charts/status_by_delivery')
def status_by_delivery_chart():
    """API endpoint to get the status by delivery chart data"""
    dataframes = load_parquet_data()
    
    if 'status_by_delivery' not in dataframes or dataframes['status_by_delivery'].empty:
        # Create an empty chart with a message
        fig = go.Figure()
        
        # Add annotation to show no data message
        fig.add_annotation(
            text="No status by delivery data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16)
        )
        
        fig.update_layout(
            title='Status by Delivery',
            xaxis_title='Delivery',
            yaxis_title='Count',
            barmode='stack',
            template='plotly_white',
            xaxis=dict(showticklabels=False),
            yaxis=dict(showticklabels=False)
        )
        
        return jsonify({
            'chart': fig.to_json()
        })
    
    df = dataframes['status_by_delivery']
    
    # Create a stacked bar chart
    fig = go.Figure()
    
    # Add a trace for each status
    for status in df.columns:
        if status != 'total':  # Skip the total column
            fig.add_trace(go.Bar(
                x=df.index.tolist(),  # Delivery is the index
                y=df[status].tolist(),
                name=status
            ))
    
    fig.update_layout(
        title='Status by Delivery',
        xaxis_title='Delivery',
        yaxis_title='Count',
        barmode='stack',
        template='plotly_white'
    )
    
    return jsonify({
        'chart': fig.to_json()
    })

@app.route('/api/charts/shipment_tree')
def shipment_tree_chart():
    """API endpoint to get the shipment tree chart data"""
    dataframes = load_parquet_data()
    
    if 'shipment_tree' not in dataframes or dataframes['shipment_tree'].empty:
        # Create an empty chart with a message
        fig = go.Figure()
        
        # Add annotation to show no data message
        fig.add_annotation(
            text="No shipment tree data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16)
        )
        
        fig.update_layout(
            title='Shipment Tree',
            xaxis_title='Delivery',
            yaxis_title='Shipment',
            template='plotly_white',
            xaxis=dict(showticklabels=False),
            yaxis=dict(showticklabels=False)
        )
        
        return jsonify({
            'chart': fig.to_json()
        })
    
    df = dataframes['shipment_tree']
    
    # Create a heatmap
    fig = go.Figure()
    
    # Remove the total column for the heatmap
    heatmap_df = df.drop(columns=['total']) if 'total' in df.columns else df
    
    fig.add_trace(go.Heatmap(
        z=heatmap_df.values,
        x=heatmap_df.columns.tolist(),
        y=heatmap_df.index.tolist(),
        colorscale='Viridis'
    ))
    
    fig.update_layout(
        title='Shipment Tree',
        xaxis_title='Delivery',
        yaxis_title='Shipment',
        template='plotly_white'
    )
    
    return jsonify({
        'chart': fig.to_json()
    })

if __name__ == '__main__':
    # Load initial data
    load_parquet_data()
    
    # Start background update thread to run compare_snapshots.py
    # This is the only place we start the background thread
    update_thread = start_background_thread()
    
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)
