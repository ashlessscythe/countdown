"""
Flask dashboard for displaying SAP snapshot comparisons.
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

# Import utility modules
from utils.background_tasks import start_background_thread, stop_background_thread
import config
from compare_snapshots import compare_snapshots, get_newest_excel_files

# Create Flask app
app = Flask(__name__)

# Global variables
comparison_data = None
last_update_time = None

def load_comparison_data():
    """Load comparison data from the JSON file"""
    global comparison_data, last_update_time
    try:
        with open('comparison_results.json', 'r') as f:
            comparison_data = json.load(f)
            last_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            return comparison_data
    except Exception as e:
        print(f"Error loading comparison data: {str(e)}")
        return None

def update_comparison_data():
    """Update comparison data by running the comparison script"""
    global comparison_data, last_update_time
    try:
        # Get the two newest Excel files from the input directory
        input_dir = config.INPUT_DIR
        file1, file2 = get_newest_excel_files(input_dir)
        
        if file1 is None or file2 is None:
            print("Need at least two Excel files for comparison")
            return
        
        print(f"Updating comparison data: {file1} and {file2}")
        results = compare_snapshots(file1, file2)
        
        # Save the results to a JSON file
        output_file = config.OUTPUT_JSON
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Update global variables
        comparison_data = results
        last_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Comparison data updated at {last_update_time}")
    except Exception as e:
        print(f"Error updating comparison data: {str(e)}")

def background_update_task():
    """Background task to update comparison data on an interval"""
    while True:
        try:
            update_comparison_data()
        except Exception as e:
            print(f"Error in background update task: {str(e)}")
        
        # Wait for the next interval
        time.sleep(config.UPDATE_INTERVAL)

@app.route('/')
def index():
    """Render the dashboard homepage"""
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    """API endpoint to get the latest comparison data"""
    global comparison_data, last_update_time
    
    if comparison_data is None:
        comparison_data = load_comparison_data()
    
    return jsonify({
        'comparison_data': comparison_data,
        'last_update_time': last_update_time
    })

@app.route('/api/update')
def trigger_update():
    """API endpoint to trigger a manual update of the comparison data"""
    update_comparison_data()
    return jsonify({
        'status': 'success',
        'message': 'Comparison data updated',
        'last_update_time': last_update_time
    })

@app.route('/api/charts/user_scan_times')
def user_scan_times_chart():
    """API endpoint to get the user scan times chart data"""
    global comparison_data
    
    if comparison_data is None:
        comparison_data = load_comparison_data()
    
    if comparison_data is None:
        return jsonify({'error': 'No comparison data available'})
    
    # Prepare data for the chart
    users = []
    avg_times = []
    total_scans = []
    
    for user, data in comparison_data['user_scan_times'].items():
        users.append(user)
        avg_times.append(data['avg_time'])
        total_scans.append(data['total_scans'])
    
    # Create a bar chart for average scan times
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=users,
        y=avg_times,
        name='Average Scan Time (seconds)',
        marker_color='rgb(55, 83, 109)'
    ))
    
    fig.update_layout(
        title='Average Scan Time by User',
        xaxis_title='User',
        yaxis_title='Average Time (seconds)',
        template='plotly_white'
    )
    
    return jsonify({
        'chart': fig.to_json(),
        'users': users,
        'avg_times': avg_times,
        'total_scans': total_scans
    })

@app.route('/api/charts/scan_distribution')
def scan_distribution_chart():
    """API endpoint to get the scan distribution chart data"""
    global comparison_data
    
    if comparison_data is None:
        comparison_data = load_comparison_data()
    
    if comparison_data is None:
        return jsonify({'error': 'No comparison data available'})
    
    # Extract time differences
    time_diffs = [delta['time_diff'] for delta in comparison_data['serial_deltas']]
    
    # Create a histogram
    fig = go.Figure()
    
    fig.add_trace(go.Histogram(
        x=time_diffs,
        nbinsx=20,
        marker_color='rgb(55, 83, 109)'
    ))
    
    fig.update_layout(
        title='Distribution of Scan Time Differences',
        xaxis_title='Time Difference (seconds)',
        yaxis_title='Count',
        template='plotly_white'
    )
    
    return jsonify({
        'chart': fig.to_json()
    })

@app.route('/api/charts/timeline')
def timeline_chart():
    """API endpoint to get the timeline chart data"""
    global comparison_data
    
    if comparison_data is None:
        comparison_data = load_comparison_data()
    
    if comparison_data is None:
        return jsonify({'error': 'No comparison data available'})
    
    # Prepare data for the chart
    serials = []
    earlier_times = []
    later_times = []
    users = []
    
    for delta in comparison_data['serial_deltas']:
        serials.append(delta['serial'])
        earlier_times.append(delta['earlier_timestamp'])
        later_times.append(delta['later_timestamp'])
        users.append(delta['earlier_user'])
    
    # Create a timeline chart
    fig = go.Figure()
    
    for i, serial in enumerate(serials):
        fig.add_trace(go.Scatter(
            x=[earlier_times[i], later_times[i]],
            y=[i, i],
            mode='markers+lines',
            name=serial,
            text=[f"User: {users[i]}, Time: {earlier_times[i]}", 
                  f"User: {users[i]}, Time: {later_times[i]}"],
            marker=dict(size=10)
        ))
    
    fig.update_layout(
        title='Scan Timeline by Serial Number',
        xaxis_title='Timestamp',
        yaxis_title='Serial Number',
        yaxis=dict(
            tickmode='array',
            tickvals=list(range(len(serials))),
            ticktext=serials
        ),
        template='plotly_white',
        height=600
    )
    
    return jsonify({
        'chart': fig.to_json()
    })

def start_background_thread():
    """Start the background thread for updating comparison data"""
    update_thread = threading.Thread(target=background_update_task, daemon=True)
    update_thread.start()
    return update_thread

if __name__ == '__main__':
    # Load initial comparison data
    load_comparison_data()
    
    # Start background update thread
    update_thread = start_background_thread()
    
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)
