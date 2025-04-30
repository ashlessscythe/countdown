import os
import json
import threading
import time
from datetime import datetime
import pandas as pd
from flask import Flask, render_template, jsonify, request
import config
from analyze_status import (
    analyze_status,
    load_data,
    extract_changes,
    extract_current_status,
    extract_status_history,
    get_status_transition_metrics,
    group_by_delivery,
    group_by_customer,
    group_by_shipment,
    group_by_user,
    group_by_status,
    get_delivery_hierarchy,
    filter_serials_by_user,
    filter_serials_by_status,
    filter_serials_by_delivery,
    filter_serials_by_customer,
    get_status_history_for_serial,
    get_status_transition_report
)
from compare import process_directory

app = Flask(__name__)

# Flag to control the background thread
keep_running = True

def get_time_since_change(timestamp_str):
    """Calculate time since last change"""
    if not timestamp_str:
        return "Unknown"
    
    try:
        # Parse the ISO format timestamp
        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        now = datetime.now().astimezone()
        
        # Calculate the difference
        diff = now - timestamp
        
        # Format the difference
        if diff.days > 0:
            return f"{diff.days} days ago"
        elif diff.seconds // 3600 > 0:
            return f"{diff.seconds // 3600} hours ago"
        elif diff.seconds // 60 > 0:
            return f"{diff.seconds // 60} minutes ago"
        else:
            return "Just now"
    except Exception as e:
        return "Error parsing time"

def load_json_data():
    """Load data from the output.json file"""
    try:
        with open(config.OUTPUT_JSON, 'r') as f:
            return json.load(f)
    except Exception as e:
        return {"error": str(e)}

def run_scheduled_compare():
    """Run the comparison process in a background thread"""
    global keep_running
    print(f"Starting scheduled comparison thread with interval of {config.UPDATE_INTERVAL} seconds")
    
    while keep_running:
        try:
            print(f"Running comparison at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            process_directory(config.INPUT_DIR, config.OUTPUT_JSON)
            print(f"Comparison completed successfully")
        except Exception as e:
            print(f"Error during comparison process: {str(e)}")
        
        # Wait for the next interval
        time.sleep(config.UPDATE_INTERVAL)

@app.route('/')
@app.route('/<view>')
def index(view=None):
    """Render the main dashboard with the specified view"""
    if view and view in ['overview', 'deliveries', 'customers', 'shipments', 'users']:
        return render_template('base.html', active_view=view)
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    """API endpoint to get the data"""
    data = load_json_data()
    
    # Add time since last change
    for file_type, file_data in data.items():
        if isinstance(file_data, dict) and 'timestamp' in file_data:
            timestamp = file_data.get('timestamp', '')
            file_data['time_since_change'] = get_time_since_change(timestamp)
    
    return jsonify(data)

@app.route('/api/summary')
def get_summary():
    """API endpoint to get the summary data"""
    try:
        result = analyze_status(config.OUTPUT_JSON, 'dataframe')
        
        # Convert DataFrames to dictionaries for JSON serialization
        summary = {
            'delivery_status': result['delivery_status'].to_dict(),
            'customer_status': result['customer_status'].to_dict(),
            'shipment_status': result['shipment_status'].to_dict(),
            'user_status': result['user_status'].to_dict(),
            'total_changes': result['total_changes'],
            'total_serials': result['total_serials'],
            'transition_metrics': result.get('transition_metrics', {})
        }
        
        return jsonify(summary)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/current_status')
def get_current_status():
    """API endpoint to get the current status of all serials"""
    try:
        data = load_data(config.OUTPUT_JSON)
        current_status = extract_current_status(data)
        
        # Add time since change to each status
        for serial, status_data in current_status.items():
            timestamp = status_data.get('timestamp', '')
            status_data['time_since_change'] = get_time_since_change(timestamp)
        
        return jsonify(current_status)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/status_history')
def get_status_history():
    """API endpoint to get the status history of all serials"""
    try:
        data = load_data(config.OUTPUT_JSON)
        status_history = extract_status_history(data)
        
        # Add time since change to each history entry
        for serial, history in status_history.items():
            for entry in history:
                timestamp = entry.get('timestamp', '')
                entry['time_since_change'] = get_time_since_change(timestamp)
        
        return jsonify(status_history)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/serial_history/<serial>')
def get_serial_history(serial):
    """API endpoint to get the status history of a specific serial"""
    try:
        history = get_status_history_for_serial(config.OUTPUT_JSON, serial)
        
        # Add time since change to each history entry
        for entry in history:
            timestamp = entry.get('timestamp', '')
            entry['time_since_change'] = get_time_since_change(timestamp)
        
        return jsonify(history)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/transition_metrics')
def get_transition_metrics():
    """API endpoint to get status transition metrics"""
    try:
        metrics = get_status_transition_report(config.OUTPUT_JSON)
        return jsonify(metrics)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/status_distribution')
def get_status_distribution():
    """API endpoint to get the distribution of statuses"""
    try:
        data = load_data(config.OUTPUT_JSON)
        current_status = extract_current_status(data)
        
        # Convert current_status dict to a list of changes for easier processing
        current_changes = []
        for serial, status_data in current_status.items():
            change = {
                'serial': serial,
                'to': status_data.get('status', ''),
                'delivery': status_data.get('delivery', ''),
                'customer_name': status_data.get('customer_name', ''),
                'shipment_number': status_data.get('shipment_number', ''),
                'created_by': status_data.get('created_by', ''),
                'timestamp': status_data.get('timestamp', '')
            }
            current_changes.append(change)
        
        # Group by status
        status_groups = group_by_status(current_changes)
        
        # Count serials by status
        status_counts = {}
        for status, serials in status_groups.items():
            status_counts[status] = len(serials)
        
        return jsonify(status_counts)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/deliveries')
def get_deliveries():
    """API endpoint to get all deliveries"""
    data = load_data(config.OUTPUT_JSON)
    current_status = extract_current_status(data)
    
    # Convert current_status dict to a list of changes for easier processing
    current_changes = []
    for serial, status_data in current_status.items():
        change = {
            'serial': serial,
            'status': status_data.get('status', ''),
            'delivery': status_data.get('delivery', ''),
            'customer_name': status_data.get('customer_name', ''),
            'shipment_number': status_data.get('shipment_number', ''),
            'created_by': status_data.get('created_by', ''),
            'timestamp': status_data.get('timestamp', '')
        }
        current_changes.append(change)
    
    # Group by delivery
    delivery_groups = {}
    for change in current_changes:
        delivery = change.get('delivery', 'Unknown')
        if delivery not in delivery_groups:
            delivery_groups[delivery] = {
                'id': delivery,
                'customer': change.get('customer_name', 'Unknown'),
                'count': 0,
                'timestamp': '',
                'time_since_change': 'Unknown',
                'status_counts': {}
            }
        
        delivery_groups[delivery]['count'] += 1
        
        # Update timestamp if this change is more recent
        if not delivery_groups[delivery]['timestamp'] or change.get('timestamp', '') > delivery_groups[delivery]['timestamp']:
            delivery_groups[delivery]['timestamp'] = change.get('timestamp', '')
            delivery_groups[delivery]['time_since_change'] = get_time_since_change(change.get('timestamp', ''))
        
        # Update status counts
        status = change.get('status', 'Unknown')
        if status not in delivery_groups[delivery]['status_counts']:
            delivery_groups[delivery]['status_counts'][status] = 0
        delivery_groups[delivery]['status_counts'][status] += 1
    
    # Convert to list for JSON serialization
    deliveries = list(delivery_groups.values())
    
    return jsonify(deliveries)

@app.route('/api/customers')
def get_customers():
    """API endpoint to get all customers"""
    data = load_data(config.OUTPUT_JSON)
    current_status = extract_current_status(data)
    
    # Convert current_status dict to a list of changes for easier processing
    current_changes = []
    for serial, status_data in current_status.items():
        change = {
            'serial': serial,
            'status': status_data.get('status', ''),
            'delivery': status_data.get('delivery', ''),
            'customer_name': status_data.get('customer_name', ''),
            'shipment_number': status_data.get('shipment_number', ''),
            'created_by': status_data.get('created_by', ''),
            'timestamp': status_data.get('timestamp', '')
        }
        current_changes.append(change)
    
    # Group by customer
    customer_groups = {}
    for change in current_changes:
        customer = change.get('customer_name', 'Unknown')
        if customer not in customer_groups:
            customer_groups[customer] = {
                'name': customer,
                'count': 0,
                'timestamp': '',
                'time_since_change': 'Unknown',
                'status_counts': {}
            }
        
        customer_groups[customer]['count'] += 1
        
        # Update timestamp if this change is more recent
        if not customer_groups[customer]['timestamp'] or change.get('timestamp', '') > customer_groups[customer]['timestamp']:
            customer_groups[customer]['timestamp'] = change.get('timestamp', '')
            customer_groups[customer]['time_since_change'] = get_time_since_change(change.get('timestamp', ''))
        
        # Update status counts
        status = change.get('status', 'Unknown')
        if status not in customer_groups[customer]['status_counts']:
            customer_groups[customer]['status_counts'][status] = 0
        customer_groups[customer]['status_counts'][status] += 1
    
    # Convert to list for JSON serialization
    customers = list(customer_groups.values())
    
    return jsonify(customers)

@app.route('/api/shipments')
def get_shipments():
    """API endpoint to get all shipments"""
    data = load_data(config.OUTPUT_JSON)
    current_status = extract_current_status(data)
    
    # Convert current_status dict to a list of changes for easier processing
    current_changes = []
    for serial, status_data in current_status.items():
        change = {
            'serial': serial,
            'status': status_data.get('status', ''),
            'delivery': status_data.get('delivery', ''),
            'customer_name': status_data.get('customer_name', ''),
            'shipment_number': status_data.get('shipment_number', ''),
            'created_by': status_data.get('created_by', ''),
            'timestamp': status_data.get('timestamp', '')
        }
        current_changes.append(change)
    
    # Group by shipment
    shipment_groups = {}
    for change in current_changes:
        shipment = change.get('shipment_number', '')
        if not shipment:
            continue
        
        if shipment not in shipment_groups:
            shipment_groups[shipment] = {
                'id': shipment,
                'customer': change.get('customer_name', 'Unknown'),
                'count': 0,
                'timestamp': '',
                'time_since_change': 'Unknown',
                'status_counts': {}
            }
        
        shipment_groups[shipment]['count'] += 1
        
        # Update timestamp if this change is more recent
        if not shipment_groups[shipment]['timestamp'] or change.get('timestamp', '') > shipment_groups[shipment]['timestamp']:
            shipment_groups[shipment]['timestamp'] = change.get('timestamp', '')
            shipment_groups[shipment]['time_since_change'] = get_time_since_change(change.get('timestamp', ''))
        
        # Update status counts
        status = change.get('status', 'Unknown')
        if status not in shipment_groups[shipment]['status_counts']:
            shipment_groups[shipment]['status_counts'][status] = 0
        shipment_groups[shipment]['status_counts'][status] += 1
    
    # Convert to list for JSON serialization
    shipments = list(shipment_groups.values())
    
    return jsonify(shipments)

@app.route('/api/users')
def get_users():
    """API endpoint to get all users"""
    data = load_data(config.OUTPUT_JSON)
    current_status = extract_current_status(data)
    
    # Convert current_status dict to a list of changes for easier processing
    current_changes = []
    for serial, status_data in current_status.items():
        change = {
            'serial': serial,
            'status': status_data.get('status', ''),
            'delivery': status_data.get('delivery', ''),
            'customer_name': status_data.get('customer_name', ''),
            'shipment_number': status_data.get('shipment_number', ''),
            'created_by': status_data.get('created_by', ''),
            'timestamp': status_data.get('timestamp', '')
        }
        current_changes.append(change)
    
    # Group by user
    user_groups = {}
    for change in current_changes:
        user = change.get('created_by', 'Unknown')
        if user not in user_groups:
            user_groups[user] = {
                'id': user,
                'count': 0,
                'timestamp': '',
                'time_since_change': 'Unknown',
                'status_counts': {}
            }
        
        user_groups[user]['count'] += 1
        
        # Update timestamp if this change is more recent
        if not user_groups[user]['timestamp'] or change.get('timestamp', '') > user_groups[user]['timestamp']:
            user_groups[user]['timestamp'] = change.get('timestamp', '')
            user_groups[user]['time_since_change'] = get_time_since_change(change.get('timestamp', ''))
        
        # Update status counts
        status = change.get('status', 'Unknown')
        if status not in user_groups[user]['status_counts']:
            user_groups[user]['status_counts'][status] = 0
        user_groups[user]['status_counts'][status] += 1
    
    # Convert to list for JSON serialization
    users = list(user_groups.values())
    
    return jsonify(users)

@app.route('/api/hierarchy')
def get_hierarchy():
    """API endpoint to get the delivery hierarchy"""
    data = load_data(config.OUTPUT_JSON)
    current_status = extract_current_status(data)
    
    # Convert current_status dict to a list of changes for easier processing
    current_changes = []
    for serial, status_data in current_status.items():
        change = {
            'serial': serial,
            'to': status_data.get('status', ''),
            'delivery': status_data.get('delivery', ''),
            'customer_name': status_data.get('customer_name', ''),
            'shipment_number': status_data.get('shipment_number', ''),
            'created_by': status_data.get('created_by', ''),
            'timestamp': status_data.get('timestamp', '')
        }
        current_changes.append(change)
    
    hierarchy = get_delivery_hierarchy(current_changes)
    
    return jsonify(hierarchy)

@app.route('/api/changes')
def get_changes():
    """API endpoint to get all changes"""
    data = load_data(config.OUTPUT_JSON)
    changes = extract_changes(data)
    
    # Add time since change to each change
    for change in changes:
        timestamp = change.get('timestamp', '')
        change['time_since_change'] = get_time_since_change(timestamp)
    
    return jsonify(changes)

@app.route('/api/filter')
def filter_data():
    """API endpoint to filter data by delivery, customer, shipment, or user"""
    filter_type = request.args.get('type', '')
    filter_value = request.args.get('value', '')
    
    if not filter_type or not filter_value:
        return jsonify({"error": "Missing filter type or value"})
    
    try:
        if filter_type == 'delivery':
            filtered_changes = filter_serials_by_delivery(config.OUTPUT_JSON, filter_value)
        elif filter_type == 'customer':
            filtered_changes = filter_serials_by_customer(config.OUTPUT_JSON, filter_value)
        elif filter_type == 'shipment':
            # Filter by shipment number
            data = load_data(config.OUTPUT_JSON)
            current_status = extract_current_status(data)
            filtered_changes = []
            for serial, status_data in current_status.items():
                if status_data.get('shipment_number', '') == filter_value:
                    filtered_changes.append({
                        'serial': serial,
                        **status_data
                    })
        elif filter_type == 'user':
            filtered_changes = filter_serials_by_user(config.OUTPUT_JSON, filter_value)
        elif filter_type == 'status':
            filtered_changes = filter_serials_by_status(config.OUTPUT_JSON, filter_value)
        else:
            return jsonify({"error": f"Invalid filter type: {filter_type}"})
        
        # Add time since change to each change
        for change in filtered_changes:
            timestamp = change.get('timestamp', '')
            change['time_since_change'] = get_time_since_change(timestamp)
        
        return jsonify(filtered_changes)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/run-compare')
def run_compare():
    """API endpoint to manually trigger a comparison"""
    try:
        process_directory(config.INPUT_DIR, config.OUTPUT_JSON)
        return jsonify({"status": "success", "message": "Comparison completed successfully"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/config')
def get_config():
    """API endpoint to get configuration settings"""
    try:
        # Return only the configuration settings that are needed by the frontend
        config_data = {
            "filter_whse": getattr(config, 'FILTER_WHSE', None),
            "update_interval": getattr(config, 'UPDATE_INTERVAL', 60)
        }
        return jsonify(config_data)
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    # Create the templates and static directories if they don't exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    os.makedirs('static/css', exist_ok=True)
    os.makedirs('static/js', exist_ok=True)
    
    # Start the scheduled comparison thread
    compare_thread = threading.Thread(target=run_scheduled_compare, daemon=True)
    compare_thread.start()
    
    try:
        app.run(debug=True, port=5000)
    finally:
        # Set the flag to stop the thread when the app is shutting down
        keep_running = False
        # Wait for the thread to finish
        if compare_thread.is_alive():
            compare_thread.join(timeout=5)
