from flask import Blueprint, jsonify
from analyze_status import (
    analyze_status,
    load_data,
    extract_changes,
    extract_current_status,
    extract_status_history,
    get_status_transition_metrics,
    get_status_history_for_serial,
    get_status_transition_report,
    group_by_status
)
import config
from utils.time_utils import get_time_since_change

status_bp = Blueprint('status', __name__)

@status_bp.route('/api/summary')
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

@status_bp.route('/api/current_status')
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

@status_bp.route('/api/status_history')
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

@status_bp.route('/api/serial_history/<serial>')
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

@status_bp.route('/api/transition_metrics')
def get_transition_metrics():
    """API endpoint to get status transition metrics"""
    try:
        metrics = get_status_transition_report(config.OUTPUT_JSON)
        return jsonify(metrics)
    except Exception as e:
        return jsonify({"error": str(e)})

@status_bp.route('/api/status_distribution')
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

@status_bp.route('/api/changes')
def get_changes():
    """API endpoint to get all changes"""
    try:
        data = load_data(config.OUTPUT_JSON)
        changes = extract_changes(data)
        
        # Add time since change to each change
        for change in changes:
            timestamp = change.get('timestamp', '')
            change['time_since_change'] = get_time_since_change(timestamp)
        
        return jsonify(changes)
    except Exception as e:
        return jsonify({"error": str(e)})
