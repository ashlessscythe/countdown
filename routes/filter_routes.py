from flask import Blueprint, jsonify, request
from analyze_status import (
    load_data,
    extract_current_status,
    filter_serials_by_user,
    filter_serials_by_status,
    filter_serials_by_delivery,
    filter_serials_by_customer
)
import config
from utils.time_utils import get_time_since_change

filter_bp = Blueprint('filter', __name__)

@filter_bp.route('/api/filter')
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
