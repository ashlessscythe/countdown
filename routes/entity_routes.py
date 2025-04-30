from flask import Blueprint, jsonify
from analyze_status import (
    load_data,
    extract_current_status,
    get_delivery_hierarchy,
    group_by_status
)
import config
from utils.time_utils import get_time_since_change

entity_bp = Blueprint('entity', __name__)

@entity_bp.route('/api/deliveries')
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

@entity_bp.route('/api/customers')
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

@entity_bp.route('/api/shipments')
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

@entity_bp.route('/api/users')
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

@entity_bp.route('/api/hierarchy')
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
