import json
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple
import config

def load_data(json_path: str) -> Dict:
    """Load data from the output.json file"""
    with open(json_path, 'r') as f:
        return json.load(f)

def extract_changes(data: Dict) -> List[Dict]:
    """Extract all changes from the data"""
    all_changes = []
    for file_type, file_data in data.items():
        if file_type == "current_status" or file_type == "summary":
            continue
        changes = file_data.get('changes', [])
        for change in changes:
            # Add file_type to each change record
            change['file_type'] = file_type
            all_changes.append(change)
    return all_changes

def extract_current_status(data: Dict) -> Dict[str, Dict]:
    """Extract the current status of all serials"""
    if "current_status" in data:
        return data["current_status"]
    
    # If current_status is not available, build it from changes
    current_status = {}
    changes = extract_changes(data)
    
    for change in changes:
        serial = change.get('serial', '')
        if serial:
            # If this serial is not in current_status or has a newer timestamp, update it
            if serial not in current_status or change.get('timestamp', '') > current_status[serial].get('timestamp', ''):
                current_status[serial] = {
                    'status': change.get('to', ''),
                    'delivery': change.get('delivery', ''),
                    'customer_name': change.get('customer_name', ''),
                    'shipment_number': change.get('shipment_number', ''),
                    'created_by': change.get('created_by', ''),
                    'timestamp': change.get('timestamp', '')
                }
    
    return current_status

def extract_status_history(data: Dict) -> Dict[str, List[Dict]]:
    """
    Extract the complete status history for each serial
    
    This function builds a comprehensive history of all status changes for each serial,
    allowing for more detailed analysis of serial status lifecycles.
    """
    status_history = defaultdict(list)
    changes = extract_changes(data)
    
    # Sort changes by timestamp to ensure chronological order
    sorted_changes = sorted(changes, key=lambda x: x.get('timestamp', ''))
    
    for change in sorted_changes:
        serial = change.get('serial', '')
        if serial:
            # Add this change to the serial's history
            status_entry = {
                'from': change.get('from', ''),
                'to': change.get('to', ''),
                'timestamp': change.get('timestamp', ''),
                'delivery': change.get('delivery', ''),
                'customer_name': change.get('customer_name', ''),
                'shipment_number': change.get('shipment_number', ''),
                'created_by': change.get('created_by', ''),
                'change_type': change.get('change_type', '')
            }
            status_history[serial].append(status_entry)
    
    return dict(status_history)

def get_status_transition_metrics(status_history: Dict[str, List[Dict]]) -> Dict:
    """
    Calculate metrics about status transitions
    
    This function analyzes the status history to identify common transition patterns,
    time spent in each status, and other metrics about the status lifecycle.
    """
    transitions = defaultdict(int)
    status_duration = defaultdict(list)
    
    for serial, history in status_history.items():
        if len(history) < 2:
            continue
        
        # Analyze transitions between statuses
        for i in range(len(history) - 1):
            from_status = history[i].get('to', '')
            to_status = history[i+1].get('to', '')
            if from_status and to_status:
                transition_key = f"{from_status}->{to_status}"
                transitions[transition_key] += 1
                
                # Calculate duration in this status if timestamps are available
                try:
                    from_time = pd.to_datetime(history[i].get('timestamp', ''))
                    to_time = pd.to_datetime(history[i+1].get('timestamp', ''))
                    if from_time and to_time:
                        duration = (to_time - from_time).total_seconds() / 3600  # Duration in hours
                        status_duration[from_status].append(duration)
                except:
                    pass
    
    # Calculate average duration in each status
    avg_duration = {}
    for status, durations in status_duration.items():
        if durations:
            avg_duration[status] = sum(durations) / len(durations)
    
    return {
        'transitions': dict(transitions),
        'avg_duration_hours': avg_duration
    }

def group_by_delivery(changes: List[Dict]) -> Dict[str, List[Dict]]:
    """Group changes by delivery"""
    delivery_groups = defaultdict(list)
    for change in changes:
        delivery = change.get('delivery', 'Unknown')
        delivery_groups[delivery].append(change)
    return dict(delivery_groups)

def group_by_customer(changes: List[Dict]) -> Dict[str, List[Dict]]:
    """Group changes by customer"""
    customer_groups = defaultdict(list)
    for change in changes:
        customer = change.get('customer_name', 'Unknown')
        customer_groups[customer].append(change)
    return dict(customer_groups)

def group_by_shipment(changes: List[Dict]) -> Dict[str, List[Dict]]:
    """Group changes by shipment (if available)"""
    shipment_groups = defaultdict(list)
    for change in changes:
        shipment = change.get('shipment_number', '')
        if shipment:  # Only group if shipment is not empty
            shipment_groups[shipment].append(change)
        else:
            # Group under delivery if shipment is not available
            key = "no_shipment"
            shipment_groups[key].append(change)
    return dict(shipment_groups)

def group_by_user(changes: List[Dict]) -> Dict[str, List[Dict]]:
    """Group changes by user (if available)"""
    user_groups = defaultdict(list)
    for change in changes:
        user = change.get('created_by', '')
        if user:  # Only group if created_by is not empty
            user_groups[user].append(change)
        else:
            user_groups['Unknown'].append(change)
    return dict(user_groups)

def group_by_status(changes: List[Dict]) -> Dict[str, List[Dict]]:
    """Group changes by status"""
    status_groups = defaultdict(list)
    for change in changes:
        status = change.get('to', 'Unknown')
        status_groups[status].append(change)
    return dict(status_groups)

def get_status_summary(changes: List[Dict]) -> Dict[str, int]:
    """Get a summary of statuses"""
    status_counts = defaultdict(int)
    for change in changes:
        status = change.get('to', '')
        if not status and change.get('from', ''):
            status = f"Removed_{change.get('from', '')}"
        elif not status:
            status = 'Unknown'
        status_counts[status] += 1
    return dict(status_counts)

def get_delivery_status_summary(delivery_groups: Dict[str, List[Dict]]) -> Dict[str, Dict[str, int]]:
    """Get status summary for each delivery"""
    delivery_status = {}
    for delivery, changes in delivery_groups.items():
        delivery_status[delivery] = get_status_summary(changes)
    return delivery_status

def get_customer_status_summary(customer_groups: Dict[str, List[Dict]]) -> Dict[str, Dict[str, int]]:
    """Get status summary for each customer"""
    customer_status = {}
    for customer, changes in customer_groups.items():
        customer_status[customer] = get_status_summary(changes)
    return customer_status

def get_shipment_status_summary(shipment_groups: Dict[str, List[Dict]]) -> Dict[str, Dict[str, int]]:
    """Get status summary for each shipment"""
    shipment_status = {}
    for shipment, changes in shipment_groups.items():
        shipment_status[shipment] = get_status_summary(changes)
    return shipment_status

def get_user_status_summary(user_groups: Dict[str, List[Dict]]) -> Dict[str, Dict[str, int]]:
    """Get status summary for each user"""
    user_status = {}
    for user, changes in user_groups.items():
        user_status[user] = get_status_summary(changes)
    return user_status

def get_serials_by_status(changes: List[Dict], status: str) -> List[str]:
    """Get all serials with a specific status"""
    return [change['serial'] for change in changes if change.get('to', '') == status]

def get_serials_by_delivery(changes: List[Dict], delivery: str) -> List[str]:
    """Get all serials for a specific delivery"""
    return [change['serial'] for change in changes if change.get('delivery', '') == delivery]

def get_serials_by_customer(changes: List[Dict], customer: str) -> List[str]:
    """Get all serials for a specific customer"""
    return [change['serial'] for change in changes if change.get('customer_name', '') == customer]

def get_delivery_hierarchy(changes: List[Dict]) -> Dict:
    """
    Create a hierarchical structure:
    Shipment > Delivery > Material > Serial
    """
    hierarchy = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    
    for change in changes:
        shipment = change.get('shipment_number', 'Unknown')
        delivery = change.get('delivery', 'Unknown')
        material = change.get('to', 'Unknown')  # Assuming 'to' field contains material info
        serial = change.get('serial', '')
        
        if serial:
            hierarchy[shipment][delivery][material].append(serial)
    
    # Convert defaultdicts to regular dicts for easier JSON serialization
    return {
        shipment: {
            delivery: {
                material: serials
                for material, serials in delivery_dict.items()
            }
            for delivery, delivery_dict in shipment_dict.items()
        }
        for shipment, shipment_dict in hierarchy.items()
    }

def analyze_status(json_path: str, output_format: str = 'text') -> None:
    """
    Analyze the status of deliveries and shipments
    
    Args:
        json_path: Path to the output.json file
        output_format: Format of the output ('text', 'json', or 'dataframe')
    """
    data = load_data(json_path)
    changes = extract_changes(data)
    current_status = extract_current_status(data)
    status_history = extract_status_history(data)
    
    # Get status transition metrics
    transition_metrics = get_status_transition_metrics(status_history)
    
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
    
    # Group changes
    delivery_groups = group_by_delivery(current_changes)
    customer_groups = group_by_customer(current_changes)
    shipment_groups = group_by_shipment(current_changes)
    user_groups = group_by_user(current_changes)
    status_groups = group_by_status(current_changes)
    
    # Get status summaries
    delivery_status = get_delivery_status_summary(delivery_groups)
    customer_status = get_customer_status_summary(customer_groups)
    shipment_status = get_shipment_status_summary(shipment_groups)
    user_status = get_user_status_summary(user_groups)
    
    # Create hierarchy
    hierarchy = get_delivery_hierarchy(current_changes)
    
    # Prepare output
    result = {
        'delivery_status': delivery_status,
        'customer_status': customer_status,
        'shipment_status': shipment_status,
        'user_status': user_status,
        'status_groups': status_groups,
        'hierarchy': hierarchy,
        'total_changes': len(changes),
        'total_serials': len(current_status),
        'status_history': status_history,
        'transition_metrics': transition_metrics
    }
    
    if output_format == 'json':
        return json.dumps(result, indent=2)
    elif output_format == 'dataframe':
        # Convert to pandas DataFrames
        delivery_df = pd.DataFrame.from_dict(delivery_status, orient='index').fillna(0).astype(int)
        customer_df = pd.DataFrame.from_dict(customer_status, orient='index').fillna(0).astype(int)
        shipment_df = pd.DataFrame.from_dict(shipment_status, orient='index').fillna(0).astype(int)
        user_df = pd.DataFrame.from_dict(user_status, orient='index').fillna(0).astype(int)
        
        return {
            'delivery_status': delivery_df,
            'customer_status': customer_df,
            'shipment_status': shipment_df,
            'user_status': user_df,
            'total_changes': len(changes),
            'total_serials': len(current_status),
            'transition_metrics': transition_metrics
        }
    else:  # text format
        output = []
        output.append(f"Total changes: {len(changes)}")
        output.append(f"Total serials: {len(current_status)}")
        
        output.append("\n=== Status Distribution ===")
        for status, serials in status_groups.items():
            output.append(f"{status}: {len(serials)} serials")
        
        output.append("\n=== Status Transitions ===")
        for transition, count in transition_metrics['transitions'].items():
            output.append(f"{transition}: {count} transitions")
        
        output.append("\n=== Average Time in Status (hours) ===")
        for status, duration in transition_metrics['avg_duration_hours'].items():
            output.append(f"{status}: {duration:.2f} hours")
        
        output.append("\n=== Status by Customer ===")
        for customer, status in customer_status.items():
            output.append(f"\nCustomer: {customer}")
            for status_name, count in status.items():
                output.append(f"  {status_name}: {count}")
        
        output.append("\n=== Status by Delivery ===")
        for delivery, status in delivery_status.items():
            output.append(f"\nDelivery: {delivery}")
            for status_name, count in status.items():
                output.append(f"  {status_name}: {count}")
        
        output.append("\n=== Status by Shipment ===")
        for shipment, status in shipment_status.items():
            output.append(f"\nShipment: {shipment}")
            for status_name, count in status.items():
                output.append(f"  {status_name}: {count}")
        
        output.append("\n=== Status by User ===")
        for user, status in user_status.items():
            output.append(f"\nUser: {user}")
            for status_name, count in status.items():
                output.append(f"  {status_name}: {count}")
        
        return "\n".join(output)

def get_serial_status(json_path: str, serial: str) -> Dict:
    """Get the status of a specific serial"""
    data = load_data(json_path)
    current_status = extract_current_status(data)
    status_history = extract_status_history(data)
    
    result = {}
    
    if serial in current_status:
        result["current_status"] = current_status[serial]
    
    if serial in status_history:
        result["status_history"] = status_history[serial]
    
    if not result:
        return {"error": f"Serial {serial} not found"}
    
    return result

def filter_serials_by_customer(json_path: str, customer: str) -> List[Dict]:
    """Filter serials by customer"""
    data = load_data(json_path)
    current_status = extract_current_status(data)
    
    result = []
    for serial, status_data in current_status.items():
        if status_data.get('customer_name', '') == customer:
            result.append({
                'serial': serial,
                **status_data
            })
    
    return result

def filter_serials_by_delivery(json_path: str, delivery: str) -> List[Dict]:
    """Filter serials by delivery"""
    data = load_data(json_path)
    current_status = extract_current_status(data)
    
    result = []
    for serial, status_data in current_status.items():
        if status_data.get('delivery', '') == delivery:
            result.append({
                'serial': serial,
                **status_data
            })
    
    return result

def filter_serials_by_status(json_path: str, status: str) -> List[Dict]:
    """Filter serials by status"""
    data = load_data(json_path)
    current_status = extract_current_status(data)
    
    result = []
    for serial, status_data in current_status.items():
        if status_data.get('status', '') == status:
            result.append({
                'serial': serial,
                **status_data
            })
    
    return result

def filter_serials_by_user(json_path: str, user: str) -> List[Dict]:
    """Filter serials by user"""
    data = load_data(json_path)
    current_status = extract_current_status(data)
    
    result = []
    for serial, status_data in current_status.items():
        if status_data.get('created_by', '') == user:
            result.append({
                'serial': serial,
                **status_data
            })
    
    return result

def get_status_history_for_serial(json_path: str, serial: str) -> List[Dict]:
    """Get the complete status history for a specific serial"""
    data = load_data(json_path)
    status_history = extract_status_history(data)
    
    if serial in status_history:
        return status_history[serial]
    
    return []

def get_status_transition_report(json_path: str) -> Dict:
    """Generate a report on status transitions"""
    data = load_data(json_path)
    status_history = extract_status_history(data)
    
    return get_status_transition_metrics(status_history)

if __name__ == "__main__":
    # Example usage
    json_path = config.OUTPUT_JSON
    
    # Print overall status analysis
    print(analyze_status(json_path))
    
    # Example: Get status for a specific delivery
    delivery_id = "0078155356"  # Example delivery ID from the sample data
    delivery_serials = filter_serials_by_delivery(json_path, delivery_id)
    print(f"\n\nSerials for delivery {delivery_id}:")
    for serial in delivery_serials:
        print(f"  Serial: {serial['serial']}, Status: {serial.get('status', 'Unknown')}")
    
    # Example: Get status for a specific customer
    customer_name = "INTERNATIONAL MOTORS LLC"  # Example customer from the sample data
    customer_serials = filter_serials_by_customer(json_path, customer_name)
    print(f"\n\nSerials for customer {customer_name}:")
    for serial in customer_serials:
        print(f"  Serial: {serial['serial']}, Status: {serial.get('status', 'Unknown')}")
