import argparse
import json
import sys
import config
from analyze_status import (
    analyze_status,
    filter_serials_by_customer,
    filter_serials_by_delivery,
    filter_serials_by_status,
    get_serial_status,
    load_data,
    extract_changes
)

def print_serial_details(serial_data):
    """Print details for a single serial"""
    print(f"\nSerial: {serial_data.get('serial', 'Unknown')}")
    print(f"  Status: {serial_data.get('to', 'Unknown')}")
    print(f"  Previous Status: {serial_data.get('from', 'None')}")
    print(f"  Delivery: {serial_data.get('delivery', 'Unknown')}")
    print(f"  Customer: {serial_data.get('customer_name', 'Unknown')}")
    print(f"  Shipment: {serial_data.get('shipment', 'None')}")
    print(f"  Change Type: {serial_data.get('change_type', 'Unknown')}")
    print(f"  File Type: {serial_data.get('file_type', 'Unknown')}")

def list_unique_values(json_path, field):
    """List all unique values for a specific field"""
    data = load_data(json_path)
    changes = extract_changes(data)
    
    unique_values = set()
    for change in changes:
        value = change.get(field, '')
        if value:
            unique_values.add(value)
    
    return sorted(list(unique_values))

def main():
    parser = argparse.ArgumentParser(description='Analyze delivery and shipment statuses')
    parser.add_argument('--json', default=config.OUTPUT_JSON, help='Path to the output.json file')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Overall status command
    status_parser = subparsers.add_parser('status', help='Show overall status')
    status_parser.add_argument('--format', choices=['text', 'json'], default=config.DEFAULT_OUTPUT_FORMAT, 
                              help='Output format (text or json)')
    
    # Filter by customer command
    customer_parser = subparsers.add_parser('customer', help='Filter by customer')
    customer_parser.add_argument('customer', nargs='?', help='Customer name to filter by')
    customer_parser.add_argument('--list', action='store_true', help='List all customers')
    
    # Filter by delivery command
    delivery_parser = subparsers.add_parser('delivery', help='Filter by delivery')
    delivery_parser.add_argument('delivery', nargs='?', help='Delivery ID to filter by')
    delivery_parser.add_argument('--list', action='store_true', help='List all deliveries')
    
    # Filter by status command
    status_filter_parser = subparsers.add_parser('filter-status', help='Filter by status')
    status_filter_parser.add_argument('status', nargs='?', help='Status to filter by')
    status_filter_parser.add_argument('--list', action='store_true', help='List all statuses')
    
    # Get serial details command
    serial_parser = subparsers.add_parser('serial', help='Get details for a specific serial')
    serial_parser.add_argument('serial', help='Serial number to get details for')
    
    args = parser.parse_args()
    
    # If no command is provided, show help
    if not args.command:
        parser.print_help()
        return
    
    # Execute the appropriate command
    if args.command == 'status':
        result = analyze_status(args.json, args.format)
        print(result)
    
    elif args.command == 'customer':
        if args.list:
            customers = list_unique_values(args.json, 'customer_name')
            print("\nAvailable customers:")
            for customer in customers:
                print(f"  {customer}")
        elif args.customer:
            serials = filter_serials_by_customer(args.json, args.customer)
            if serials:
                print(f"\nFound {len(serials)} serials for customer '{args.customer}':")
                for serial in serials:
                    print_serial_details(serial)
            else:
                print(f"No serials found for customer '{args.customer}'")
        else:
            customer_parser.print_help()
    
    elif args.command == 'delivery':
        if args.list:
            deliveries = list_unique_values(args.json, 'delivery')
            print("\nAvailable deliveries:")
            for delivery in deliveries:
                print(f"  {delivery}")
        elif args.delivery:
            serials = filter_serials_by_delivery(args.json, args.delivery)
            if serials:
                print(f"\nFound {len(serials)} serials for delivery '{args.delivery}':")
                for serial in serials:
                    print_serial_details(serial)
            else:
                print(f"No serials found for delivery '{args.delivery}'")
        else:
            delivery_parser.print_help()
    
    elif args.command == 'filter-status':
        if args.list:
            # For status, we need to look at the 'to' field
            data = load_data(args.json)
            changes = extract_changes(data)
            statuses = set()
            for change in changes:
                status = change.get('to', '')
                if status:
                    statuses.add(status)
            
            print("\nAvailable statuses:")
            for status in sorted(list(statuses)):
                print(f"  {status}")
        elif args.status:
            serials = filter_serials_by_status(args.json, args.status)
            if serials:
                print(f"\nFound {len(serials)} serials with status '{args.status}':")
                for serial in serials:
                    print_serial_details(serial)
            else:
                print(f"No serials found with status '{args.status}'")
        else:
            status_filter_parser.print_help()
    
    elif args.command == 'serial':
        serial_data = get_serial_status(args.json, args.serial)
        if 'error' in serial_data:
            print(serial_data['error'])
        else:
            print(f"Details for serial '{args.serial}':")
            print_serial_details(serial_data)

if __name__ == '__main__':
    main()
