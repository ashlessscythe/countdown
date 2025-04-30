import os
import json
from flask import Blueprint, jsonify
import config
from datetime import datetime

delta_bp = Blueprint('delta', __name__)

def get_data_dir():
    """Get the data directory path"""
    return os.path.join(config.BASE_DIR, "data")

def get_snapshots_dir():
    """Get the snapshots directory path"""
    return os.path.join(get_data_dir(), "snapshots")

def get_changes_dir():
    """Get the changes directory path"""
    return os.path.join(get_data_dir(), "changes")

def load_json_file(file_path):
    """Load a JSON file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        return {"error": str(e)}

@delta_bp.route('/api/snapshots')
def get_snapshots():
    """API endpoint to get a list of all snapshots"""
    try:
        snapshots_dir = get_snapshots_dir()
        if not os.path.exists(snapshots_dir):
            return jsonify({"error": f"Snapshots directory not found: {snapshots_dir}"})
        
        snapshots = []
        for filename in os.listdir(snapshots_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(snapshots_dir, filename)
                # Get file stats
                stats = os.stat(file_path)
                # Try to extract timestamp from filename
                try:
                    # Format is typically YYYYMMDD-HHMMSS_suffix.json
                    date_part = filename.split('_')[0]
                    timestamp = datetime.strptime(date_part, "%Y%m%d-%H%M%S").replace(tzinfo=datetime.now().astimezone().tzinfo).isoformat()
                except:
                    # If parsing fails, use file modification time
                    timestamp = datetime.fromtimestamp(stats.st_mtime).replace(tzinfo=datetime.now().astimezone().tzinfo).isoformat()
                
                # Load the file to get metadata
                data = load_json_file(file_path)
                record_count = data.get("metadata", {}).get("record_count", 0)
                source_file = data.get("metadata", {}).get("source_file", "")
                
                snapshots.append({
                    "filename": filename,
                    "path": file_path,
                    "timestamp": timestamp,
                    "size": stats.st_size,
                    "record_count": record_count,
                    "source_file": source_file
                })
        
        # Sort snapshots by timestamp (newest first)
        snapshots.sort(key=lambda x: x["timestamp"], reverse=True)
        
        return jsonify(snapshots)
    except Exception as e:
        return jsonify({"error": str(e)})

@delta_bp.route('/api/snapshot/<filename>')
def get_snapshot(filename):
    """API endpoint to get a specific snapshot"""
    try:
        file_path = os.path.join(get_snapshots_dir(), filename)
        if not os.path.exists(file_path):
            return jsonify({"error": f"Snapshot file not found: {filename}"})
        
        data = load_json_file(file_path)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})

@delta_bp.route('/api/deltas')
def get_deltas():
    """API endpoint to get a list of all deltas"""
    try:
        changes_dir = get_changes_dir()
        if not os.path.exists(changes_dir):
            return jsonify({"error": f"Changes directory not found: {changes_dir}"})
        
        deltas = []
        for filename in os.listdir(changes_dir):
            if filename.endswith('_delta.json'):
                file_path = os.path.join(changes_dir, filename)
                # Get file stats
                stats = os.stat(file_path)
                # Try to extract timestamp from filename
                try:
                    # Format is typically YYYYMMDD-HHMMSS_delta.json
                    date_part = filename.split('_')[0]
                    timestamp = datetime.strptime(date_part, "%Y%m%d-%H%M%S").replace(tzinfo=datetime.now().astimezone().tzinfo).isoformat()
                except:
                    # If parsing fails, use file modification time
                    timestamp = datetime.fromtimestamp(stats.st_mtime).replace(tzinfo=datetime.now().astimezone().tzinfo).isoformat()
                
                # Load the file to get metadata
                data = load_json_file(file_path)
                added_count = data.get("metadata", {}).get("added_count", 0)
                removed_count = data.get("metadata", {}).get("removed_count", 0)
                updated_count = data.get("metadata", {}).get("updated_count", 0)
                
                deltas.append({
                    "filename": filename,
                    "path": file_path,
                    "timestamp": timestamp,
                    "size": stats.st_size,
                    "added_count": added_count,
                    "removed_count": removed_count,
                    "updated_count": updated_count,
                    "total_changes": added_count + removed_count + updated_count
                })
        
        # Sort deltas by timestamp (newest first)
        deltas.sort(key=lambda x: x["timestamp"], reverse=True)
        
        return jsonify(deltas)
    except Exception as e:
        return jsonify({"error": str(e)})

@delta_bp.route('/api/delta/<filename>')
def get_delta(filename):
    """API endpoint to get a specific delta"""
    try:
        file_path = os.path.join(get_changes_dir(), filename)
        if not os.path.exists(file_path):
            return jsonify({"error": f"Delta file not found: {filename}"})
        
        data = load_json_file(file_path)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})

@delta_bp.route('/api/master-history')
def get_master_history():
    """API endpoint to get the master history"""
    try:
        file_path = os.path.join(get_data_dir(), "master_history.json")
        if not os.path.exists(file_path):
            return jsonify({"error": f"Master history file not found: {file_path}"})
        
        data = load_json_file(file_path)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})

@delta_bp.route('/api/current-status')
def get_current_status():
    """API endpoint to get the current status"""
    try:
        file_path = os.path.join(get_data_dir(), "current_status.json")
        if not os.path.exists(file_path):
            return jsonify({"error": f"Current status file not found: {file_path}"})
        
        data = load_json_file(file_path)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})

@delta_bp.route('/api/statistics')
def get_statistics():
    """API endpoint to get the statistics"""
    try:
        file_path = os.path.join(get_data_dir(), "statistics.json")
        if not os.path.exists(file_path):
            return jsonify({"error": f"Statistics file not found: {file_path}"})
        
        data = load_json_file(file_path)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})
