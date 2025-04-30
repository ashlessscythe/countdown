from flask import Blueprint, render_template, jsonify
from utils.data_utils import load_json_data
from utils.time_utils import get_time_since_change

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
@main_bp.route('/<view>')
def index(view=None):
    """Render the main dashboard with the specified view"""
    if view and view in ['overview', 'deliveries', 'customers', 'shipments', 'users']:
        return render_template('base.html', active_view=view)
    return render_template('index.html')

@main_bp.route('/api/data')
def get_data():
    """API endpoint to get the data"""
    data = load_json_data()
    
    # Add time since last change
    for file_type, file_data in data.items():
        if isinstance(file_data, dict) and 'timestamp' in file_data:
            timestamp = file_data.get('timestamp', '')
            file_data['time_since_change'] = get_time_since_change(timestamp)
    
    return jsonify(data)

@main_bp.route('/api/config')
def get_config():
    """API endpoint to get configuration settings"""
    try:
        # Return only the configuration settings that are needed by the frontend
        import config
        config_data = {
            "filter_whse": getattr(config, 'FILTER_WHSE', None),
            "update_interval": getattr(config, 'UPDATE_INTERVAL', 60)
        }
        return jsonify(config_data)
    except Exception as e:
        return jsonify({"error": str(e)})
