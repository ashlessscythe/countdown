from flask import Blueprint, jsonify
from compare import process_directory
import config

misc_bp = Blueprint('misc', __name__)

@misc_bp.route('/api/run-compare')
def run_compare():
    """API endpoint to manually trigger a comparison"""
    try:
        process_directory(config.INPUT_DIR, config.OUTPUT_JSON)
        return jsonify({"status": "success", "message": "Comparison completed successfully"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
