import json
import config

def load_json_data():
    """Load data from the output.json file"""
    try:
        with open(config.OUTPUT_JSON, 'r') as f:
            return json.load(f)
    except Exception as e:
        return {"error": str(e)}
