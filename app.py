import os
from flask import Flask
from utils.background_tasks import start_background_thread, stop_background_thread

# Import blueprints
from routes.main_routes import main_bp
from routes.status_routes import status_bp
from routes.entity_routes import entity_bp
from routes.filter_routes import filter_bp
from routes.misc_routes import misc_bp

app = Flask(__name__)

# Register blueprints
app.register_blueprint(main_bp)
app.register_blueprint(status_bp)
app.register_blueprint(entity_bp)
app.register_blueprint(filter_bp)
app.register_blueprint(misc_bp)

if __name__ == '__main__':
    # Create the templates and static directories if they don't exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    os.makedirs('static/css', exist_ok=True)
    os.makedirs('static/js', exist_ok=True)
    
    # Start the scheduled comparison thread
    compare_thread = start_background_thread()
    
    try:
        app.run(debug=True, port=5000)
    finally:
        # Stop the background thread when the app is shutting down
        stop_background_thread()
        # Wait for the thread to finish
        if compare_thread.is_alive():
            compare_thread.join(timeout=5)
