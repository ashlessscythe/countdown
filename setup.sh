#!/bin/bash
# Setup script for Shipment Tracking Tool
# This script installs required packages and sets up the environment

echo "Setting up Shipment Tracking Tool..."

# Check if Python is installed
if ! command -v python &> /dev/null; then
    echo "Python is not installed. Please install Python 3.7+ and try again."
    exit 1
fi

# Check Python version
python_version=$(python --version 2>&1 | awk '{print $2}')
echo "Found Python version: $python_version"

# Create config.py from example if it doesn't exist
if [ ! -f "config.py" ]; then
    echo "Creating config.py from example..."
    cp config.py.example config.py
    echo "Please edit config.py to match your environment."
fi

# Install required packages
echo "Installing required packages..."
pip install -r requirements.txt

# Create required directories
echo "Creating required directories..."
mkdir -p $(python -c "import config; print(config.SERIAL_NUMBERS_DIR)")
mkdir -p $(python -c "import config; print(config.DELIVERY_INFO_DIR)")
mkdir -p $(python -c "import config; print(config.OUT_DIR)")
mkdir -p visualizations

echo "Setup complete! Running environment check..."
echo ""

# Run environment check
python check_environment.py

echo ""
echo "Next steps:"
echo "1. Edit config.py to match your environment"
echo "2. Generate sample data: python generate_sample_data.py"
echo "3. Run the tool: python shipment_tracker.py"
echo "   or run the entire workflow: python run_workflow.py"
