#!/bin/bash
# Install Shipment Tracking Tool as a systemd service
# This script creates a systemd service that runs the shipment tracker in the background

# Check if running as root
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root (use sudo)"
  exit 1
fi

# Get the current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "Installing service from directory: $SCRIPT_DIR"

# Get the user who ran sudo
if [ -n "$SUDO_USER" ]; then
  ACTUAL_USER="$SUDO_USER"
else
  ACTUAL_USER="$(whoami)"
fi
echo "Service will run as user: $ACTUAL_USER"

# Create the service file
SERVICE_FILE="/etc/systemd/system/shipment-tracker.service"
echo "Creating service file: $SERVICE_FILE"

cat > "$SERVICE_FILE" << EOL
[Unit]
Description=Shipment Tracking Tool
After=network.target

[Service]
Type=simple
User=$ACTUAL_USER
WorkingDirectory=$SCRIPT_DIR
ExecStart=/usr/bin/python3 $SCRIPT_DIR/shipment_tracker.py
Restart=on-failure
RestartSec=10
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=shipment-tracker

[Install]
WantedBy=multi-user.target
EOL

# Set permissions
chmod 644 "$SERVICE_FILE"

# Reload systemd
systemctl daemon-reload

# Enable the service to start on boot
systemctl enable shipment-tracker.service

echo "Service installed successfully!"
echo "You can now manage the service with these commands:"
echo "  sudo systemctl start shipment-tracker.service   # Start the service"
echo "  sudo systemctl stop shipment-tracker.service    # Stop the service"
echo "  sudo systemctl restart shipment-tracker.service # Restart the service"
echo "  sudo systemctl status shipment-tracker.service  # Check service status"
echo "  sudo journalctl -u shipment-tracker.service     # View service logs"
echo ""
echo "The service is set to start automatically on system boot."
echo "To start it now, run: sudo systemctl start shipment-tracker.service"
