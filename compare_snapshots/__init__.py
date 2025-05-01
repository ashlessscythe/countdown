"""
Compare Snapshots Package

This package provides functionality to analyze Excel snapshots of shipment data,
track changes over time, and generate metrics for dashboard visualization.

The package follows a modular structure:
- main.py: Entry point and main loop functionality
- io_utils.py: File operations (loading snapshots, listing files)
- diff_utils.py: Data processing and metrics generation
"""

from .main import run_loop
