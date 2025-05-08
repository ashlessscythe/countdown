"""
Environment Checker for Shipment Tracking Tool

This script checks if the system meets all requirements for running the shipment tracking tool.
It verifies Python version, required packages, and directory access.
"""

import sys
import os
import importlib
import platform
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def check_python_version():
    """Check if Python version is 3.7+"""
    required_version = (3, 7)
    current_version = sys.version_info[:2]
    
    logging.info(f"Checking Python version: {sys.version}")
    if current_version >= required_version:
        logging.info(f"✓ Python version {current_version[0]}.{current_version[1]} meets requirements")
        return True
    else:
        logging.error(f"✗ Python version {current_version[0]}.{current_version[1]} does not meet minimum requirement of 3.7")
        return False

def check_package(package_name, min_version=None):
    """Check if a package is installed and meets minimum version if specified"""
    try:
        module = importlib.import_module(package_name)
        if hasattr(module, '__version__'):
            version = module.__version__
        elif hasattr(module, 'version'):
            version = module.version
        else:
            version = "Unknown"
        
        if min_version:
            # Simple version comparison (assumes semantic versioning)
            installed_parts = [int(x) for x in version.split('.')]
            required_parts = [int(x) for x in min_version.split('.')]
            
            # Pad with zeros if needed
            while len(installed_parts) < len(required_parts):
                installed_parts.append(0)
            
            meets_requirement = installed_parts >= required_parts
            if meets_requirement:
                logging.info(f"✓ {package_name} version {version} meets requirement (>= {min_version})")
                return True
            else:
                logging.error(f"✗ {package_name} version {version} does not meet requirement (>= {min_version})")
                return False
        else:
            logging.info(f"✓ {package_name} is installed (version {version})")
            return True
    except ImportError:
        logging.error(f"✗ {package_name} is not installed")
        return False
    except Exception as e:
        logging.error(f"✗ Error checking {package_name}: {e}")
        return False

def check_directory_access(directory):
    """Check if a directory exists and is accessible"""
    path = Path(directory)
    
    if not path.exists():
        try:
            path.mkdir(parents=True)
            logging.info(f"✓ Created directory: {directory}")
            return True
        except Exception as e:
            logging.error(f"✗ Cannot create directory {directory}: {e}")
            return False
    
    # Check if directory is readable and writable
    if os.access(path, os.R_OK | os.W_OK):
        logging.info(f"✓ Directory {directory} is accessible (read/write)")
        return True
    else:
        logging.error(f"✗ Directory {directory} is not accessible (read/write permissions required)")
        return False

def check_config():
    """Check if config.py exists and can be imported"""
    if not Path('config.py').exists():
        logging.error("✗ config.py not found. Please create it from config.py.example")
        return False
    
    try:
        import config
        logging.info("✓ config.py imported successfully")
        
        # Check required config variables
        required_vars = ['BASE_DIR', 'SERIAL_NUMBERS_DIR', 'DELIVERY_INFO_DIR', 'OUT_DIR', 'WAREHOUSE_FILTER', 'STATUS_MAPPING', 'INTERVAL_SECONDS', 'WINDOW_MINUTES']
        missing_vars = [var for var in required_vars if not hasattr(config, var)]
        
        if missing_vars:
            logging.error(f"✗ Missing required variables in config.py: {', '.join(missing_vars)}")
            return False
        
        logging.info("✓ All required configuration variables are present")
        return True
    except Exception as e:
        logging.error(f"✗ Error importing config.py: {e}")
        return False

def main():
    """Run all environment checks"""
    logging.info("Starting environment check for Shipment Tracking Tool")
    logging.info(f"System: {platform.system()} {platform.release()}")
    
    checks = []
    
    # Check Python version
    checks.append(check_python_version())
    
    # Check required packages
    checks.append(check_package('pandas', '1.3.0'))
    checks.append(check_package('pyarrow', '7.0.0'))
    checks.append(check_package('matplotlib', '3.5.0'))
    
    # Check config
    config_ok = check_config()
    checks.append(config_ok)
    
    # If config is OK, check directories
    if config_ok:
        try:
            import config
            checks.append(check_directory_access(config.BASE_DIR))
            checks.append(check_directory_access(config.SERIAL_NUMBERS_DIR))
            checks.append(check_directory_access(config.DELIVERY_INFO_DIR))
            checks.append(check_directory_access(config.OUT_DIR))
        except Exception as e:
            logging.error(f"✗ Error checking directories: {e}")
            checks.append(False)
    
    # Check visualizations directory
    checks.append(check_directory_access('visualizations'))
    
    # Summary
    total_checks = len(checks)
    passed_checks = sum(checks)
    
    logging.info(f"\nEnvironment check summary: {passed_checks}/{total_checks} checks passed")
    
    if all(checks):
        logging.info("✓ All checks passed! The environment is ready to run the shipment tracking tool.")
        return 0
    else:
        logging.error("✗ Some checks failed. Please fix the issues above before running the tool.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
