"""
Data caching system for quick dashboard updates.
"""
import os
import json
import time
import logging
import threading
from datetime import datetime
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config import OUT_DIR

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DashboardCache:
    """
    Cache for dashboard data to provide quick access without reprocessing.
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """
        Singleton pattern to ensure only one cache instance exists.
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DashboardCache, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """
        Initialize the cache.
        """
        if self._initialized:
            return
        
        self._cache = {}
        self._last_updated = {}
        self._cache_file = os.path.join(OUT_DIR, "dashboard_cache.json")
        self._initialized = True
        self._lock = threading.RLock()
        
        # Load cache from disk if available
        self._load_cache()
    
    def _load_cache(self):
        """
        Load cache from disk.
        """
        try:
            if os.path.exists(self._cache_file):
                with open(self._cache_file, 'r') as f:
                    cache_data = json.load(f)
                    self._cache = cache_data.get('data', {})
                    self._last_updated = cache_data.get('last_updated', {})
                    logger.info(f"Loaded cache from {self._cache_file}")
        except Exception as e:
            logger.error(f"Error loading cache: {str(e)}")
    
    def _save_cache(self):
        """
        Save cache to disk.
        """
        try:
            cache_data = {
                'data': self._cache,
                'last_updated': self._last_updated
            }
            
            with open(self._cache_file, 'w') as f:
                json.dump(cache_data, f, default=str, indent=2)
                logger.info(f"Saved cache to {self._cache_file}")
        except Exception as e:
            logger.error(f"Error saving cache: {str(e)}")
    
    def get(self, key, default=None):
        """
        Get a value from the cache.
        
        Args:
            key (str): Cache key
            default: Default value to return if key not found
            
        Returns:
            The cached value or default
        """
        with self._lock:
            return self._cache.get(key, default)
    
    def set(self, key, value):
        """
        Set a value in the cache.
        
        Args:
            key (str): Cache key
            value: Value to cache
        """
        with self._lock:
            self._cache[key] = value
            self._last_updated[key] = datetime.now().isoformat()
            self._save_cache()
    
    def update(self, data):
        """
        Update multiple values in the cache.
        
        Args:
            data (dict): Dictionary of key-value pairs to update
        """
        with self._lock:
            for key, value in data.items():
                self._cache[key] = value
                self._last_updated[key] = datetime.now().isoformat()
            self._save_cache()
    
    def delete(self, key):
        """
        Delete a value from the cache.
        
        Args:
            key (str): Cache key
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                if key in self._last_updated:
                    del self._last_updated[key]
                self._save_cache()
    
    def clear(self):
        """
        Clear the entire cache.
        """
        with self._lock:
            self._cache = {}
            self._last_updated = {}
            self._save_cache()
    
    def get_last_updated(self, key):
        """
        Get the last updated timestamp for a key.
        
        Args:
            key (str): Cache key
            
        Returns:
            str: ISO format timestamp or None if key not found
        """
        with self._lock:
            return self._last_updated.get(key)
    
    def is_stale(self, key, max_age_seconds):
        """
        Check if a cached value is stale.
        
        Args:
            key (str): Cache key
            max_age_seconds (int): Maximum age in seconds
            
        Returns:
            bool: True if the value is stale or not found, False otherwise
        """
        with self._lock:
            if key not in self._last_updated:
                return True
            
            try:
                last_updated = datetime.fromisoformat(self._last_updated[key])
                age = (datetime.now() - last_updated).total_seconds()
                return age > max_age_seconds
            except Exception:
                return True
    
    def get_all(self):
        """
        Get all cached data.
        
        Returns:
            dict: All cached data
        """
        with self._lock:
            return self._cache.copy()
    
    def get_dashboard_data(self):
        """
        Get the cached dashboard data.
        
        Returns:
            dict: Dashboard data or empty dict if not cached
        """
        return self.get('dashboard_data', {})
    
    def set_dashboard_data(self, dashboard_data):
        """
        Set the dashboard data in the cache.
        
        Args:
            dashboard_data (dict): Dashboard data to cache
        """
        self.set('dashboard_data', dashboard_data)
    
    def get_section_data(self, section):
        """
        Get a specific section of the dashboard data.
        
        Args:
            section (str): Section name (e.g., 'users', 'deliveries')
            
        Returns:
            list: Section data or empty list if not found
        """
        dashboard_data = self.get_dashboard_data()
        return dashboard_data.get(section, [])
    
    def set_section_data(self, section, data):
        """
        Set a specific section of the dashboard data.
        
        Args:
            section (str): Section name (e.g., 'users', 'deliveries')
            data (list): Section data
        """
        dashboard_data = self.get_dashboard_data()
        dashboard_data[section] = data
        self.set_dashboard_data(dashboard_data)

# Create a global cache instance
dashboard_cache = DashboardCache()
