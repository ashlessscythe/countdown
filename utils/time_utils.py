from datetime import datetime

def get_time_since_change(timestamp_str):
    """Calculate time since last change"""
    if not timestamp_str:
        return "Unknown"
    
    try:
        # Parse the ISO format timestamp
        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        now = datetime.now().astimezone()
        
        # Calculate the difference
        diff = now - timestamp
        
        # Format the difference
        if diff.days > 0:
            return f"{diff.days} days ago"
        elif diff.seconds // 3600 > 0:
            return f"{diff.seconds // 3600} hours ago"
        elif diff.seconds // 60 > 0:
            return f"{diff.seconds // 60} minutes ago"
        else:
            return "Just now"
    except Exception as e:
        return "Error parsing time"
