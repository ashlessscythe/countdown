"""
API endpoints for dashboard data.
"""
from fastapi import APIRouter, HTTPException, Depends, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
import logging
import asyncio
from datetime import datetime, timedelta
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from backend.api.services import (
    get_dashboard_data, 
    get_user_activity, 
    get_delivery_progress, 
    get_scan_times,
    track_user_activity,
    get_real_time_updates
)
from config import FRONTEND_UPDATE_INTERVAL

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api", tags=["dashboard"])

# WebSocket connections manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket client disconnected. Remaining connections: {len(self.active_connections)}")
    
    async def broadcast(self, message: Dict[str, Any]):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to WebSocket: {str(e)}")

manager = ConnectionManager()

@router.get("/dashboard")
async def dashboard():
    """
    Get all dashboard data.
    
    Returns:
        dict: Complete dashboard data
    """
    try:
        data = get_dashboard_data()
        return JSONResponse(content=data)
    except Exception as e:
        logger.error(f"Error retrieving dashboard data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/users")
async def users(active_only: bool = Query(False)):
    """
    Get user activity data.
    
    Args:
        active_only (bool): If True, return only active users
        
    Returns:
        list: User activity data
    """
    try:
        data = get_user_activity(active_only)
        return JSONResponse(content=data)
    except Exception as e:
        logger.error(f"Error retrieving user activity data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/progress")
async def progress(delivery_id: Optional[str] = None, user_id: Optional[str] = None):
    """
    Get delivery progress data.
    
    Args:
        delivery_id (str, optional): Filter by delivery ID
        user_id (str, optional): Filter by user ID
        
    Returns:
        list: Delivery progress data
    """
    try:
        data = get_delivery_progress(delivery_id, user_id)
        return JSONResponse(content=data)
    except Exception as e:
        logger.error(f"Error retrieving delivery progress data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/scan-times")
async def scan_times(user_id: Optional[str] = None):
    """
    Get scan time data.
    
    Args:
        user_id (str, optional): Filter by user ID
        
    Returns:
        list: Scan time data
    """
    try:
        data = get_scan_times(user_id)
        return JSONResponse(content=data)
    except Exception as e:
        logger.error(f"Error retrieving scan time data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/track-activity")
async def track_activity(user_id: str, activity_type: str):
    """
    Track user activity.
    
    Args:
        user_id (str): User ID
        activity_type (str): Type of activity (e.g., 'scan', 'view')
        
    Returns:
        dict: Confirmation message
    """
    try:
        track_user_activity(user_id, activity_type)
        return {"message": f"Activity {activity_type} tracked for user {user_id}"}
    except Exception as e:
        logger.error(f"Error tracking user activity: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time updates.
    """
    await manager.connect(websocket)
    try:
        # Send initial data
        initial_data = get_dashboard_data()
        await websocket.send_json({"type": "initial", "data": initial_data})
        
        # Start sending real-time updates
        while True:
            # Get updates
            updates = get_real_time_updates()
            
            # Send updates if there are any
            if updates:
                await websocket.send_json({"type": "update", "data": updates})
            
            # Wait for the next update interval
            await asyncio.sleep(FRONTEND_UPDATE_INTERVAL)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")
        manager.disconnect(websocket)

# Broadcast updates to all connected clients
async def broadcast_updates():
    """
    Broadcast updates to all connected WebSocket clients.
    """
    while True:
        try:
            # Get updates
            updates = get_real_time_updates()
            
            # Broadcast updates if there are any and if there are active connections
            if updates and manager.active_connections:
                await manager.broadcast({"type": "update", "data": updates})
        except Exception as e:
            logger.error(f"Error broadcasting updates: {str(e)}")
        
        # Wait for the next update interval
        await asyncio.sleep(FRONTEND_UPDATE_INTERVAL)
