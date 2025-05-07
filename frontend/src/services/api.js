import axios from "axios";

// Create axios instance with direct backend URL
const api = axios.create({
  baseURL: "http://localhost:8080/api",
  timeout: 30000, // Increased timeout for large responses
  headers: {
    "Content-Type": "application/json",
  },
});

// Get all dashboard data
export const getDashboardData = async () => {
  try {
    const response = await api.get("/dashboard");
    return response.data;
  } catch (error) {
    console.error("Error fetching dashboard data:", error);
    throw error;
  }
};

// Get user activity data
export const getUserActivity = async (activeOnly = false) => {
  try {
    const response = await api.get(`/users?active_only=${activeOnly}`);
    return response.data;
  } catch (error) {
    console.error("Error fetching user activity:", error);
    throw error;
  }
};

// Get delivery progress data
export const getDeliveryProgress = async (deliveryId = null, userId = null) => {
  try {
    let url = "/progress";
    const params = new URLSearchParams();

    if (deliveryId) params.append("delivery_id", deliveryId);
    if (userId) params.append("user_id", userId);

    const queryString = params.toString();
    if (queryString) url += `?${queryString}`;

    const response = await api.get(url);
    return response.data;
  } catch (error) {
    console.error("Error fetching delivery progress:", error);
    throw error;
  }
};

// Get scan time data
export const getScanTimes = async (userId = null) => {
  try {
    let url = "/scan-times";
    if (userId) url += `?user_id=${userId}`;

    const response = await api.get(url);
    return response.data;
  } catch (error) {
    console.error("Error fetching scan times:", error);
    throw error;
  }
};

// Track user activity
export const trackUserActivity = async (userId, activityType) => {
  try {
    const response = await api.post(
      `/track-activity?user_id=${userId}&activity_type=${activityType}`
    );
    return response.data;
  } catch (error) {
    console.error("Error tracking user activity:", error);
    throw error;
  }
};

// WebSocket connection for real-time updates
export const connectWebSocket = (onMessage, onError) => {
  // Connect directly to the backend WebSocket endpoint
  // This bypasses the proxy for WebSocket connections
  const wsUrl = "ws://localhost:8080/api/ws";

  const socket = new WebSocket(wsUrl);

  socket.onopen = () => {
    console.log("WebSocket connection established");
  };

  socket.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      onMessage(data);
    } catch (error) {
      console.error("Error parsing WebSocket message:", error);
    }
  };

  socket.onerror = (error) => {
    console.error("WebSocket error:", error);
    if (onError) onError(error);
  };

  socket.onclose = () => {
    console.log("WebSocket connection closed");
  };

  // Return the socket so it can be closed later if needed
  return socket;
};

export default {
  getDashboardData,
  getUserActivity,
  getDeliveryProgress,
  getScanTimes,
  trackUserActivity,
  connectWebSocket,
};
