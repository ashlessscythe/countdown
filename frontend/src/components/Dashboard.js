import React, { useState, useEffect, useCallback } from "react";
import UserShipmentInfo from "./UserShipmentInfo";
import ProgressIndicator from "./ProgressIndicator";
import ScanTimeDisplay from "./ScanTimeDisplay";
import {
  getDashboardData,
  getUserActivity,
  getDeliveryProgress,
  getScanTimes,
  connectWebSocket,
} from "../services/api";

/**
 * Main Dashboard component that integrates all dashboard elements
 */
const Dashboard = () => {
  // State for dashboard data
  const [dashboardData, setDashboardData] = useState({
    users: [],
    deliveries: [],
    progress: [],
    scan_times: [],
  });

  // State for selected user and delivery
  const [selectedUser, setSelectedUser] = useState(null);
  const [selectedDelivery, setSelectedDelivery] = useState(null);

  // State for scan times
  const [scanTimes, setScanTimes] = useState({
    current: null,
    previous: null,
  });

  // State for auto-refresh
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);

  // State for loading status
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Function to load all dashboard data
  const loadDashboardData = useCallback(async () => {
    try {
      setLoading(true);
      const data = await getDashboardData();
      setDashboardData(data);

      // Set the first user as selected if none is selected
      if (!selectedUser && data.users && data.users.length > 0) {
        setSelectedUser(data.users[0]);
      }

      // Set the first delivery as selected if none is selected
      if (!selectedDelivery && data.deliveries && data.deliveries.length > 0) {
        setSelectedDelivery(data.deliveries[0]);
      }

      setLastUpdated(new Date());
      setError(null);
    } catch (err) {
      console.error("Error loading dashboard data:", err);
      setError("Failed to load dashboard data. Please try again.");
    } finally {
      setLoading(false);
    }
  }, [selectedUser, selectedDelivery]);

  // Function to load scan times for the selected user
  const loadScanTimes = useCallback(async () => {
    if (!selectedUser) return;

    try {
      const userId = selectedUser.user_id;
      const scanTimeData = await getScanTimes(userId);

      if (scanTimeData && scanTimeData.length > 0) {
        // Sort scan times by timestamp (newest first)
        const sortedScanTimes = [...scanTimeData].sort((a, b) => {
          return new Date(b.timestamp) - new Date(a.timestamp);
        });

        setScanTimes({
          current: sortedScanTimes[0] || null,
          previous: sortedScanTimes[1] || null,
        });
      }
    } catch (err) {
      console.error("Error loading scan times:", err);
    }
  }, [selectedUser]);

  // Function to handle WebSocket messages
  const handleWebSocketMessage = useCallback((data) => {
    if (data.type === "initial") {
      setDashboardData(data.data);
    } else if (data.type === "update") {
      // Update dashboard data with the received updates
      setDashboardData((prevData) => {
        const newData = { ...prevData };

        // Update each section with new data if available
        Object.keys(data.data.updates || {}).forEach((section) => {
          if (newData[section]) {
            newData[section] = data.data.updates[section];
          }
        });

        return newData;
      });

      setLastUpdated(new Date());
    }
  }, []);

  // Effect to load initial dashboard data
  useEffect(() => {
    loadDashboardData();

    // Set up WebSocket connection for real-time updates
    let socket;
    if (autoRefresh) {
      socket = connectWebSocket(handleWebSocketMessage, (error) =>
        console.error("WebSocket error:", error)
      );
    }

    // Clean up WebSocket connection on unmount
    return () => {
      if (socket) {
        socket.close();
      }
    };
  }, [loadDashboardData, autoRefresh, handleWebSocketMessage]);

  // Effect to load scan times when selected user changes
  useEffect(() => {
    loadScanTimes();
  }, [loadScanTimes, selectedUser]);

  // Function to handle user selection change
  const handleUserChange = (event) => {
    const userId = event.target.value;
    const user = dashboardData.users.find((u) => u.user_id === userId);
    setSelectedUser(user || null);
  };

  // Function to handle delivery selection change
  const handleDeliveryChange = (event) => {
    const deliveryId = event.target.value;
    const delivery = dashboardData.deliveries.find(
      (d) => d.delivery_id === deliveryId
    );
    setSelectedDelivery(delivery || null);
  };

  // Function to toggle auto-refresh
  const toggleAutoRefresh = () => {
    setAutoRefresh(!autoRefresh);
  };

  // Function to manually refresh data
  const handleManualRefresh = () => {
    loadDashboardData();
    loadScanTimes();
  };

  // Calculate progress data for the selected delivery
  const getProgressData = () => {
    if (!selectedDelivery || !dashboardData.progress)
      return { current: 0, total: 0 };

    const progressData = dashboardData.progress.find(
      (p) => p.delivery === selectedDelivery.delivery_id
    );

    if (!progressData) return { current: 0, total: 0 };

    return {
      current: progressData.completed || 0,
      total: progressData.total || 0,
    };
  };

  // Format the last updated time
  const formatLastUpdated = () => {
    if (!lastUpdated) return "Never";
    return lastUpdated.toLocaleTimeString();
  };

  // Get progress data
  const progressData = getProgressData();

  return (
    <div className="dashboard-container">
      <div className="dashboard-header">
        <div className="d-flex justify-content-between align-items-center">
          <h1>
            <i className="fas fa-truck-loading me-2"></i> Delivery Dashboard
          </h1>
          <div>
            <button
              className="btn btn-outline-light me-2"
              onClick={handleManualRefresh}
              disabled={loading}
            >
              <i className="fas fa-sync-alt me-1"></i> Refresh
            </button>
            <button
              className={`btn ${
                autoRefresh ? "btn-success" : "btn-outline-light"
              }`}
              onClick={toggleAutoRefresh}
            >
              <i className="fas fa-clock me-1"></i>{" "}
              {autoRefresh ? "Auto-Refresh On" : "Auto-Refresh Off"}
            </button>
          </div>
        </div>
        <div className="text-light mt-2">
          <small>
            Last updated: {formatLastUpdated()}
            {autoRefresh && (
              <span className="refresh-indicator ms-2">
                <i className="fas fa-circle text-success me-1"></i> Live
              </span>
            )}
          </small>
        </div>
      </div>

      {error && (
        <div className="alert alert-danger" role="alert">
          <i className="fas fa-exclamation-triangle me-2"></i> {error}
        </div>
      )}

      {loading ? (
        <div className="text-center my-5">
          <div className="spinner-border text-primary" role="status">
            <span className="visually-hidden">Loading...</span>
          </div>
          <p className="mt-2">Loading dashboard data...</p>
        </div>
      ) : (
        <>
          <div className="row mb-4">
            <div className="col-md-6">
              <div className="form-group">
                <label htmlFor="userSelect">Select User:</label>
                <select
                  id="userSelect"
                  className="form-select"
                  value={selectedUser?.user_id || ""}
                  onChange={handleUserChange}
                >
                  <option value="">Select a user</option>
                  {dashboardData.users &&
                    dashboardData.users.map((user) => (
                      <option key={user.user_id} value={user.user_id}>
                        {user.user_id} - {user.name || "Unknown"}
                      </option>
                    ))}
                </select>
              </div>
            </div>
            <div className="col-md-6">
              <div className="form-group">
                <label htmlFor="deliverySelect">Select Delivery:</label>
                <select
                  id="deliverySelect"
                  className="form-select"
                  value={selectedDelivery?.delivery_id || ""}
                  onChange={handleDeliveryChange}
                >
                  <option value="">Select a delivery</option>
                  {dashboardData.deliveries &&
                    dashboardData.deliveries.map((delivery) => (
                      <option
                        key={delivery.delivery_id}
                        value={delivery.delivery_id}
                      >
                        {delivery.delivery_id} -{" "}
                        {delivery.description || "No description"}
                      </option>
                    ))}
                </select>
              </div>
            </div>
          </div>

          {selectedUser && (
            <UserShipmentInfo
              userId={selectedUser.user_id}
              shipmentId={selectedDelivery?.delivery_id}
              lastActivity={selectedUser.last_activity}
              lastActivityType={selectedUser.last_activity_type}
            />
          )}

          <div className="row">
            <div className="col-md-6">
              <div className="dashboard-card">
                <h4>Delivery Progress</h4>
                <ProgressIndicator
                  current={progressData.current}
                  total={progressData.total}
                  label="Items Processed"
                  unit="items"
                />
              </div>
            </div>
            <div className="col-md-6">
              <div className="dashboard-card">
                <h4>Scan Times</h4>
                <ScanTimeDisplay
                  currentScan={scanTimes.current}
                  previousScan={scanTimes.previous}
                />
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
};

export default Dashboard;
