import React from "react";

/**
 * Component to display user ID and shipment information
 *
 * @param {Object} props
 * @param {string} props.userId - The user ID
 * @param {string} props.shipmentId - The shipment ID
 * @param {string} props.lastActivity - Timestamp of the last activity
 * @param {string} props.lastActivityType - Type of the last activity
 */
const UserShipmentInfo = ({
  userId,
  shipmentId,
  lastActivity,
  lastActivityType,
}) => {
  // Format the timestamp for display
  const formatTimestamp = (timestamp) => {
    if (!timestamp) return "N/A";

    try {
      const date = new Date(timestamp);
      return date.toLocaleString();
    } catch (error) {
      console.error("Error formatting timestamp:", error);
      return "Invalid date";
    }
  };

  return (
    <div className="user-shipment-info">
      <div className="row">
        <div className="col-md-6">
          <div className="card mb-3">
            <div className="card-header bg-primary text-white">
              <i className="fas fa-user me-2"></i> User Information
            </div>
            <div className="card-body">
              <h5 className="card-title">User ID: {userId || "N/A"}</h5>
              <p className="card-text">
                <strong>Last Activity:</strong> {formatTimestamp(lastActivity)}
              </p>
              <p className="card-text">
                <strong>Activity Type:</strong> {lastActivityType || "N/A"}
              </p>
            </div>
          </div>
        </div>
        <div className="col-md-6">
          <div className="card mb-3">
            <div className="card-header bg-info text-white">
              <i className="fas fa-shipping-fast me-2"></i> Shipment Information
            </div>
            <div className="card-body">
              <h5 className="card-title">Shipment ID: {shipmentId || "N/A"}</h5>
              <p className="card-text">
                <strong>Status:</strong> Active
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default UserShipmentInfo;
