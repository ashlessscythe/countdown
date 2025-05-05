import React from "react";

/**
 * Component to display scan time information (current and previous)
 *
 * @param {Object} props
 * @param {Object} props.currentScan - Current scan information
 * @param {Object} props.previousScan - Previous scan information
 */
const ScanTimeDisplay = ({ currentScan, previousScan }) => {
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

  // Calculate time difference in seconds
  const calculateTimeDifference = (current, previous) => {
    if (!current || !previous) return null;

    try {
      const currentDate = new Date(current);
      const previousDate = new Date(previous);
      const diffInSeconds = Math.round((currentDate - previousDate) / 1000);

      if (diffInSeconds < 60) {
        return `${diffInSeconds} seconds`;
      } else if (diffInSeconds < 3600) {
        const minutes = Math.floor(diffInSeconds / 60);
        const seconds = diffInSeconds % 60;
        return `${minutes} min ${seconds} sec`;
      } else {
        const hours = Math.floor(diffInSeconds / 3600);
        const minutes = Math.floor((diffInSeconds % 3600) / 60);
        return `${hours} hr ${minutes} min`;
      }
    } catch (error) {
      console.error("Error calculating time difference:", error);
      return "N/A";
    }
  };

  // Get time difference between current and previous scan
  const timeDifference = calculateTimeDifference(
    currentScan?.timestamp,
    previousScan?.timestamp
  );

  return (
    <div className="scan-time-display">
      <div className="scan-time-card current">
        <h5>
          <i className="fas fa-clock me-2"></i> Current Scan
        </h5>
        <p className="mb-1">
          <strong>Time:</strong> {formatTimestamp(currentScan?.timestamp)}
        </p>
        <p className="mb-1">
          <strong>Serial:</strong> {currentScan?.serial || "N/A"}
        </p>
        <p className="mb-1">
          <strong>Status:</strong> {currentScan?.status || "N/A"}
        </p>
      </div>

      <div className="scan-time-card previous">
        <h5>
          <i className="fas fa-history me-2"></i> Previous Scan
        </h5>
        <p className="mb-1">
          <strong>Time:</strong> {formatTimestamp(previousScan?.timestamp)}
        </p>
        <p className="mb-1">
          <strong>Serial:</strong> {previousScan?.serial || "N/A"}
        </p>
        <p className="mb-1">
          <strong>Status:</strong> {previousScan?.status || "N/A"}
        </p>
      </div>

      {timeDifference && (
        <div className="scan-time-card">
          <h5>
            <i className="fas fa-stopwatch me-2"></i> Time Between Scans
          </h5>
          <p className="display-6 text-center">{timeDifference}</p>
        </div>
      )}
    </div>
  );
};

export default ScanTimeDisplay;
