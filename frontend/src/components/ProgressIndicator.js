import React from "react";

/**
 * Component to display progress indicators (e.g., "30 out of 75")
 *
 * @param {Object} props
 * @param {number} props.current - Current progress value
 * @param {number} props.total - Total target value
 * @param {string} props.label - Label for the progress indicator
 * @param {string} props.unit - Unit of measurement (optional)
 */
const ProgressIndicator = ({ current, total, label, unit = "" }) => {
  // Calculate percentage for the progress bar
  const percentage = total > 0 ? Math.round((current / total) * 100) : 0;

  // Determine color based on percentage
  const getColorClass = () => {
    if (percentage < 25) return "bg-danger";
    if (percentage < 50) return "bg-warning";
    if (percentage < 75) return "bg-info";
    return "bg-success";
  };

  return (
    <div className="progress-indicator">
      <h5>{label}</h5>
      <div className="d-flex justify-content-between">
        <span>
          <strong>
            {current} {unit}
          </strong>{" "}
          out of{" "}
          <strong>
            {total} {unit}
          </strong>
        </span>
        <span className="badge bg-secondary">{percentage}%</span>
      </div>
      <div className="progress-bar-container">
        <div
          className={`progress-bar ${getColorClass()}`}
          role="progressbar"
          style={{ width: `${percentage}%` }}
          aria-valuenow={percentage}
          aria-valuemin="0"
          aria-valuemax="100"
        ></div>
      </div>
    </div>
  );
};

export default ProgressIndicator;
