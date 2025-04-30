/**
 * Utility functions for the Delivery Analysis Dashboard
 */

/**
 * Format a timestamp as a human-readable time since change
 * @param {string} timestamp_str - ISO format timestamp
 * @returns {string} Human-readable time since change
 */
function getTimeSinceChange(timestamp_str) {
  if (!timestamp_str) {
    return "Unknown";
  }

  try {
    // Parse the ISO format timestamp
    const timestamp = new Date(timestamp_str);
    const now = new Date();

    // Calculate the difference
    const diff = now - timestamp;
    const diffInSeconds = Math.floor(diff / 1000);
    const diffInMinutes = Math.floor(diffInSeconds / 60);
    const diffInHours = Math.floor(diffInMinutes / 60);
    const diffInDays = Math.floor(diffInHours / 24);

    // Format the difference
    if (diffInDays > 0) {
      return `${diffInDays} days ago`;
    } else if (diffInHours > 0) {
      return `${diffInHours} hours ago`;
    } else if (diffInMinutes > 0) {
      return `${diffInMinutes} minutes ago`;
    } else {
      return "Just now";
    }
  } catch (e) {
    console.error("Error parsing time:", e);
    return "Error parsing time";
  }
}

/**
 * Sort changes by timestamp (most recent first)
 * @param {Array} changes - Array of change objects
 * @returns {Array} Sorted array of change objects
 */
function sortByTimestamp(changes) {
  return changes.sort((a, b) => {
    const dateA = a.timestamp ? new Date(a.timestamp) : new Date(0);
    const dateB = b.timestamp ? new Date(b.timestamp) : new Date(0);
    return dateB - dateA;
  });
}

/**
 * Sort changes by serial
 * @param {Array} changes - Array of change objects
 * @returns {Array} Sorted array of change objects
 */
function sortBySerial(changes) {
  return changes.sort((a, b) => (a.serial || "").localeCompare(b.serial || ""));
}

/**
 * Show error message in a table
 * @param {string} tableId - ID of the table body element
 * @param {number} colSpan - Number of columns in the table
 * @param {string} message - Error message to display
 */
function showTableError(tableId, colSpan, message) {
  document.getElementById(
    tableId
  ).innerHTML = `<tr><td colspan="${colSpan}" class="text-center">${message}</td></tr>`;
}

/**
 * Show loading message in a table
 * @param {string} tableId - ID of the table body element
 * @param {number} colSpan - Number of columns in the table
 */
function showTableLoading(tableId, colSpan) {
  document.getElementById(
    tableId
  ).innerHTML = `<tr><td colspan="${colSpan}" class="text-center">Loading data...</td></tr>`;
}
