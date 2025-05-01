/**
 * Dashboard JavaScript for SAP Snapshot Comparison Dashboard
 * Updated to work with Parquet data
 */

// Global variables
let dashboardData = null;
let updateInterval = 60000; // Default update interval: 60 seconds

// DOM elements
const refreshBtn = document.getElementById("refresh-btn");
const lastUpdateTimeEl = document.getElementById("last-update-time");
const filesComparedEl = document.getElementById("files-compared");
const commonSerialsEl = document.getElementById("common-serials");
const totalUsersEl = document.getElementById("total-users");
const avgTimeEl = document.getElementById("avg-time");
const warehouseFilterEl = document.getElementById("warehouse-filter");
const scanDataTableEl = document.getElementById("scan-data-table");

// Chart containers
const statusSummaryChartEl = document.getElementById("status-summary-chart");
const userActivityChartEl = document.getElementById("user-activity-chart");
const statusByDeliveryChartEl = document.getElementById(
  "status-by-delivery-chart"
);
const shipmentTreeChartEl = document.getElementById("shipment-tree-chart");

/**
 * Initialize the dashboard
 */
function initDashboard() {
  // Add event listeners
  refreshBtn.addEventListener("click", refreshData);

  // Load initial data
  loadData();

  // Set up auto-refresh
  setInterval(refreshData, updateInterval);
}

/**
 * Display error message in a chart container
 */
function displayChartError(container, message) {
  container.innerHTML = `
    <div class="alert alert-warning text-center" role="alert">
      <i class="bi bi-exclamation-triangle-fill me-2"></i>${message}
    </div>
  `;
}

/**
 * Load data from the API
 */
async function loadData() {
  try {
    // Show loading indicators
    showLoadingState();

    const response = await fetch("/api/data");

    // Check if response is ok (status in the range 200-299)
    if (!response.ok) {
      throw new Error(
        `Server returned ${response.status}: ${response.statusText}`
      );
    }

    const data = await response.json();

    if (data.data && Object.keys(data.data).length > 0) {
      dashboardData = data.data;
      updateLastUpdateTime(data.last_update_time);
      updateDashboard(data.metadata);
    } else {
      console.error("No data available from API");
      displayNoDataMessage();
    }
  } catch (error) {
    console.error("Error loading data:", error);
    displayErrorMessage(error.message);
  }
}

/**
 * Show loading state for all dashboard elements
 */
function showLoadingState() {
  // Update summary cards with loading indicators
  filesComparedEl.innerHTML =
    '<div class="spinner-border spinner-border-sm" role="status"></div>';
  commonSerialsEl.innerHTML =
    '<div class="spinner-border spinner-border-sm" role="status"></div>';
  totalUsersEl.innerHTML =
    '<div class="spinner-border spinner-border-sm" role="status"></div>';
  avgTimeEl.innerHTML =
    '<div class="spinner-border spinner-border-sm" role="status"></div>';
  warehouseFilterEl.innerHTML =
    '<div class="spinner-border spinner-border-sm" role="status"></div>';

  // Show loading indicators in chart containers
  statusSummaryChartEl.innerHTML =
    '<div class="d-flex justify-content-center align-items-center h-100"><div class="spinner-border" role="status"></div></div>';
  userActivityChartEl.innerHTML =
    '<div class="d-flex justify-content-center align-items-center h-100"><div class="spinner-border" role="status"></div></div>';
  statusByDeliveryChartEl.innerHTML =
    '<div class="d-flex justify-content-center align-items-center h-100"><div class="spinner-border" role="status"></div></div>';
  shipmentTreeChartEl.innerHTML =
    '<div class="d-flex justify-content-center align-items-center h-100"><div class="spinner-border" role="status"></div></div>';

  // Clear table and show loading indicator
  const tbody = scanDataTableEl.querySelector("tbody");
  tbody.innerHTML = `
    <tr>
      <td colspan="9" class="text-center">
        <div class="spinner-border" role="status"></div>
        <span class="ms-2">Loading data...</span>
      </td>
    </tr>
  `;
}

/**
 * Display no data message when API returns empty data
 */
function displayNoDataMessage() {
  // Update summary cards
  filesComparedEl.textContent = "No files processed";
  commonSerialsEl.textContent = "No data";
  totalUsersEl.textContent = "No data";
  avgTimeEl.textContent = "No data";
  warehouseFilterEl.textContent = "Default window";

  // Show no data message in chart containers
  displayChartError(statusSummaryChartEl, "No status summary data available");
  displayChartError(userActivityChartEl, "No user activity data available");
  displayChartError(
    statusByDeliveryChartEl,
    "No status by delivery data available"
  );
  displayChartError(shipmentTreeChartEl, "No shipment tree data available");

  // Update table with no data message
  const tbody = scanDataTableEl.querySelector("tbody");
  tbody.innerHTML = `
    <tr>
      <td colspan="9" class="text-center">
        <div class="alert alert-info mb-0" role="alert">
          No data available. Please check if Excel files are present in the data directory and try refreshing.
        </div>
      </td>
    </tr>
  `;

  // Update last update time
  lastUpdateTimeEl.textContent = "Last updated: Never";
}

/**
 * Display error message when API request fails
 */
function displayErrorMessage(errorMsg) {
  // Update summary cards
  filesComparedEl.textContent = "Error";
  commonSerialsEl.textContent = "Error";
  totalUsersEl.textContent = "Error";
  avgTimeEl.textContent = "Error";
  warehouseFilterEl.textContent = "Error";

  // Show error message in chart containers
  displayChartError(statusSummaryChartEl, "Error loading chart data");
  displayChartError(userActivityChartEl, "Error loading chart data");
  displayChartError(statusByDeliveryChartEl, "Error loading chart data");
  displayChartError(shipmentTreeChartEl, "Error loading chart data");

  // Update table with error message
  const tbody = scanDataTableEl.querySelector("tbody");
  tbody.innerHTML = `
    <tr>
      <td colspan="9" class="text-center">
        <div class="alert alert-danger mb-0" role="alert">
          <strong>Error loading data:</strong> ${errorMsg}
          <hr>
          <p class="mb-0">Please check the server logs for more information and try refreshing.</p>
        </div>
      </td>
    </tr>
  `;
}

/**
 * Refresh data from the API
 */
async function refreshData() {
  try {
    // Show loading state
    refreshBtn.innerHTML =
      '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Refreshing...';
    refreshBtn.disabled = true;

    // Trigger update on the server
    const response = await fetch("/api/update");

    // Check if response is ok (status in the range 200-299)
    if (!response.ok) {
      throw new Error(
        `Server returned ${response.status}: ${response.statusText}`
      );
    }

    const data = await response.json();

    if (data.status === "success") {
      // Load the updated data
      await loadData();
    } else if (data.status === "warning") {
      // Show warning message
      console.warn(data.message);
      // Still try to load data
      await loadData();
    } else {
      // Show error message
      console.error(data.message);
      displayErrorMessage(data.message);
    }

    // Reset button state
    refreshBtn.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Refresh Data';
    refreshBtn.disabled = false;
  } catch (error) {
    console.error("Error refreshing data:", error);

    // Display error message
    displayErrorMessage(error.message);

    // Reset button state
    refreshBtn.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Refresh Data';
    refreshBtn.disabled = false;
  }
}

/**
 * Update the last update time display
 */
function updateLastUpdateTime(timestamp) {
  if (timestamp) {
    lastUpdateTimeEl.textContent = `Last updated: ${timestamp}`;
  } else {
    lastUpdateTimeEl.textContent = "Last updated: Never";
  }
}

/**
 * Update the dashboard with the latest data
 */
function updateDashboard(metadata) {
  if (!dashboardData) return;

  // Update summary cards
  updateSummaryCards(metadata);

  // Load charts
  loadStatusSummaryChart();
  loadUserActivityChart();
  loadStatusByDeliveryChart();
  loadShipmentTreeChart();

  // Update data table
  updateDataTable();
}

/**
 * Update the summary cards with the latest data
 */
function updateSummaryCards(metadata) {
  // Files processed
  const filesProcessed = metadata.files_processed || [];
  filesComparedEl.textContent =
    filesProcessed.length > 0
      ? filesProcessed.join(", ")
      : "No files processed";

  // User activity data
  if (dashboardData.user_activity && dashboardData.user_activity.length > 0) {
    // Total users
    totalUsersEl.textContent = dashboardData.user_activity.length;

    // Calculate average time between scans (if available)
    if (dashboardData.user_activity.length > 0) {
      let totalScans = 0;
      dashboardData.user_activity.forEach((user) => {
        totalScans += user.num_scans;
      });

      // For now, just show total scans as we don't have time difference data
      avgTimeEl.textContent = `${totalScans} total`;
    } else {
      avgTimeEl.textContent = "No data";
    }
  } else {
    totalUsersEl.textContent = "No data";
    avgTimeEl.textContent = "No data";
  }

  // Status summary data
  if (dashboardData.status_summary && dashboardData.status_summary.length > 0) {
    // Count total serials
    let totalSerials = 0;
    dashboardData.status_summary.forEach((status) => {
      totalSerials += status.count;
    });
    commonSerialsEl.textContent = totalSerials;
  } else {
    commonSerialsEl.textContent = "No data";
  }

  // Window minutes
  warehouseFilterEl.textContent = metadata.window_minutes
    ? `${metadata.window_minutes} min window`
    : "Default window";
}

/**
 * Load the status summary chart
 */
async function loadStatusSummaryChart() {
  try {
    const response = await fetch("/api/charts/status_summary");

    // Check if response is ok (status in the range 200-299)
    if (!response.ok) {
      throw new Error(
        `Server returned ${response.status}: ${response.statusText}`
      );
    }

    const data = await response.json();

    if (data.chart) {
      const chartData = JSON.parse(data.chart);
      Plotly.newPlot(statusSummaryChartEl, chartData.data, chartData.layout);
    } else if (data.error) {
      displayChartError(statusSummaryChartEl, data.error);
    } else {
      displayChartError(statusSummaryChartEl, "No data available for chart");
    }
  } catch (error) {
    console.error("Error loading status summary chart:", error);
    displayChartError(statusSummaryChartEl, "Error loading chart data");
  }
}

/**
 * Load the user activity chart
 */
async function loadUserActivityChart() {
  try {
    const response = await fetch("/api/charts/user_activity");

    // Check if response is ok (status in the range 200-299)
    if (!response.ok) {
      throw new Error(
        `Server returned ${response.status}: ${response.statusText}`
      );
    }

    const data = await response.json();

    if (data.chart) {
      const chartData = JSON.parse(data.chart);
      Plotly.newPlot(userActivityChartEl, chartData.data, chartData.layout);
    } else if (data.error) {
      displayChartError(userActivityChartEl, data.error);
    } else {
      displayChartError(userActivityChartEl, "No data available for chart");
    }
  } catch (error) {
    console.error("Error loading user activity chart:", error);
    displayChartError(userActivityChartEl, "Error loading chart data");
  }
}

/**
 * Load the status by delivery chart
 */
async function loadStatusByDeliveryChart() {
  try {
    const response = await fetch("/api/charts/status_by_delivery");

    // Check if response is ok (status in the range 200-299)
    if (!response.ok) {
      throw new Error(
        `Server returned ${response.status}: ${response.statusText}`
      );
    }

    const data = await response.json();

    if (data.chart) {
      const chartData = JSON.parse(data.chart);
      Plotly.newPlot(statusByDeliveryChartEl, chartData.data, chartData.layout);
    } else if (data.error) {
      displayChartError(statusByDeliveryChartEl, data.error);
    } else {
      displayChartError(statusByDeliveryChartEl, "No data available for chart");
    }
  } catch (error) {
    console.error("Error loading status by delivery chart:", error);
    displayChartError(statusByDeliveryChartEl, "Error loading chart data");
  }
}

/**
 * Load the shipment tree chart
 */
async function loadShipmentTreeChart() {
  try {
    const response = await fetch("/api/charts/shipment_tree");

    // Check if response is ok (status in the range 200-299)
    if (!response.ok) {
      throw new Error(
        `Server returned ${response.status}: ${response.statusText}`
      );
    }

    const data = await response.json();

    if (data.chart) {
      const chartData = JSON.parse(data.chart);
      Plotly.newPlot(shipmentTreeChartEl, chartData.data, chartData.layout);
    } else if (data.error) {
      displayChartError(shipmentTreeChartEl, data.error);
    } else {
      displayChartError(shipmentTreeChartEl, "No data available for chart");
    }
  } catch (error) {
    console.error("Error loading shipment tree chart:", error);
    displayChartError(shipmentTreeChartEl, "Error loading chart data");
  }
}

/**
 * Update the data table with the latest data
 */
function updateDataTable() {
  // Clear existing rows
  const tbody = scanDataTableEl.querySelector("tbody");
  tbody.innerHTML = "";

  // Check if we have user activity data
  if (
    !dashboardData.user_activity ||
    dashboardData.user_activity.length === 0
  ) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 9;
    cell.textContent = "No user activity data available";
    cell.className = "text-center";
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  // Table headers are now defined in the HTML template

  // Add rows for user activity
  dashboardData.user_activity.forEach((user) => {
    const row = document.createElement("tr");

    // Create cells
    const userCell = document.createElement("td");
    userCell.textContent = user.user;

    // New cells for shipment and progress
    const shipmentCell = document.createElement("td");
    shipmentCell.textContent = user.current_shipment || "N/A";

    const progressCell = document.createElement("td");
    if (user.completed_items !== null && user.total_items !== null) {
      // Calculate percentage
      const percentage = Math.round(
        (user.completed_items / user.total_items) * 100
      );
      // Create progress bar
      progressCell.innerHTML = `
        <div class="d-flex align-items-center">
          <div class="me-2">${user.completed_items} out of ${user.total_items} (${percentage}%)</div>
          <div class="progress flex-grow-1" style="height: 10px;">
            <div class="progress-bar bg-success" role="progressbar" style="width: ${percentage}%;" 
                 aria-valuenow="${percentage}" aria-valuemin="0" aria-valuemax="100"></div>
          </div>
        </div>
      `;
    } else {
      progressCell.textContent = "N/A";
    }

    const statusCountCell = document.createElement("td");
    if (user.ash_count !== null && user.shp_count !== null) {
      // Create a more visual representation with colored badges
      statusCountCell.innerHTML = `
        <span class="badge bg-warning me-1">ASH: ${user.ash_count}</span>
        <span class="badge bg-success">SHP: ${user.shp_count}</span>
      `;
    } else {
      statusCountCell.textContent = "N/A";
    }

    const numScansCell = document.createElement("td");
    numScansCell.textContent = user.num_scans;

    const lastScanCell = document.createElement("td");
    lastScanCell.textContent = formatTimestamp(user.last_scan_ts);

    const prevScanCell = document.createElement("td");
    prevScanCell.textContent = formatTimestamp(user.prev_scan_time);

    const timeSinceCell = document.createElement("td");
    timeSinceCell.textContent = getTimeSinceChange(user.last_scan_ts);

    const secsSinceCell = document.createElement("td");
    secsSinceCell.textContent =
      user.secs_since_last_scan !== null
        ? `${Math.round(user.secs_since_last_scan)} seconds`
        : "N/A";

    // Add cells to row
    row.appendChild(userCell);
    row.appendChild(shipmentCell);
    row.appendChild(progressCell);
    row.appendChild(statusCountCell);
    row.appendChild(numScansCell);
    row.appendChild(lastScanCell);
    row.appendChild(prevScanCell);
    row.appendChild(timeSinceCell);
    row.appendChild(secsSinceCell);

    // Add row to table
    tbody.appendChild(row);
  });
}

/**
 * Format a timestamp for display
 */
function formatTimestamp(timestamp) {
  if (!timestamp) return "N/A";

  try {
    const date = new Date(timestamp);
    return date.toLocaleString();
  } catch (e) {
    console.error("Error formatting timestamp:", e);
    return timestamp;
  }
}

/**
 * Get time since change
 */
function getTimeSinceChange(timestamp) {
  if (!timestamp) {
    return "Unknown";
  }

  try {
    // Parse the timestamp
    const timestamp_date = new Date(timestamp);
    const now = new Date();

    // Calculate the difference
    const diff = now - timestamp_date;
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

// Initialize the dashboard when the DOM is loaded
document.addEventListener("DOMContentLoaded", initDashboard);
