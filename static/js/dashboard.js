/**
 * Dashboard JavaScript for SAP Snapshot Comparison Dashboard
 */

// Global variables
let comparisonData = null;
let updateInterval = 60000; // Default update interval: 60 seconds

// DOM elements
const refreshBtn = document.getElementById("refresh-btn");
const lastUpdateTimeEl = document.getElementById("last-update-time");
const filesComparedEl = document.getElementById("files-compared");
const commonSerialsEl = document.getElementById("common-serials");
const totalUsersEl = document.getElementById("total-users");
const avgTimeEl = document.getElementById("avg-time");
const scanDataTableEl = document.getElementById("scan-data-table");

// Chart containers
const userScanTimesChartEl = document.getElementById("user-scan-times-chart");
const scanDistributionChartEl = document.getElementById(
  "scan-distribution-chart"
);
const timelineChartEl = document.getElementById("timeline-chart");

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
 * Load data from the API
 */
async function loadData() {
  try {
    const response = await fetch("/api/data");
    const data = await response.json();

    if (data.comparison_data) {
      comparisonData = data.comparison_data;
      updateLastUpdateTime(data.last_update_time);
      updateDashboard();
    } else {
      console.error("No comparison data available");
    }
  } catch (error) {
    console.error("Error loading data:", error);
  }
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
    const data = await response.json();

    if (data.status === "success") {
      // Load the updated data
      await loadData();
    }

    // Reset button state
    refreshBtn.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Refresh Data';
    refreshBtn.disabled = false;
  } catch (error) {
    console.error("Error refreshing data:", error);

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
  }
}

/**
 * Update the dashboard with the latest data
 */
function updateDashboard() {
  if (!comparisonData) return;

  // Update summary cards
  updateSummaryCards();

  // Update charts
  loadUserScanTimesChart();
  loadScanDistributionChart();
  loadTimelineChart();

  // Update data table
  updateDataTable();
}

/**
 * Update the summary cards with the latest data
 */
function updateSummaryCards() {
  // Files compared
  const file1 = comparisonData.metadata.file1;
  const file2 = comparisonData.metadata.file2;
  filesComparedEl.textContent = `${file1} & ${file2}`;

  // Common serials
  const commonSerials = comparisonData.metadata.common_serials_count;
  commonSerialsEl.textContent = commonSerials;

  // Total users
  const users = Object.keys(comparisonData.user_scan_times);
  totalUsersEl.textContent = users.length;

  // Average time between scans
  let totalTime = 0;
  let totalScans = 0;

  for (const user in comparisonData.user_scan_times) {
    const userData = comparisonData.user_scan_times[user];
    totalTime += userData.total_time;
    totalScans += userData.total_scans;
  }

  const avgTime = totalScans > 0 ? (totalTime / totalScans).toFixed(2) : 0;
  avgTimeEl.textContent = `${avgTime} seconds`;
}

/**
 * Load the user scan times chart
 */
async function loadUserScanTimesChart() {
  try {
    const response = await fetch("/api/charts/user_scan_times");
    const data = await response.json();

    if (data.chart) {
      const chartData = JSON.parse(data.chart);
      Plotly.newPlot(userScanTimesChartEl, chartData.data, chartData.layout);
    }
  } catch (error) {
    console.error("Error loading user scan times chart:", error);
  }
}

/**
 * Load the scan distribution chart
 */
async function loadScanDistributionChart() {
  try {
    const response = await fetch("/api/charts/scan_distribution");
    const data = await response.json();

    if (data.chart) {
      const chartData = JSON.parse(data.chart);
      Plotly.newPlot(scanDistributionChartEl, chartData.data, chartData.layout);
    }
  } catch (error) {
    console.error("Error loading scan distribution chart:", error);
  }
}

/**
 * Load the timeline chart
 */
async function loadTimelineChart() {
  try {
    const response = await fetch("/api/charts/timeline");
    const data = await response.json();

    if (data.chart) {
      const chartData = JSON.parse(data.chart);
      Plotly.newPlot(timelineChartEl, chartData.data, chartData.layout);
    }
  } catch (error) {
    console.error("Error loading timeline chart:", error);
  }
}

/**
 * Update the data table with the latest data
 */
function updateDataTable() {
  // Clear existing rows
  const tbody = scanDataTableEl.querySelector("tbody");
  tbody.innerHTML = "";

  // Add new rows
  comparisonData.serial_deltas.forEach((delta) => {
    const row = document.createElement("tr");

    // Create cells
    const serialCell = document.createElement("td");
    serialCell.textContent = delta.serial;

    const timeDiffCell = document.createElement("td");
    timeDiffCell.textContent = delta.time_diff.toFixed(2);

    const earlierUserCell = document.createElement("td");
    earlierUserCell.textContent = delta.earlier_user;

    const laterUserCell = document.createElement("td");
    laterUserCell.textContent = delta.later_user;

    const earlierTimestampCell = document.createElement("td");
    earlierTimestampCell.textContent = formatTimestamp(delta.earlier_timestamp);

    const laterTimestampCell = document.createElement("td");
    laterTimestampCell.textContent = formatTimestamp(delta.later_timestamp);

    const statusCell = document.createElement("td");
    statusCell.textContent = delta.status;

    const customerCell = document.createElement("td");
    customerCell.textContent = delta.customer_name;

    // Add cells to row
    row.appendChild(serialCell);
    row.appendChild(timeDiffCell);
    row.appendChild(earlierUserCell);
    row.appendChild(laterUserCell);
    row.appendChild(earlierTimestampCell);
    row.appendChild(laterTimestampCell);
    row.appendChild(statusCell);
    row.appendChild(customerCell);

    // Add row to table
    tbody.appendChild(row);
  });
}

/**
 * Format a timestamp for display
 */
function formatTimestamp(timestamp) {
  if (!timestamp) return "";

  const date = new Date(timestamp);
  return date.toLocaleString();
}

// Initialize the dashboard when the DOM is loaded
document.addEventListener("DOMContentLoaded", initDashboard);
