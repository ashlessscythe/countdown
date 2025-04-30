/**
 * Summary component JavaScript
 * Handles fetching and displaying summary statistics
 */

// Function to fetch data and update the summary component
function updateSummaryStats() {
  // Fetch data from the API
  Promise.all([
    fetch("/api/data").then((response) => response.json()),
    fetch("/api/config").then((response) => response.json()),
  ])
    .then(([data, configData]) => {
      // Process the data and update the summary component
      displaySummaryStats(data, configData);
    })
    .catch((error) => {
      console.error("Error fetching summary data:", error);
    });
}

// Function to display summary statistics
function displaySummaryStats(data, configData) {
  // Check if summary data is available
  if (!data.summary) {
    console.error("Summary data not available in the API response");
    return;
  }

  const summary = data.summary;
  const fullStats = summary.full_statistics || {};
  const changesStats = summary.changes_statistics || {};

  // Get overall statistics
  const totalChanges = summary.total_changes || 0;
  const totalRows = fullStats.total_rows || 0;
  const totalCustomers = fullStats.total_customers || 0;
  const totalUsers = fullStats.total_users || 0;
  const statusDistribution = fullStats.status_distribution || {};
  const customerStats = fullStats.customer_stats || {};
  const shipmentStats = fullStats.shipment_stats || {};
  const lastUpdated = formatTimestamp(summary.timestamp || "");

  // Get warehouse filter from config
  const warehouseFilter =
    configData && configData.filter_whse ? configData.filter_whse : "None";

  // Update the DOM with the statistics
  document.getElementById("total-rows").textContent = totalRows;
  document.getElementById("warehouse-filter").textContent = warehouseFilter;
  document.getElementById("total-changes").textContent = totalChanges;
  document.getElementById("total-customers").textContent = totalCustomers;
  document.getElementById("total-users").textContent = totalUsers;
  document.getElementById("last-updated").textContent = lastUpdated;

  // Display status distribution
  displayStatusDistribution(statusDistribution);

  // Display customer statistics
  displayCustomerStats(customerStats);

  // Display shipment statistics
  displayShipmentStats(shipmentStats);

  // Log additional information to console for debugging
  console.log("Total rows in full dataset:", totalRows);
  console.log("Full statistics:", fullStats);
  console.log("Changes statistics:", changesStats);
}

// Format timestamp to a readable format
function formatTimestamp(timestamp) {
  if (!timestamp) return "Unknown";

  try {
    // Parse the ISO format timestamp
    const date = new Date(timestamp);
    return date.toLocaleString();
  } catch (e) {
    return timestamp;
  }
}

// Display status distribution
function displayStatusDistribution(statusDistribution) {
  const container = document.getElementById("status-distribution");
  container.innerHTML = "";

  // Sort statuses by count (descending)
  const sortedStatuses = Object.entries(statusDistribution).sort(
    (a, b) => b[1] - a[1]
  );

  // Create a table to display the status distribution
  const table = document.createElement("table");
  table.className = "table table-sm";

  // Create table header
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  const statusHeader = document.createElement("th");
  statusHeader.textContent = "Status";
  const countHeader = document.createElement("th");
  countHeader.textContent = "Count";
  headerRow.appendChild(statusHeader);
  headerRow.appendChild(countHeader);
  thead.appendChild(headerRow);
  table.appendChild(thead);

  // Create table body
  const tbody = document.createElement("tbody");
  sortedStatuses.forEach(([status, count]) => {
    const row = document.createElement("tr");
    const statusCell = document.createElement("td");
    statusCell.textContent = status;
    const countCell = document.createElement("td");
    countCell.textContent = count;
    row.appendChild(statusCell);
    row.appendChild(countCell);
    tbody.appendChild(row);
  });
  table.appendChild(tbody);

  container.appendChild(table);
}

// Display customer statistics
function displayCustomerStats(customerStats) {
  const container = document.getElementById("customer-stats");
  container.innerHTML = "";

  // Sort customers by serial count (descending)
  const sortedCustomers = Object.entries(customerStats)
    .sort((a, b) => b[1].serial_count - a[1].serial_count)
    .slice(0, 5); // Show top 5 customers

  // Create a table to display the customer statistics
  const table = document.createElement("table");
  table.className = "table table-sm";

  // Create table header
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  const customerHeader = document.createElement("th");
  customerHeader.textContent = "Customer";
  const serialsHeader = document.createElement("th");
  serialsHeader.textContent = "Serials";
  const changesHeader = document.createElement("th");
  changesHeader.textContent = "Changes";
  headerRow.appendChild(customerHeader);
  headerRow.appendChild(serialsHeader);
  headerRow.appendChild(changesHeader);
  thead.appendChild(headerRow);
  table.appendChild(thead);

  // Create table body
  const tbody = document.createElement("tbody");
  sortedCustomers.forEach(([customer, stats]) => {
    const row = document.createElement("tr");
    const customerCell = document.createElement("td");
    customerCell.textContent =
      customer.length > 20 ? customer.substring(0, 20) + "..." : customer;
    customerCell.title = customer; // Show full name on hover
    const serialsCell = document.createElement("td");
    serialsCell.textContent = stats.serial_count;
    const changesCell = document.createElement("td");
    changesCell.textContent = stats.change_count;
    row.appendChild(customerCell);
    row.appendChild(serialsCell);
    row.appendChild(changesCell);
    tbody.appendChild(row);
  });
  table.appendChild(tbody);

  container.appendChild(table);
}

// Display shipment statistics
function displayShipmentStats(shipmentStats) {
  const container = document.getElementById("shipment-stats");
  container.innerHTML = "";

  // Sort shipments by serial count (descending)
  const sortedShipments = Object.entries(shipmentStats)
    .sort((a, b) => b[1].serial_count - a[1].serial_count)
    .slice(0, 5); // Show top 5 shipments

  // Create a table to display the shipment statistics
  const table = document.createElement("table");
  table.className = "table table-sm";

  // Create table header
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  const shipmentHeader = document.createElement("th");
  shipmentHeader.textContent = "Shipment";
  const serialsHeader = document.createElement("th");
  serialsHeader.textContent = "Serials";
  const customerHeader = document.createElement("th");
  customerHeader.textContent = "Customer";
  headerRow.appendChild(shipmentHeader);
  headerRow.appendChild(serialsHeader);
  headerRow.appendChild(customerHeader);
  thead.appendChild(headerRow);
  table.appendChild(thead);

  // Create table body
  const tbody = document.createElement("tbody");
  sortedShipments.forEach(([shipment, stats]) => {
    const row = document.createElement("tr");
    const shipmentCell = document.createElement("td");
    shipmentCell.textContent = shipment;
    const serialsCell = document.createElement("td");
    serialsCell.textContent = stats.serial_count;
    const customerCell = document.createElement("td");
    customerCell.textContent =
      stats.customer.length > 15
        ? stats.customer.substring(0, 15) + "..."
        : stats.customer;
    customerCell.title = stats.customer; // Show full name on hover
    row.appendChild(shipmentCell);
    row.appendChild(serialsCell);
    row.appendChild(customerCell);
    tbody.appendChild(row);
  });
  table.appendChild(tbody);

  container.appendChild(table);
}

// Initialize the summary component
document.addEventListener("DOMContentLoaded", function () {
  // Update summary stats when the page loads
  updateSummaryStats();

  // Set up periodic refresh (every 60 seconds)
  setInterval(updateSummaryStats, 60000);
});
