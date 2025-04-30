/**
 * Common functionality for the Delivery Analysis Dashboard
 */

// Global variables
let currentView = "overview";
let currentFilter = {
  type: "",
  value: "",
};
let chartInstances = {};

// DOM elements
const filterTypeSelect = document.getElementById("filter-type");
const filterValueSelect = document.getElementById("filter-value");
const applyFilterBtn = document.getElementById("apply-filter");
const clearFilterBtn = document.getElementById("clear-filter");
const refreshBtn = document.getElementById("refresh-btn");
const runCompareBtn = document.getElementById("run-compare-btn");
const lastUpdatedSpan = document.getElementById("last-updated");

// Initialize the dashboard
document.addEventListener("DOMContentLoaded", function () {
  // Determine the current view from the URL path
  const path = window.location.pathname;
  if (path === "/" || path === "") {
    currentView = "overview";
  } else {
    // Remove the leading slash and use as view name
    currentView = path.substring(1);
  }

  // Load initial data
  loadDashboardData();

  // Set up event listeners
  setupEventListeners();

  // Set up auto-refresh (every 60 seconds)
  setInterval(loadDashboardData, 60000);
});

// Set up event listeners
function setupEventListeners() {
  // Navigation links - we're now using actual URLs instead of JavaScript view switching
  // The links in the navbar now point to actual routes

  // Filter type change
  filterTypeSelect.addEventListener("change", function () {
    const filterType = this.value;
    if (filterType) {
      loadFilterValues(filterType);
      filterValueSelect.disabled = false;
    } else {
      filterValueSelect.disabled = true;
      filterValueSelect.innerHTML =
        '<option value="">Select a filter type first</option>';
      applyFilterBtn.disabled = true;
    }
  });

  // Filter value change
  filterValueSelect.addEventListener("change", function () {
    applyFilterBtn.disabled = !this.value;
  });

  // Apply filter button
  applyFilterBtn.addEventListener("click", function () {
    const filterType = filterTypeSelect.value;
    const filterValue = filterValueSelect.value;

    if (filterType && filterValue) {
      currentFilter = {
        type: filterType,
        value: filterValue,
      };
      loadDashboardData();
    }
  });

  // Clear filter button
  clearFilterBtn.addEventListener("click", function () {
    currentFilter = {
      type: "",
      value: "",
    };
    filterTypeSelect.value = "";
    filterValueSelect.value = "";
    filterValueSelect.disabled = true;
    applyFilterBtn.disabled = true;
    loadDashboardData();
  });

  // Refresh button
  refreshBtn.addEventListener("click", function () {
    loadDashboardData();
  });

  // Run Compare button
  if (runCompareBtn) {
    runCompareBtn.addEventListener("click", function () {
      runCompareBtn.disabled = true;
      runCompareBtn.innerHTML =
        '<i class="bi bi-hourglass-split"></i> Running...';

      runCompare()
        .then(() => {
          loadDashboardData();
        })
        .finally(() => {
          runCompareBtn.disabled = false;
          runCompareBtn.innerHTML =
            '<i class="bi bi-play-fill"></i> Run Compare';
        });
    });
  }
}

// Run the comparison process
function runCompare() {
  return fetch("/api/run-compare")
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        console.error("Error running comparison:", data.error);
        alert("Error running comparison: " + data.error);
      } else {
        console.log("Comparison completed successfully");
      }
    })
    .catch((error) => {
      console.error("Error running comparison:", error);
      alert("Error running comparison: " + error.message);
    });
}

// Switch between views
function switchView(view) {
  // Hide all views
  document.querySelectorAll(".view-content").forEach((el) => {
    el.classList.add("d-none");
  });

  // Show the selected view
  document.getElementById(`${view}-view`).classList.remove("d-none");

  // Update navigation links
  document.querySelectorAll(".nav-link").forEach((link) => {
    link.classList.remove("active");
    if (link.getAttribute("data-view") === view) {
      link.classList.add("active");
    }
  });

  // Update current view
  currentView = view;

  // Load view-specific data
  loadViewData(view);
}

// Load dashboard data
function loadDashboardData() {
  // Update last updated time
  lastUpdatedSpan.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;

  // Load summary data
  loadSummaryData();

  // Load view-specific data
  loadViewData(currentView);
}

// Load summary data
function loadSummaryData() {
  fetch("/api/summary")
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        console.error("Error loading summary data:", data.error);
        return;
      }

      // Update summary cards
      document.getElementById("total-changes").textContent = data.total_changes;

      // Count unique deliveries, customers, and shipments
      const deliveryStatus = data.delivery_status || {};
      const customerStatus = data.customer_status || {};
      const shipmentStatus = data.shipment_status || {};
      const userStatus = data.user_status || {};

      document.getElementById("total-deliveries").textContent =
        Object.keys(deliveryStatus).length;
      document.getElementById("total-customers").textContent =
        Object.keys(customerStatus).length;
      document.getElementById("total-shipments").textContent =
        Object.keys(shipmentStatus).length;

      // Update user count if the element exists
      const totalUsersElement = document.getElementById("total-users");
      if (totalUsersElement) {
        totalUsersElement.textContent = Object.keys(userStatus).length;
      }
    })
    .catch((error) => {
      console.error("Error fetching summary data:", error);
    });
}

// Load view-specific data
function loadViewData(view) {
  switch (view) {
    case "overview":
      loadOverviewData();
      break;
    case "deliveries":
      loadDeliveriesData();
      break;
    case "customers":
      loadCustomersData();
      break;
    case "shipments":
      loadShipmentsData();
      break;
    case "users":
      loadUsersData();
      break;
  }
}

// Load filter values
function loadFilterValues(filterType) {
  let endpoint = "";

  switch (filterType) {
    case "delivery":
      endpoint = "/api/deliveries";
      break;
    case "customer":
      endpoint = "/api/customers";
      break;
    case "shipment":
      endpoint = "/api/shipments";
      break;
    case "user":
      endpoint = "/api/users";
      break;
    default:
      return;
  }

  fetch(endpoint)
    .then((response) => response.json())
    .then((data) => {
      let options = '<option value="">Select a value</option>';

      if (filterType === "delivery") {
        data.forEach((item) => {
          options += `<option value="${item.id}">${item.id} (${item.customer})</option>`;
        });
      } else if (filterType === "customer") {
        data.forEach((item) => {
          options += `<option value="${item.name}">${item.name}</option>`;
        });
      } else if (filterType === "shipment") {
        data.forEach((item) => {
          options += `<option value="${item.id}">${item.id} (${item.customer})</option>`;
        });
      } else if (filterType === "user") {
        data.forEach((item) => {
          options += `<option value="${item.id}">${item.id}</option>`;
        });
      }

      filterValueSelect.innerHTML = options;
    })
    .catch((error) => {
      console.error(`Error loading ${filterType} values:`, error);
      filterValueSelect.innerHTML =
        '<option value="">Error loading values</option>';
    });
}
