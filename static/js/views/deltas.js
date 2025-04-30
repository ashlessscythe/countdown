/**
 * Delta view functionality
 *
 * This file handles the display of delta and snapshot data
 */

// Load delta data
function loadDeltaData() {
  // Load deltas list
  loadDeltasList();

  // Load snapshots list
  loadSnapshotsList();

  // Load statistics
  loadDeltaStatistics();
}

// Load deltas list
function loadDeltasList() {
  showTableLoading("deltas-table", 5);

  fetch("/api/deltas")
    .then((response) => response.json())
    .then((deltas) => {
      const tableBody = document.getElementById("deltas-table");

      if (!deltas || deltas.error || deltas.length === 0) {
        showTableError("deltas-table", 5, "No deltas found");
        return;
      }

      // Build table rows
      let html = "";
      deltas.forEach((delta) => {
        // Format timestamp
        const timestamp = new Date(delta.timestamp).toLocaleString();
        const timeSinceChange = getTimeSinceChange(delta.timestamp);

        html += `
          <tr>
            <td>${delta.filename}</td>
            <td>${timestamp}<br><small class="text-muted">${timeSinceChange}</small></td>
            <td>
              <span class="badge bg-success">${delta.added_count} Added</span>
              <span class="badge bg-danger">${delta.removed_count} Removed</span>
              <span class="badge bg-warning">${delta.updated_count} Updated</span>
            </td>
            <td>${delta.total_changes}</td>
            <td>
              <button class="btn btn-sm btn-primary view-delta-btn" data-filename="${delta.filename}">
                View Details
              </button>
            </td>
          </tr>
        `;
      });

      tableBody.innerHTML = html;

      // Add event listeners to view buttons
      document.querySelectorAll(".view-delta-btn").forEach((btn) => {
        btn.addEventListener("click", function () {
          const filename = this.getAttribute("data-filename");
          loadDeltaDetails(filename);
        });
      });
    })
    .catch((error) => {
      console.error("Error fetching deltas:", error);
      showTableError("deltas-table", 5, "Error loading deltas");
    });
}

// Load snapshots list
function loadSnapshotsList() {
  showTableLoading("snapshots-table", 5);

  fetch("/api/snapshots")
    .then((response) => response.json())
    .then((snapshots) => {
      const tableBody = document.getElementById("snapshots-table");

      if (!snapshots || snapshots.error || snapshots.length === 0) {
        showTableError("snapshots-table", 5, "No snapshots found");
        return;
      }

      // Build table rows
      let html = "";
      snapshots.forEach((snapshot) => {
        // Format timestamp
        const timestamp = new Date(snapshot.timestamp).toLocaleString();
        const timeSinceChange = getTimeSinceChange(snapshot.timestamp);

        // Format size in KB
        const sizeKB = Math.round(snapshot.size / 1024);

        html += `
          <tr>
            <td>${snapshot.filename}</td>
            <td>${timestamp}<br><small class="text-muted">${timeSinceChange}</small></td>
            <td>${snapshot.source_file || "N/A"}</td>
            <td>${snapshot.record_count}</td>
            <td>
              <button class="btn btn-sm btn-primary view-snapshot-btn" data-filename="${
                snapshot.filename
              }">
                View Details
              </button>
            </td>
          </tr>
        `;
      });

      tableBody.innerHTML = html;

      // Add event listeners to view buttons
      document.querySelectorAll(".view-snapshot-btn").forEach((btn) => {
        btn.addEventListener("click", function () {
          const filename = this.getAttribute("data-filename");
          loadSnapshotDetails(filename);
        });
      });
    })
    .catch((error) => {
      console.error("Error fetching snapshots:", error);
      showTableError("snapshots-table", 5, "Error loading snapshots");
    });
}

// Load delta details
function loadDeltaDetails(filename) {
  // Show loading state
  const detailsContainer = document.getElementById("delta-details");
  detailsContainer.innerHTML =
    '<div class="text-center"><div class="spinner-border" role="status"></div><p>Loading delta details...</p></div>';

  // Show the details container
  document.getElementById("delta-details-container").classList.remove("d-none");

  fetch(`/api/delta/${filename}`)
    .then((response) => response.json())
    .then((delta) => {
      if (!delta || delta.error) {
        detailsContainer.innerHTML = `<div class="alert alert-danger">Error loading delta: ${
          delta.error || "Unknown error"
        }</div>`;
        return;
      }

      // Extract metadata
      const metadata = delta.metadata || {};
      const snapshot1 = metadata.snapshot1 || "Unknown";
      const snapshot2 = metadata.snapshot2 || "Unknown";
      const createdAt = metadata.created_at
        ? new Date(metadata.created_at).toLocaleString()
        : "Unknown";
      const addedCount = metadata.added_count || 0;
      const removedCount = metadata.removed_count || 0;
      const updatedCount = metadata.updated_count || 0;

      // Build HTML
      let html = `
        <div class="card mb-4">
          <div class="card-header d-flex justify-content-between align-items-center">
            <h5 class="mb-0">Delta Details: ${filename}</h5>
            <button type="button" class="btn-close" aria-label="Close" id="close-delta-details"></button>
          </div>
          <div class="card-body">
            <div class="row mb-3">
              <div class="col-md-6">
                <h6>Metadata</h6>
                <ul class="list-group">
                  <li class="list-group-item"><strong>Created:</strong> ${createdAt}</li>
                  <li class="list-group-item"><strong>Base Snapshot:</strong> ${snapshot1}</li>
                  <li class="list-group-item"><strong>New Snapshot:</strong> ${snapshot2}</li>
                </ul>
              </div>
              <div class="col-md-6">
                <h6>Change Summary</h6>
                <div class="d-flex justify-content-between">
                  <div class="text-center p-3 border rounded">
                    <h3 class="text-success">${addedCount}</h3>
                    <p>Added</p>
                  </div>
                  <div class="text-center p-3 border rounded">
                    <h3 class="text-danger">${removedCount}</h3>
                    <p>Removed</p>
                  </div>
                  <div class="text-center p-3 border rounded">
                    <h3 class="text-warning">${updatedCount}</h3>
                    <p>Updated</p>
                  </div>
                </div>
              </div>
            </div>
            
            <ul class="nav nav-tabs" id="deltaDetailsTabs" role="tablist">
              <li class="nav-item" role="presentation">
                <button class="nav-link active" id="added-tab" data-bs-toggle="tab" data-bs-target="#added" type="button" role="tab">
                  Added (${addedCount})
                </button>
              </li>
              <li class="nav-item" role="presentation">
                <button class="nav-link" id="removed-tab" data-bs-toggle="tab" data-bs-target="#removed" type="button" role="tab">
                  Removed (${removedCount})
                </button>
              </li>
              <li class="nav-item" role="presentation">
                <button class="nav-link" id="updated-tab" data-bs-toggle="tab" data-bs-target="#updated" type="button" role="tab">
                  Updated (${updatedCount})
                </button>
              </li>
            </ul>
            
            <div class="tab-content p-3 border border-top-0 rounded-bottom" id="deltaDetailsTabContent">
              <div class="tab-pane fade show active" id="added" role="tabpanel">
                ${buildAddedTable(delta.added || [])}
              </div>
              <div class="tab-pane fade" id="removed" role="tabpanel">
                ${buildRemovedTable(delta.removed || [])}
              </div>
              <div class="tab-pane fade" id="updated" role="tabpanel">
                ${buildUpdatedTable(delta.updated || [])}
              </div>
            </div>
          </div>
        </div>
      `;

      detailsContainer.innerHTML = html;

      // Add event listener to close button
      document
        .getElementById("close-delta-details")
        .addEventListener("click", function () {
          document
            .getElementById("delta-details-container")
            .classList.add("d-none");
        });
    })
    .catch((error) => {
      console.error("Error fetching delta details:", error);
      detailsContainer.innerHTML = `<div class="alert alert-danger">Error loading delta: ${error.message}</div>`;
    });
}

// Load snapshot details
function loadSnapshotDetails(filename) {
  // Show loading state
  const detailsContainer = document.getElementById("snapshot-details");
  detailsContainer.innerHTML =
    '<div class="text-center"><div class="spinner-border" role="status"></div><p>Loading snapshot details...</p></div>';

  // Show the details container
  document
    .getElementById("snapshot-details-container")
    .classList.remove("d-none");

  fetch(`/api/snapshot/${filename}`)
    .then((response) => response.json())
    .then((snapshot) => {
      if (!snapshot || snapshot.error) {
        detailsContainer.innerHTML = `<div class="alert alert-danger">Error loading snapshot: ${
          snapshot.error || "Unknown error"
        }</div>`;
        return;
      }

      // Extract metadata
      const metadata = snapshot.metadata || {};
      const sourceFile = metadata.source_file || "Unknown";
      const createdAt = metadata.created_at
        ? new Date(metadata.created_at).toLocaleString()
        : "Unknown";
      const recordCount = metadata.record_count || 0;

      // Build HTML
      let html = `
        <div class="card mb-4">
          <div class="card-header d-flex justify-content-between align-items-center">
            <h5 class="mb-0">Snapshot Details: ${filename}</h5>
            <button type="button" class="btn-close" aria-label="Close" id="close-snapshot-details"></button>
          </div>
          <div class="card-body">
            <div class="row mb-3">
              <div class="col-md-6">
                <h6>Metadata</h6>
                <ul class="list-group">
                  <li class="list-group-item"><strong>Created:</strong> ${createdAt}</li>
                  <li class="list-group-item"><strong>Source File:</strong> ${sourceFile}</li>
                  <li class="list-group-item"><strong>Record Count:</strong> ${recordCount}</li>
                </ul>
              </div>
            </div>
            
            <h6>Records (${recordCount})</h6>
            <div class="table-responsive">
              ${buildRecordsTable(snapshot.records || [])}
            </div>
          </div>
        </div>
      `;

      detailsContainer.innerHTML = html;

      // Add event listener to close button
      document
        .getElementById("close-snapshot-details")
        .addEventListener("click", function () {
          document
            .getElementById("snapshot-details-container")
            .classList.add("d-none");
        });
    })
    .catch((error) => {
      console.error("Error fetching snapshot details:", error);
      detailsContainer.innerHTML = `<div class="alert alert-danger">Error loading snapshot: ${error.message}</div>`;
    });
}

// Load delta statistics
function loadDeltaStatistics() {
  fetch("/api/statistics")
    .then((response) => response.json())
    .then((data) => {
      if (!data || data.error) {
        console.error("Error loading statistics:", data.error);
        return;
      }

      const statistics = data.statistics || {};

      // Update statistics cards
      updateStatisticsCards(statistics);

      // Update charts
      updateStatisticsCharts(statistics);
    })
    .catch((error) => {
      console.error("Error fetching statistics:", error);
    });
}

// Update statistics cards
function updateStatisticsCards(statistics) {
  // Update total records
  const totalRecordsElement = document.getElementById("total-records");
  if (totalRecordsElement) {
    totalRecordsElement.textContent = statistics.total_records || 0;
  }

  // Update status distribution
  const statusDistribution = statistics.status_distribution || {};
  const statusDistributionElement = document.getElementById(
    "status-distribution"
  );
  if (statusDistributionElement) {
    let html = "";
    for (const [status, count] of Object.entries(statusDistribution)) {
      html += `<div class="d-flex justify-content-between">
                <span>${status}</span>
                <span class="badge bg-primary">${count}</span>
              </div>`;
    }
    statusDistributionElement.innerHTML = html;
  }
}

// Update statistics charts
function updateStatisticsCharts(statistics) {
  // Status distribution chart
  const statusDistribution = statistics.status_distribution || {};
  const statusChartElement = document.getElementById(
    "status-distribution-chart"
  );

  if (statusChartElement) {
    const ctx = statusChartElement.getContext("2d");

    // Prepare chart data
    const labels = Object.keys(statusDistribution);
    const data = Object.values(statusDistribution);

    // Define colors for each status
    const backgroundColors = labels.map((label) => {
      const statusMap = {
        ASH: "#28a745",
        SHP: "#007bff",
        DELIVERED: "#6610f2",
        CANCELLED: "#dc3545",
        PENDING: "#ffc107",
      };

      return statusMap[label] || "#6c757d";
    });

    // Create or update chart
    if (chartInstances.statusDistributionChart) {
      chartInstances.statusDistributionChart.destroy();
    }

    chartInstances.statusDistributionChart = new Chart(ctx, {
      type: "pie",
      data: {
        labels: labels,
        datasets: [
          {
            data: data,
            backgroundColor: backgroundColors,
            borderWidth: 1,
          },
        ],
      },
      options: {
        responsive: true,
        plugins: {
          legend: {
            position: "right",
          },
          title: {
            display: true,
            text: "Status Distribution",
          },
        },
      },
    });
  }

  // Customer statistics chart
  const customerStats = statistics.customer_stats || {};
  const customerChartElement = document.getElementById("customer-stats-chart");

  if (customerChartElement) {
    const ctx = customerChartElement.getContext("2d");

    // Prepare chart data
    const customers = Object.keys(customerStats);
    const ashCounts = customers.map(
      (customer) => customerStats[customer].ash_count || 0
    );
    const shpCounts = customers.map(
      (customer) => customerStats[customer].shp_count || 0
    );

    // Take only top 5 customers by total count
    const topCustomers = customers
      .map((customer) => ({
        name: customer,
        total:
          (customerStats[customer].ash_count || 0) +
          (customerStats[customer].shp_count || 0),
      }))
      .sort((a, b) => b.total - a.total)
      .slice(0, 5);

    const topCustomerNames = topCustomers.map((c) => c.name);
    const topAshCounts = topCustomerNames.map(
      (customer) => customerStats[customer].ash_count || 0
    );
    const topShpCounts = topCustomerNames.map(
      (customer) => customerStats[customer].shp_count || 0
    );

    // Create or update chart
    if (chartInstances.customerStatsChart) {
      chartInstances.customerStatsChart.destroy();
    }

    chartInstances.customerStatsChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: topCustomerNames,
        datasets: [
          {
            label: "ASH",
            data: topAshCounts,
            backgroundColor: "#28a745",
            borderWidth: 1,
          },
          {
            label: "SHP",
            data: topShpCounts,
            backgroundColor: "#007bff",
            borderWidth: 1,
          },
        ],
      },
      options: {
        responsive: true,
        plugins: {
          legend: {
            position: "top",
          },
          title: {
            display: true,
            text: "Top 5 Customers by Status",
          },
        },
        scales: {
          x: {
            stacked: true,
          },
          y: {
            stacked: true,
            beginAtZero: true,
          },
        },
      },
    });
  }
}

// Helper function to build added table
function buildAddedTable(added) {
  if (!added || added.length === 0) {
    return '<div class="alert alert-info">No records added</div>';
  }

  let html = `
    <div class="table-responsive">
      <table class="table table-striped table-sm">
        <thead>
          <tr>
            <th>Serial</th>
            <th>Status</th>
            <th>Delivery</th>
            <th>Customer</th>
            <th>Shipment</th>
            <th>Created By</th>
          </tr>
        </thead>
        <tbody>
  `;

  added.forEach((item) => {
    const record = item.record || {};
    html += `
      <tr>
        <td>${item.serial || "N/A"}</td>
        <td>${record.status || "N/A"}</td>
        <td>${record.delivery || "N/A"}</td>
        <td>${record.customer_name || "N/A"}</td>
        <td>${record.shipment_number || "N/A"}</td>
        <td>${record.created_by || "N/A"}</td>
      </tr>
    `;
  });

  html += `
        </tbody>
      </table>
    </div>
  `;

  return html;
}

// Helper function to build removed table
function buildRemovedTable(removed) {
  if (!removed || removed.length === 0) {
    return '<div class="alert alert-info">No records removed</div>';
  }

  let html = `
    <div class="table-responsive">
      <table class="table table-striped table-sm">
        <thead>
          <tr>
            <th>Serial</th>
            <th>Status</th>
            <th>Delivery</th>
            <th>Customer</th>
            <th>Shipment</th>
            <th>Created By</th>
          </tr>
        </thead>
        <tbody>
  `;

  removed.forEach((item) => {
    const record = item.record || {};
    html += `
      <tr>
        <td>${item.serial || "N/A"}</td>
        <td>${record.status || "N/A"}</td>
        <td>${record.delivery || "N/A"}</td>
        <td>${record.customer_name || "N/A"}</td>
        <td>${record.shipment_number || "N/A"}</td>
        <td>${record.created_by || "N/A"}</td>
      </tr>
    `;
  });

  html += `
        </tbody>
      </table>
    </div>
  `;

  return html;
}

// Helper function to build updated table
function buildUpdatedTable(updated) {
  if (!updated || updated.length === 0) {
    return '<div class="alert alert-info">No records updated</div>';
  }

  let html = `
    <div class="table-responsive">
      <table class="table table-striped table-sm">
        <thead>
          <tr>
            <th>Serial</th>
            <th>Changes</th>
            <th>Current Status</th>
            <th>Delivery</th>
            <th>Customer</th>
          </tr>
        </thead>
        <tbody>
  `;

  updated.forEach((item) => {
    const record = item.record || {};
    const changes = item.changes || {};

    // Build changes display
    let changesHtml = "";
    for (const [field, change] of Object.entries(changes)) {
      changesHtml += `
        <div class="mb-1">
          <strong>${field}:</strong> 
          <span class="text-danger">${change.from || "N/A"}</span> → 
          <span class="text-success">${change.to || "N/A"}</span>
        </div>
      `;
    }

    html += `
      <tr>
        <td>${item.serial || "N/A"}</td>
        <td>${changesHtml}</td>
        <td>${record.status || "N/A"}</td>
        <td>${record.delivery || "N/A"}</td>
        <td>${record.customer_name || "N/A"}</td>
      </tr>
    `;
  });

  html += `
        </tbody>
      </table>
    </div>
  `;

  return html;
}

// Helper function to build records table
function buildRecordsTable(records) {
  if (!records || records.length === 0) {
    return '<div class="alert alert-info">No records found</div>';
  }

  // Get all possible columns from the first few records
  const sampleSize = Math.min(10, records.length);
  const columns = new Set();

  for (let i = 0; i < sampleSize; i++) {
    Object.keys(records[i]).forEach((key) => columns.add(key));
  }

  // Prioritize important columns
  const priorityColumns = [
    "serial",
    "status",
    "delivery",
    "customer_name",
    "shipment_number",
    "created_by",
    "timestamp",
  ];
  const sortedColumns = [
    ...priorityColumns.filter((col) => columns.has(col)),
    ...[...columns].filter((col) => !priorityColumns.includes(col)),
  ];

  let html = `
    <table class="table table-striped table-sm">
      <thead>
        <tr>
  `;

  // Add headers
  sortedColumns.forEach((column) => {
    html += `<th>${column}</th>`;
  });

  html += `
        </tr>
      </thead>
      <tbody>
  `;

  // Add rows
  records.forEach((record) => {
    html += `<tr>`;

    sortedColumns.forEach((column) => {
      html += `<td>${record[column] || "N/A"}</td>`;
    });

    html += `</tr>`;
  });

  html += `
      </tbody>
    </table>
  `;

  return html;
}
