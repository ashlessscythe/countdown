/**
 * Overview view functionality
 */

// Load overview data
function loadOverviewData() {
  // Load recent changes
  loadRecentChanges();

  // Load charts
  loadStatusChart();
  loadCustomerChart();
}

// Load recent changes
function loadRecentChanges() {
  fetch("/api/changes")
    .then((response) => response.json())
    .then((changes) => {
      const tableBody = document.getElementById("recent-changes-table");

      if (changes.length === 0) {
        showTableError("recent-changes-table", 7, "No changes found");
        return;
      }

      // Sort changes by timestamp (most recent first)
      changes = sortByTimestamp(changes);

      // Take only the first 10 changes
      const recentChanges = changes.slice(0, 10);

      // Build table rows
      let html = "";
      recentChanges.forEach((change) => {
        html += `
          <tr>
            <td>${change.serial || "N/A"}</td>
            <td>${change.from || "N/A"}</td>
            <td>${change.to || "N/A"}</td>
            <td>${change.delivery || "N/A"}</td>
            <td>${change.customer_name || "N/A"}</td>
            <td>${change.shipment_number || "N/A"}</td>
            <td>${change.time_since_change || "Unknown"}</td>
          </tr>
        `;
      });

      tableBody.innerHTML = html;
    })
    .catch((error) => {
      console.error("Error fetching recent changes:", error);
      showTableError("recent-changes-table", 7, "Error loading data");
    });
}

// Load status chart
function loadStatusChart() {
  fetch("/api/summary")
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        console.error("Error loading status chart data:", data.error);
        return;
      }

      // Aggregate status counts across all deliveries
      const statusCounts = {};
      const deliveryStatus = data.delivery_status || {};

      for (const delivery in deliveryStatus) {
        const statuses = deliveryStatus[delivery];
        for (const status in statuses) {
          statusCounts[status] = (statusCounts[status] || 0) + statuses[status];
        }
      }

      // Prepare chart data
      const labels = Object.keys(statusCounts);
      const counts = Object.values(statusCounts);

      // Define colors for each status
      const backgroundColors = labels.map((label) => {
        const statusMap = {
          ASH: "#28a745",
          SHIPPED: "#007bff",
          DELIVERED: "#6610f2",
          CANCELLED: "#dc3545",
          PENDING: "#ffc107",
        };

        return statusMap[label] || "#6c757d";
      });

      // Create or update chart
      const ctx = document.getElementById("status-chart").getContext("2d");

      if (chartInstances.statusChart) {
        chartInstances.statusChart.destroy();
      }

      chartInstances.statusChart = new Chart(ctx, {
        type: "pie",
        data: {
          labels: labels,
          datasets: [
            {
              data: counts,
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
    })
    .catch((error) => {
      console.error("Error fetching status chart data:", error);
    });
}

// Load customer chart
function loadCustomerChart() {
  fetch("/api/summary")
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        console.error("Error loading customer chart data:", data.error);
        return;
      }

      // Get customer status data
      const customerStatus = data.customer_status || {};

      // Calculate total changes per customer
      const customerCounts = {};
      for (const customer in customerStatus) {
        const statuses = customerStatus[customer];
        customerCounts[customer] = Object.values(statuses).reduce(
          (sum, count) => sum + count,
          0
        );
      }

      // Sort customers by count and take top 5
      const sortedCustomers = Object.entries(customerCounts)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5);

      // Prepare chart data
      const labels = sortedCustomers.map((item) => item[0]);
      const counts = sortedCustomers.map((item) => item[1]);

      // Create or update chart
      const ctx = document.getElementById("customer-chart").getContext("2d");

      if (chartInstances.customerChart) {
        chartInstances.customerChart.destroy();
      }

      chartInstances.customerChart = new Chart(ctx, {
        type: "bar",
        data: {
          labels: labels,
          datasets: [
            {
              label: "Number of Changes",
              data: counts,
              backgroundColor: "#0d6efd",
              borderWidth: 1,
            },
          ],
        },
        options: {
          responsive: true,
          plugins: {
            legend: {
              display: false,
            },
            title: {
              display: true,
              text: "Top 5 Customers by Change Count",
            },
          },
          scales: {
            y: {
              beginAtZero: true,
              title: {
                display: true,
                text: "Number of Changes",
              },
            },
            x: {
              title: {
                display: true,
                text: "Customer",
              },
            },
          },
        },
      });
    })
    .catch((error) => {
      console.error("Error fetching customer chart data:", error);
    });
}
