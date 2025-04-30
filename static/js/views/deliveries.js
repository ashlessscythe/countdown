/**
 * Deliveries view functionality
 */

// Load deliveries data
function loadDeliveriesData() {
  fetch("/api/deliveries")
    .then((response) => response.json())
    .then((deliveries) => {
      const tableBody = document.getElementById("deliveries-table");

      if (deliveries.length === 0) {
        showTableError("deliveries-table", 6, "No deliveries found");
        return;
      }

      // Sort deliveries by time since change (most recent first)
      deliveries = sortByTimestamp(deliveries);

      // Build table rows
      let html = "";
      deliveries.forEach((delivery) => {
        // Create a status badge for each status
        let statusBadges = "";
        if (delivery.status_counts) {
          for (const [status, count] of Object.entries(
            delivery.status_counts
          )) {
            const badgeClass = getStatusBadgeClass(status);
            statusBadges += `<span class="badge ${badgeClass} me-1">${status}: ${count}</span>`;
          }
        }

        html += `
          <tr>
            <td>${delivery.id || "N/A"}</td>
            <td>${delivery.customer || "N/A"}</td>
            <td>${delivery.count || 0}</td>
            <td>${statusBadges || "N/A"}</td>
            <td>${delivery.time_since_change || "Unknown"}</td>
            <td>
              <i class="bi bi-eye action-btn" onclick="showDeliveryDetails('${
                delivery.id
              }')"></i>
            </td>
          </tr>
        `;
      });

      tableBody.innerHTML = html;
    })
    .catch((error) => {
      console.error("Error fetching deliveries data:", error);
      showTableError("deliveries-table", 6, "Error loading data");
    });
}

// Show delivery details
function showDeliveryDetails(deliveryId) {
  // Update header
  document.getElementById("delivery-id-header").textContent = deliveryId;

  // Show details card
  document.getElementById("delivery-details-card").classList.remove("d-none");

  // Show loading indicator
  showTableLoading("delivery-details-table", 6);

  // Load delivery details
  fetch(`/api/filter?type=delivery&value=${deliveryId}`)
    .then((response) => response.json())
    .then((serials) => {
      const tableBody = document.getElementById("delivery-details-table");

      if (serials.length === 0) {
        showTableError("delivery-details-table", 6, "No details found");
        return;
      }

      // Sort serials by serial number
      serials = sortBySerial(serials);

      // Build table rows
      let html = "";
      serials.forEach((serial) => {
        // Get status badge class
        const badgeClass = getStatusBadgeClass(serial.status);

        html += `
          <tr>
            <td>${serial.serial || "N/A"}</td>
            <td><span class="badge ${badgeClass}">${
          serial.status || "N/A"
        }</span></td>
            <td>${serial.customer_name || "N/A"}</td>
            <td>${serial.shipment_number || "N/A"}</td>
            <td>${serial.created_by || "N/A"}</td>
            <td>${serial.time_since_change || "Unknown"}</td>
          </tr>
        `;
      });

      tableBody.innerHTML = html;
    })
    .catch((error) => {
      console.error("Error fetching delivery details:", error);
      showTableError("delivery-details-table", 6, "Error loading details");
    });
}

// Get status badge class based on status
function getStatusBadgeClass(status) {
  switch (status) {
    case "ASH":
      return "bg-primary"; // Blue - Assigned to delivery
    case "SHP":
      return "bg-success"; // Green - Shipped
    case "RCV":
      return "bg-info"; // Light blue - Received
    case "TSP":
      return "bg-warning"; // Yellow - In rack
    default:
      return "bg-secondary"; // Gray - Unknown or other status
  }
}
