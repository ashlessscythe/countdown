/**
 * Shipments view functionality
 */

// Load shipments data
function loadShipmentsData() {
  fetch("/api/shipments")
    .then((response) => response.json())
    .then((shipments) => {
      const tableBody = document.getElementById("shipments-table");

      if (shipments.length === 0) {
        showTableError("shipments-table", 5, "No shipments found");
        return;
      }

      // Sort shipments by time since change (most recent first)
      shipments = sortByTimestamp(shipments);

      // Build table rows
      let html = "";
      shipments.forEach((shipment) => {
        html += `
          <tr>
            <td>${shipment.id || "N/A"}</td>
            <td>${shipment.customer || "N/A"}</td>
            <td>${shipment.count || 0}</td>
            <td>${shipment.time_since_change || "Unknown"}</td>
            <td>
              <i class="bi bi-eye action-btn" onclick="showShipmentDetails('${
                shipment.id
              }')"></i>
            </td>
          </tr>
        `;
      });

      tableBody.innerHTML = html;
    })
    .catch((error) => {
      console.error("Error fetching shipments data:", error);
      showTableError("shipments-table", 5, "Error loading data");
    });
}

// Show shipment details
function showShipmentDetails(shipmentId) {
  // Update header
  document.getElementById("shipment-id-header").textContent = shipmentId;

  // Show details card
  document.getElementById("shipment-details-card").classList.remove("d-none");

  // Show loading indicator
  showTableLoading("shipment-details-table", 6);

  // Load shipment details
  fetch(`/api/filter?type=shipment&value=${shipmentId}`)
    .then((response) => response.json())
    .then((changes) => {
      const tableBody = document.getElementById("shipment-details-table");

      if (changes.length === 0) {
        showTableError("shipment-details-table", 6, "No details found");
        return;
      }

      // Sort changes by serial
      changes = sortBySerial(changes);

      // Build table rows
      let html = "";
      changes.forEach((change) => {
        html += `
          <tr>
            <td>${change.serial || "N/A"}</td>
            <td>${change.to || "N/A"}</td>
            <td>${change.from || "N/A"}</td>
            <td>${change.delivery || "N/A"}</td>
            <td>${change.customer_name || "N/A"}</td>
            <td>${change.time_since_change || "Unknown"}</td>
          </tr>
        `;
      });

      tableBody.innerHTML = html;
    })
    .catch((error) => {
      console.error("Error fetching shipment details:", error);
      showTableError("shipment-details-table", 6, "Error loading details");
    });
}
