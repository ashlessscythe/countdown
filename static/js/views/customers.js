/**
 * Customers view functionality
 */

// Load customers data
function loadCustomersData() {
  fetch("/api/customers")
    .then((response) => response.json())
    .then((customers) => {
      const tableBody = document.getElementById("customers-table");

      if (customers.length === 0) {
        showTableError("customers-table", 4, "No customers found");
        return;
      }

      // Sort customers by time since change (most recent first)
      customers = sortByTimestamp(customers);

      // Build table rows
      let html = "";
      customers.forEach((customer) => {
        html += `
          <tr>
            <td>${customer.name || "N/A"}</td>
            <td>${customer.count || 0}</td>
            <td>${customer.time_since_change || "Unknown"}</td>
            <td>
              <i class="bi bi-eye action-btn" onclick="showCustomerDetails('${
                customer.name
              }')"></i>
            </td>
          </tr>
        `;
      });

      tableBody.innerHTML = html;
    })
    .catch((error) => {
      console.error("Error fetching customers data:", error);
      showTableError("customers-table", 4, "Error loading data");
    });
}

// Show customer details
function showCustomerDetails(customerName) {
  // Update header
  document.getElementById("customer-name-header").textContent = customerName;

  // Show details card
  document.getElementById("customer-details-card").classList.remove("d-none");

  // Show loading indicator
  showTableLoading("customer-details-table", 6);

  // Load customer details
  fetch(`/api/filter?type=customer&value=${encodeURIComponent(customerName)}`)
    .then((response) => response.json())
    .then((changes) => {
      const tableBody = document.getElementById("customer-details-table");

      if (changes.length === 0) {
        showTableError("customer-details-table", 6, "No details found");
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
            <td>${change.shipment_number || "N/A"}</td>
            <td>${change.time_since_change || "Unknown"}</td>
          </tr>
        `;
      });

      tableBody.innerHTML = html;
    })
    .catch((error) => {
      console.error("Error fetching customer details:", error);
      showTableError("customer-details-table", 6, "Error loading details");
    });
}
