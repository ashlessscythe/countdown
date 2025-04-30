/**
 * Users view functionality
 */

// Load users data
function loadUsersData() {
  // Check if the users view exists
  const usersView = document.getElementById("users-view");
  if (!usersView) return;

  fetch("/api/users")
    .then((response) => response.json())
    .then((users) => {
      const tableBody = document.getElementById("users-table");
      if (!tableBody) return;

      if (users.length === 0) {
        showTableError("users-table", 4, "No users found");
        return;
      }

      // Sort users by time since change (most recent first)
      users = sortByTimestamp(users);

      // Build table rows
      let html = "";
      users.forEach((user) => {
        html += `
          <tr>
            <td>${user.id || "N/A"}</td>
            <td>${user.count || 0}</td>
            <td>${user.time_since_change || "Unknown"}</td>
            <td>
              <i class="bi bi-eye action-btn" onclick="showUserDetails('${
                user.id
              }')"></i>
            </td>
          </tr>
        `;
      });

      tableBody.innerHTML = html;
    })
    .catch((error) => {
      console.error("Error fetching users data:", error);
      if (document.getElementById("users-table")) {
        showTableError("users-table", 4, "Error loading data");
      }
    });
}

// Show user details
function showUserDetails(userId) {
  // Check if the user details elements exist
  const userIdHeader = document.getElementById("user-id-header");
  const userDetailsCard = document.getElementById("user-details-card");

  if (!userIdHeader || !userDetailsCard) return;

  // Update header
  userIdHeader.textContent = userId;

  // Show details card
  userDetailsCard.classList.remove("d-none");

  // Show loading indicator
  showTableLoading("user-details-table", 6);

  // Load user details
  fetch(`/api/filter?type=user&value=${encodeURIComponent(userId)}`)
    .then((response) => response.json())
    .then((changes) => {
      const tableBody = document.getElementById("user-details-table");
      if (!tableBody) return;

      if (changes.length === 0) {
        showTableError("user-details-table", 6, "No details found");
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
      console.error("Error fetching user details:", error);
      if (document.getElementById("user-details-table")) {
        showTableError("user-details-table", 6, "Error loading details");
      }
    });
}
