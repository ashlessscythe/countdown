const { createProxyMiddleware } = require("http-proxy-middleware");

module.exports = function (app) {
  // Proxy API requests to the backend
  app.use(
    "/api",
    createProxyMiddleware({
      target: "http://localhost:8080",
      changeOrigin: true,
      ws: true, // Enable WebSocket proxying
      timeout: 60000, // Increase timeout to 60 seconds
      proxyTimeout: 60000, // Increase proxy timeout to 60 seconds
    })
  );

  // Handle missing asset files to prevent proxy errors
  app.use("/logo192.png", (req, res) => {
    res.status(204).end(); // Return empty response with 204 No Content
  });

  app.use("/logo512.png", (req, res) => {
    res.status(204).end(); // Return empty response with 204 No Content
  });

  app.use("/favicon.ico", (req, res) => {
    res.status(204).end(); // Return empty response with 204 No Content
  });
};
