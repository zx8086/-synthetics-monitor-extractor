/* mock-otel-server.js */

import { createServer } from "node:http";
import { writeFileSync, appendFileSync } from "node:fs";

const PORT = 4318;
const LOG_FILE = "./otel-logs.jsonl";

// Create log file if it doesn't exist
try {
  writeFileSync(LOG_FILE, "");
} catch (error) {
  console.log("Log file already exists");
}

const server = createServer((req, res) => {
  const { method, url } = req;
  
  console.log(`${method} ${url}`);
  
  // Set CORS headers
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  
  if (method === "OPTIONS") {
    res.writeHead(200);
    res.end();
    return;
  }
  
  if (method === "POST") {
    let body = "";
    
    req.on("data", (chunk) => {
      body += chunk.toString();
    });
    
    req.on("end", () => {
      try {
        const data = JSON.parse(body);
        const timestamp = new Date().toISOString();
        
        // Log to console
        console.log(`[${timestamp}] Received ${url}:`, JSON.stringify(data, null, 2));
        
        // Write to file
        appendFileSync(LOG_FILE, JSON.stringify({
          timestamp,
          endpoint: url,
          data
        }) + "\n");
        
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "success" }));
      } catch (error) {
        console.error("Error processing request:", error);
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Invalid JSON" }));
      }
    });
  } else {
    res.writeHead(404);
    res.end("Not found");
  }
});

server.listen(PORT, () => {
  console.log(`Mock OpenTelemetry server running on http://localhost:${PORT}`);
  console.log(`Logs will be written to: ${LOG_FILE}`);
}); 