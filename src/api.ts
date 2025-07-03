/* src/api.ts */

import type { Server } from "bun";
import { join } from "node:path";
import { config } from "./config.js";
import { registry } from "./metrics.js";
import {
  deleteInvalidRecord,
  getAllInvalidRecords,
  getInvalidRecordsByMonitor,
  getInvalidRecordsByType,
  getInvalidRecordsSummary,
} from "./database.js";
import { recordHttpRequest, recordHttpResponseTime } from "./instrumentation.js";
import { log, err } from "./utils/logger.js";

export function startApiServer(port: number = config.metrics.port): Server {
  const server = Bun.serve({
    port,
    async fetch(req) {
      const startTime = performance.now();
      const url = new URL(req.url);
      const method = req.method;
      const route = url.pathname;

      // Record incoming request
      recordHttpRequest(method, route);

      // Enable CORS
      const headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
      };

      // Handle preflight requests
      if (req.method === "OPTIONS") {
        const response = new Response(null, { headers });
        // Record response time
        recordHttpResponseTime(performance.now() - startTime, route, 200);
        return response;
      }

      // Check if this is a Prometheus metrics request
      if (url.pathname === config.metrics.endpoint) {
        try {
          const metrics = await registry.metrics();
          const response = new Response(metrics, {
            headers: { "Content-Type": registry.contentType },
          });
          recordHttpResponseTime(performance.now() - startTime, route, 200);
          return response;
        } catch (error) {
          err("Error generating metrics:", error);
          const response = new Response("Internal Server Error", { status: 500 });
          recordHttpResponseTime(performance.now() - startTime, route, 500);
          return response;
        }
      }

      // Handle DELETE requests for the delete endpoint
      if (
        req.method === "DELETE" &&
        url.pathname.startsWith(`${config.api.invalidRecordsEndpoint}/delete/`)
      ) {
        const idStr = url.pathname.split("/").pop();
        const id = parseInt(idStr || "", 10);

        if (Number.isNaN(id)) {
          const response = new Response(JSON.stringify({ error: "Invalid record ID" }), {
            status: 400,
            headers,
          });
          recordHttpResponseTime(performance.now() - startTime, route, 400);
          return response;
        }

        const success = deleteInvalidRecord(id);
        const response = new Response(JSON.stringify({ success }), { headers });
        recordHttpResponseTime(performance.now() - startTime, route, 200);
        return response;
      }

      // Only accept GET requests for API endpoints (except the delete endpoint)
      if (req.method !== "GET" && !url.pathname.startsWith("/ui")) {
        const response = new Response(JSON.stringify({ error: "Method not allowed" }), {
          status: 405,
          headers,
        });
        recordHttpResponseTime(performance.now() - startTime, route, 405);
        return response;
      }

      // Route handling
      try {
        if (url.pathname === config.api.invalidRecordsEndpoint) {
          // Query parameters
          const type = url.searchParams.get("type");
          const monitorName = url.searchParams.get("monitor");

          let result: unknown;

          if (type) {
            result = getInvalidRecordsByType(type);
          } else if (monitorName) {
            result = getInvalidRecordsByMonitor(monitorName);
          } else {
            result = getAllInvalidRecords();
          }

          const response = new Response(JSON.stringify(result), { headers });
          recordHttpResponseTime(performance.now() - startTime, route, 200);
          return response;
        } else if (
          url.pathname === `${config.api.invalidRecordsEndpoint}/summary`
        ) {
          const summary = getInvalidRecordsSummary();
          const response = new Response(JSON.stringify(summary), { headers });
          recordHttpResponseTime(performance.now() - startTime, route, 200);
          return response;
        } else if (url.pathname === config.api.uiEndpoint) {
          // Format the KAFKA_CLIENT_ID for use in the title:
          // 1. Replace hyphens with spaces
          // 2. Capitalize each word
          const formatTitlePrefix = (clientId: string) => {
            return clientId
              .split("-")
              .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
              .join(" ");
          };

          // Get the formatted client ID or fallback to "Synthetics" if not available
          const titlePrefix = config.kafka.clientId
            ? formatTitlePrefix(config.kafka.clientId)
            : "Synthetics";

          // Get the HTML content as text
          const htmlContent = await Bun.file(
            join(import.meta.dir, "public", "index.html"),
          ).text();

          // Replace both the title tag and the h1 tag content
          const modifiedHtml = htmlContent
            .replace(
              /<title>Synthetics Monitor Errors<\/title>/,
              `<title>${titlePrefix} Monitor Errors</title>`,
            )
            .replace(
              /<h1>Synthetics Monitor Errors<\/h1>/,
              `<h1>${titlePrefix} Monitor Errors</h1>`,
            );

          const response = new Response(modifiedHtml, {
            headers: { "Content-Type": "text/html" },
          });
          recordHttpResponseTime(performance.now() - startTime, route, 200);
          return response;
        } else if (url.pathname === "/" || url.pathname === "/api") {
          const response = new Response(
            JSON.stringify({
              message: "Synthetic Monitors API",
              endpoints: [
                config.api.invalidRecordsEndpoint,
                `${config.api.invalidRecordsEndpoint}?type=monitor_transformation`,
                `${config.api.invalidRecordsEndpoint}?monitor=monitor_name`,
                `${config.api.invalidRecordsEndpoint}/summary`,
                config.metrics.endpoint,
                config.api.uiEndpoint,
              ],
            }),
            { headers },
          );
          recordHttpResponseTime(performance.now() - startTime, route, 200);
          return response;
        }

        const response = new Response(JSON.stringify({ error: "Not found" }), {
          status: 404,
          headers,
        });
        recordHttpResponseTime(performance.now() - startTime, route, 404);
        return response;
      } catch (error) {
        err("API error:", error);
        const response = new Response(
          JSON.stringify({
            error: "Internal server error",
            message: error instanceof Error ? error.message : String(error),
          }),
          {
            status: 500,
            headers,
          },
        );
        recordHttpResponseTime(performance.now() - startTime, route, 500);
        return response;
      }
    },
  });

  log(`API server started on http://localhost:${port}`);
  return server;
}
