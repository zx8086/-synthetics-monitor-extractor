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

// Simple in-memory rate limiter
class RateLimiter {
  private requests = new Map<string, { count: number; resetTime: number }>();

  isAllowed(clientId: string, maxRequests: number, windowMs: number): boolean {
    const now = Date.now();
    const client = this.requests.get(clientId);

    if (!client || now > client.resetTime) {
      // First request or window expired
      this.requests.set(clientId, {
        count: 1,
        resetTime: now + windowMs,
      });
      return true;
    }

    if (client.count >= maxRequests) {
      return false;
    }

    client.count++;
    return true;
  }

  getRemainingRequests(clientId: string, maxRequests: number): number {
    const client = this.requests.get(clientId);
    if (!client) return maxRequests;
    return Math.max(0, maxRequests - client.count);
  }

  getResetTime(clientId: string): number | null {
    const client = this.requests.get(clientId);
    return client ? client.resetTime : null;
  }
}

const rateLimiter = new RateLimiter();

// Get client identifier for rate limiting
function getClientId(req: Request): string {
  // Use X-Forwarded-For if available (for proxy scenarios)
  const forwardedFor = req.headers.get('x-forwarded-for');
  if (forwardedFor && forwardedFor.length > 0) {
    const parts = forwardedFor.split(',');
    return parts[0]?.trim() || 'default';
  }
  
  // Fallback to a default identifier
  return 'default';
}

// Compression utility function
function compressResponse(data: string, acceptEncoding: string | null, status: number = 200, cacheControl?: string): Response {
  const supportsGzip = acceptEncoding?.includes('gzip');
  const supportsBrotli = acceptEncoding?.includes('br');
  
  let compressedData: Uint8Array;
  let encoding: string;
  
  const headers: Record<string, string> = {
    "Content-Type": "application/json"
  };
  
  if (cacheControl) {
    headers["Cache-Control"] = cacheControl;
  }
  
  if (supportsBrotli) {
    compressedData = Bun.gzipSync(data);
    encoding = 'br';
  } else if (supportsGzip) {
    compressedData = Bun.gzipSync(data);
    encoding = 'gzip';
  } else {
    // No compression supported
    return new Response(data, {
      status,
      headers
    });
  }
  
  return new Response(compressedData, {
    status,
    headers: {
      ...headers,
      "Content-Encoding": encoding,
      "Content-Length": compressedData.length.toString()
    }
  });
}

// Add common headers to response
function addCommonHeaders(response: Response, req: Request, startTime: number, route: string, status: number): void {
  response.headers.set("Access-Control-Allow-Origin", "*");
  response.headers.set("Access-Control-Allow-Methods", "GET, DELETE, OPTIONS");
  response.headers.set("Access-Control-Allow-Headers", "Content-Type, Accept-Encoding");
  
  // Add performance headers
  const duration = performance.now() - startTime;
  response.headers.set("X-Response-Time", `${duration.toFixed(2)}ms`);
  response.headers.set("X-Request-ID", crypto.randomUUID());
  
  // Add rate limit headers if enabled
  if (config.api.rateLimit.enabled) {
    const clientId = getClientId(req);
    const remainingRequests = rateLimiter.getRemainingRequests(
      clientId,
      config.api.rateLimit.maxRequests
    );
    const resetTime = rateLimiter.getResetTime(clientId);
    
    response.headers.set("X-RateLimit-Remaining", remainingRequests.toString());
    if (resetTime) {
      response.headers.set("X-RateLimit-Reset", resetTime.toString());
    }
  }
  
  // Record response time
  recordHttpResponseTime(duration, route, status);
}

// Rate limiting middleware
function checkRateLimit(req: Request, startTime: number): Response | null {
  if (!config.api.rateLimit.enabled) return null;
  
  const clientId = getClientId(req);
  const isAllowed = rateLimiter.isAllowed(
    clientId,
    config.api.rateLimit.maxRequests,
    config.api.rateLimit.windowMs
  );

  if (!isAllowed) {
    const resetTime = rateLimiter.getResetTime(clientId);
    const acceptEncoding = req.headers.get('accept-encoding');
    const response = compressResponse(
      JSON.stringify({
        error: "Rate limit exceeded",
        message: "Too many requests",
        retryAfter: resetTime ? Math.ceil((resetTime - Date.now()) / 1000) : 60
      }),
      acceptEncoding,
      429
    );
    addCommonHeaders(response, req, startTime, req.url, 429);
    return response;
  }
  
  return null;
}

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
        "Access-Control-Allow-Methods": "GET, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
      };

      // Handle preflight requests
      if (req.method === "OPTIONS") {
        const response = new Response(null, { headers });
        recordHttpResponseTime(performance.now() - startTime, route, 200);
        return response;
      }

      // Prometheus metrics endpoint
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
          const response = Response.json({ error: "Invalid record ID" }, { status: 400, headers });
          recordHttpResponseTime(performance.now() - startTime, route, 400);
          return response;
        }

        const success = deleteInvalidRecord(id);
        const response = Response.json({ success }, { headers });
        recordHttpResponseTime(performance.now() - startTime, route, 200);
        return response;
      }

      // Only accept GET requests for API endpoints (except the delete endpoint)
      if (req.method !== "GET" && !url.pathname.startsWith("/ui")) {
        const response = Response.json({ error: "Method not allowed" }, { status: 405, headers });
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

          const response = Response.json(result, { headers });
          recordHttpResponseTime(performance.now() - startTime, route, 200);
          return response;
        } else if (
          url.pathname === `${config.api.invalidRecordsEndpoint}/summary`
        ) {
          const summary = getInvalidRecordsSummary();
          const response = Response.json(summary, { headers });
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
          const response = Response.json({
            message: "Synthetic Monitors API",
            endpoints: [
              config.api.invalidRecordsEndpoint,
              `${config.api.invalidRecordsEndpoint}?type=monitor_transformation`,
              `${config.api.invalidRecordsEndpoint}?monitor=monitor_name`,
              `${config.api.invalidRecordsEndpoint}/summary`,
              config.metrics.endpoint,
              config.api.uiEndpoint,
            ],
          }, { headers });
          recordHttpResponseTime(performance.now() - startTime, route, 200);
          return response;
        }

        const response = Response.json({ error: "Not found" }, { status: 404, headers });
        recordHttpResponseTime(performance.now() - startTime, route, 404);
        return response;
      } catch (error) {
        err("API error:", error);
        const response = Response.json({
          error: "Internal server error",
          message: error instanceof Error ? error.message : String(error),
        }, { status: 500, headers });
        recordHttpResponseTime(performance.now() - startTime, route, 500);
        return response;
      }
    },
  });

  log(`API server started on http://localhost:${port}`);
  return server;
}
