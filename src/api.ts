/* src/api.ts */

import { join } from "node:path";
import { context, SpanKind, SpanStatusCode, trace } from "@opentelemetry/api";
import type { Server } from "bun";
import { config } from "./config.js";
import {
	deleteInvalidRecord,
	getAllInvalidRecords,
	getInvalidRecordsByMonitor,
	getInvalidRecordsByType,
	getInvalidRecordsSummary,
} from "./database.js";
import {
	getMeter,
	getOpenTelemetryMetrics,
	recordHttpRequest,
	recordHttpResponseTime,
} from "./instrumentation-nodesdk.js";
import { registry } from "./metrics.js";
import { debug, err, log } from "./utils/logger.js";
import {
	validateConnections,
	formatValidationSummary,
} from "./utils/connectionValidator.js";

// Create a tracer for the API server
const tracer = trace.getTracer("api-server", "1.0.0");

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
	const forwardedFor = req.headers.get("x-forwarded-for");
	if (forwardedFor && forwardedFor.length > 0) {
		const parts = forwardedFor.split(",");
		return parts[0]?.trim() || "default";
	}

	// Fallback to a default identifier
	return "default";
}

// Compression utility function
function compressResponse(
	data: string,
	acceptEncoding: string | null,
	status: number = 200,
	cacheControl?: string,
): Response {
	const supportsGzip = acceptEncoding?.includes("gzip");
	const supportsBrotli = acceptEncoding?.includes("br");

	let compressedData: Uint8Array;
	let encoding: string;

	const headers: Record<string, string> = {
		"Content-Type": "application/json",
	};

	if (cacheControl) {
		headers["Cache-Control"] = cacheControl;
	}

	if (supportsBrotli) {
		compressedData = Bun.gzipSync(data);
		encoding = "br";
	} else if (supportsGzip) {
		compressedData = Bun.gzipSync(data);
		encoding = "gzip";
	} else {
		// No compression supported
		return new Response(data, {
			status,
			headers,
		});
	}

	return new Response(compressedData, {
		status,
		headers: {
			...headers,
			"Content-Encoding": encoding,
			"Content-Length": compressedData.length.toString(),
		},
	});
}

// Security configuration
const SECURITY_CONFIG = {
	maxRequestSize: 1024 * 1024, // 1MB
	maxUrlLength: 2048,
	maxHeaderSize: 8192,
	allowedOrigins: ["*"], // Configure as needed for production
	rateLimitByIP: true,
};

// Add common headers with security enhancements
function addCommonHeaders(
	response: Response,
	req: Request,
	startTime: number,
	route: string,
	status: number,
): void {
	// CORS headers
	response.headers.set("Access-Control-Allow-Origin", "*");
	response.headers.set("Access-Control-Allow-Methods", "GET, DELETE, OPTIONS");
	response.headers.set(
		"Access-Control-Allow-Headers",
		"Content-Type, Accept-Encoding, Content-Length",
	);

	// Security headers
	response.headers.set("X-Content-Type-Options", "nosniff");
	response.headers.set("X-Frame-Options", "DENY");
	response.headers.set("X-XSS-Protection", "1; mode=block");
	response.headers.set("Referrer-Policy", "strict-origin-when-cross-origin");
	response.headers.set(
		"Content-Security-Policy",
		"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'",
	);

	// Prevent caching of sensitive endpoints
	if (route.includes("/api/")) {
		response.headers.set(
			"Cache-Control",
			"no-store, no-cache, must-revalidate, private",
		);
		response.headers.set("Pragma", "no-cache");
		response.headers.set("Expires", "0");
	}

	// Add performance headers
	const duration = performance.now() - startTime;
	response.headers.set("X-Response-Time", `${duration.toFixed(2)}ms`);
	response.headers.set("X-Request-ID", crypto.randomUUID());

	// Add rate limit headers if enabled
	if (config.api.rateLimit.enabled) {
		const clientId = getClientId(req);
		const remainingRequests = rateLimiter.getRemainingRequests(
			clientId,
			config.api.rateLimit.maxRequests,
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

// Input validation middleware
function validateRequest(req: Request, startTime: number): Response | null {
	const url = new URL(req.url);

	// Check URL length
	if (req.url.length > SECURITY_CONFIG.maxUrlLength) {
		const response = Response.json(
			{
				error: "Request URL too long",
				maxLength: SECURITY_CONFIG.maxUrlLength,
			},
			{ status: 414 },
		);
		addCommonHeaders(response, req, startTime, url.pathname, 414);
		return response;
	}

	// Check for suspicious patterns in URL
	const suspiciousPatterns = [
		/<script/i,
		/javascript:/i,
		/data:/i,
		/vbscript:/i,
		/<iframe/i,
		/\.\.\//,
		/\/\.\./,
	];

	for (const pattern of suspiciousPatterns) {
		if (pattern.test(req.url)) {
			log(`Blocked suspicious request: ${req.url}`, {
				security_event: "suspicious_request_blocked",
				client_ip: req.headers.get("x-forwarded-for") || "unknown",
				user_agent: req.headers.get("user-agent") || "unknown",
			});
			const response = Response.json(
				{ error: "Invalid request" },
				{ status: 400 },
			);
			addCommonHeaders(response, req, startTime, url.pathname, 400);
			return response;
		}
	}

	// Validate Content-Length if present
	const contentLength = req.headers.get("content-length");
	if (
		contentLength &&
		parseInt(contentLength) > SECURITY_CONFIG.maxRequestSize
	) {
		const response = Response.json(
			{
				error: "Request entity too large",
				maxSize: SECURITY_CONFIG.maxRequestSize,
			},
			{ status: 413 },
		);
		addCommonHeaders(response, req, startTime, url.pathname, 413);
		return response;
	}

	return null;
}

// Rate limiting middleware
function checkRateLimit(req: Request, startTime: number): Response | null {
	if (!config.api.rateLimit.enabled) return null;

	const clientId = getClientId(req);
	const isAllowed = rateLimiter.isAllowed(
		clientId,
		config.api.rateLimit.maxRequests,
		config.api.rateLimit.windowMs,
	);

	if (!isAllowed) {
		const resetTime = rateLimiter.getResetTime(clientId);
		const acceptEncoding = req.headers.get("accept-encoding");
		const response = compressResponse(
			JSON.stringify({
				error: "Rate limit exceeded",
				message: "Too many requests",
				retryAfter: resetTime ? Math.ceil((resetTime - Date.now()) / 1000) : 60,
			}),
			acceptEncoding,
			429,
		);
		addCommonHeaders(response, req, startTime, req.url, 429);
		return response;
	}

	return null;
}

// Helper function to execute request handler within a span context
async function executeWithSpan<T>(
	spanName: string,
	attributes: Record<string, any>,
	fn: () => Promise<T>,
): Promise<T> {
	// Check if OpenTelemetry is enabled
	if (!config.openTelemetry.enabled) {
		// If disabled, just execute the function without instrumentation
		return fn();
	}

	const span = tracer.startSpan(spanName, {
		kind: SpanKind.SERVER,
		attributes,
	});

	// Log span creation in debug mode
	debug(`Created span: ${spanName} with ID: ${span.spanContext().spanId}`);

	return context.with(trace.setSpan(context.active(), span), async () => {
		try {
			const result = await fn();
			span.setStatus({ code: SpanStatusCode.OK });
			return result;
		} catch (error) {
			span.setStatus({
				code: SpanStatusCode.ERROR,
				message: error instanceof Error ? error.message : String(error),
			});
			if (error instanceof Error) {
				span.recordException(error);
			}
			throw error;
		} finally {
			// Log span completion
			debug(`Ending span: ${spanName} with ID: ${span.spanContext().spanId}`);
			span.end();
		}
	});
}

export function startApiServer(port: number = config.metrics.port): Server {
	const server = Bun.serve({
		port,
		async fetch(req) {
			const startTime = performance.now();
			const url = new URL(req.url);
			const method = req.method;
			const route = url.pathname;

			// Create initial span attributes
			const spanAttributes = {
				"http.method": method,
				"http.scheme": url.protocol.replace(":", ""),
				"http.host": url.host,
				"http.target": url.pathname + url.search,
				"http.url": req.url,
				"http.route": route,
				"net.peer.ip": req.headers.get("x-forwarded-for") || "unknown",
				"http.user_agent": req.headers.get("user-agent") || "unknown",
			};

			// Execute the entire request within a span
			return executeWithSpan(`${method} ${route}`, spanAttributes, async () => {
				// Record incoming request inside the span context
				recordHttpRequest(method, route);

				// Validate request security
				const validationError = validateRequest(req, startTime);
				if (validationError) {
					return validationError;
				}

				// Check rate limiting
				const rateLimitError = checkRateLimit(req, startTime);
				if (rateLimitError) {
					return rateLimitError;
				}

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
					// Add response attributes to current span
					const span = trace.getActiveSpan();
					if (span) {
						span.setAttributes({
							"http.status_code": 200,
							"http.response_content_length": 0,
						});
					}
					return response;
				}

				// Prometheus metrics endpoint (includes OpenTelemetry metrics)
				if (url.pathname === config.metrics.endpoint) {
					try {
						// Get prom-client metrics
						const promMetrics = await registry.metrics();

						// Get OpenTelemetry metrics
						const otelMetrics = await getOpenTelemetryMetrics();

						// Combine metrics
						const combinedMetrics = otelMetrics
							? `${promMetrics}\n# OpenTelemetry metrics\n${otelMetrics}`
							: promMetrics;

						const response = new Response(combinedMetrics, {
							headers: { "Content-Type": registry.contentType },
						});
						recordHttpResponseTime(performance.now() - startTime, route, 200);
						// Add response attributes to current span
						const span = trace.getActiveSpan();
						if (span) {
							span.setAttributes({
								"http.status_code": 200,
								"http.response_content_length": combinedMetrics.length,
								"http.response_content_type": registry.contentType,
							});
						}
						return response;
					} catch (error) {
						err("Error generating metrics:", error);
						const response = new Response("Internal Server Error", {
							status: 500,
						});
						recordHttpResponseTime(performance.now() - startTime, route, 500);
						// Add error attributes to current span
						const span = trace.getActiveSpan();
						if (span) {
							span.setAttributes({
								"http.status_code": 500,
								"error.type":
									error instanceof Error ? error.constructor.name : "Error",
								"error.message":
									error instanceof Error ? error.message : String(error),
							});
						}
						return response;
					}
				}

				// Handle DELETE requests for the delete endpoint
				if (
					req.method === "DELETE" &&
					url.pathname.startsWith(
						`${config.api.invalidRecordsEndpoint}/delete/`,
					)
				) {
					const idStr = url.pathname.split("/").pop();
					const id = parseInt(idStr || "", 10);

					if (Number.isNaN(id)) {
						const response = Response.json(
							{ error: "Invalid record ID" },
							{ status: 400, headers },
						);
						recordHttpResponseTime(performance.now() - startTime, route, 400);
						const span = trace.getActiveSpan();
						if (span) {
							span.setAttributes({
								"http.status_code": 400,
								"error.type": "ValidationError",
								"error.message": "Invalid record ID",
							});
						}
						return response;
					}

					const success = deleteInvalidRecord(id);
					const response = Response.json({ success }, { headers });
					recordHttpResponseTime(performance.now() - startTime, route, 200);
					const span = trace.getActiveSpan();
					if (span) {
						span.setAttributes({
							"http.status_code": 200,
							"api.record_id": id,
							"api.delete_success": success,
						});
					}
					return response;
				}

				// Only accept GET requests for API endpoints (except the delete endpoint)
				if (req.method !== "GET" && !url.pathname.startsWith("/ui")) {
					const response = Response.json(
						{ error: "Method not allowed" },
						{ status: 405, headers },
					);
					recordHttpResponseTime(performance.now() - startTime, route, 405);
					const span = trace.getActiveSpan();
					if (span) {
						span.setAttributes({
							"http.status_code": 405,
							"error.type": "MethodNotAllowed",
						});
					}
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
						const span = trace.getActiveSpan();
						if (span) {
							span.setAttributes({
								"http.status_code": 200,
								"api.query_type":
									type || monitorName
										? type
											? "by_type"
											: "by_monitor"
										: "all",
								"api.result_count": Array.isArray(result) ? result.length : 1,
							});
						}
						return response;
					} else if (
						url.pathname === `${config.api.invalidRecordsEndpoint}/summary`
					) {
						const summary = getInvalidRecordsSummary();
						const response = Response.json(summary, { headers });
						recordHttpResponseTime(performance.now() - startTime, route, 200);
						const span = trace.getActiveSpan();
						if (span) {
							span.setAttributes({
								"http.status_code": 200,
								"api.endpoint_type": "summary",
							});
						}
						return response;
					} else if (url.pathname === config.api.uiEndpoint) {
						// Format the KAFKA_CLIENT_ID for use in the title:
						// Special handling for synthetics-extractor prefix
						const formatTitlePrefix = (clientId: string) => {
							// Handle synthetics-extractor prefix specially
							if (clientId.startsWith("synthetics-extractor")) {
								// Remove the synthetics-extractor prefix
								const remaining = clientId.slice("synthetics-extractor".length);

								// Check if there's anything after synthetics-extractor
								if (remaining.length === 0) {
									return "Synthetics Extractor";
								}

								// Remove the leading hyphen and uppercase the rest
								const suffix = remaining.slice(1).toUpperCase();

								return `Synthetics Extractor ${suffix}`;
							}

							// Fallback to original logic for other patterns
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
						// In production, the public folder is in src/public, not dist/public
						const publicPath =
							process.env.NODE_ENV === "production"
								? join(process.cwd(), "src", "public", "index.html")
								: join(import.meta.dir, "public", "index.html");

						const htmlContent = await Bun.file(publicPath).text();

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
						const span = trace.getActiveSpan();
						if (span) {
							span.setAttributes({
								"http.status_code": 200,
								"http.response_content_type": "text/html",
								"api.ui_title_prefix": titlePrefix,
							});
						}
						return response;
					} else if (url.pathname === "/health") {
						try {
							// Validate all external service connections
							const summary = await validateConnections();

							const baseResponse = {
								timestamp: new Date().toISOString(),
								service: config.openTelemetry.serviceName,
								version:
									config.openTelemetry.serviceVersion ||
									process.env.BUILD_VERSION ||
									"unknown",
								environment: process.env.NODE_ENV || "development",
							};

							// Extract service status from results
							const elasticsearch = summary.results.find(
								(r) => r.service === "Elasticsearch",
							);
							const kafka = summary.results.find((r) => r.service === "Kafka");
							const opentelemetry = summary.results.filter((r) =>
								r.service.startsWith("OpenTelemetry"),
							);

							if (summary.allConnected) {
								const response = Response.json(
									{
										status: "OK",
										...baseResponse,
										checks: {
											elasticsearch: elasticsearch?.connected ? "OK" : "KO",
											kafka: kafka?.connected ? "OK" : "KO",
											opentelemetry:
												opentelemetry.length > 0 &&
												opentelemetry.every((o) => o.connected)
													? "OK"
													: "KO",
										},
									},
									{ headers },
								);
								recordHttpResponseTime(
									performance.now() - startTime,
									route,
									200,
								);
								const span = trace.getActiveSpan();
								if (span) {
									span.setAttributes({
										"http.status_code": 200,
										"api.endpoint_type": "health",
										"health.status": "OK",
									});
								}
								return response;
							} else {
								const failedServices = summary.results.filter(
									(r) => !r.connected,
								);
								const response = Response.json(
									{
										status: "KO",
										...baseResponse,
										checks: {
											elasticsearch: elasticsearch?.connected ? "OK" : "KO",
											kafka: kafka?.connected ? "OK" : "KO",
											opentelemetry:
												opentelemetry.length > 0 &&
												opentelemetry.every((o) => o.connected)
													? "OK"
													: "KO",
										},
										errors: failedServices.map((s) => s.error).filter(Boolean),
									},
									{ status: 503, headers },
								);
								recordHttpResponseTime(
									performance.now() - startTime,
									route,
									503,
								);
								const span = trace.getActiveSpan();
								if (span) {
									span.setAttributes({
										"http.status_code": 503,
										"api.endpoint_type": "health",
										"health.status": "KO",
										"health.failed_services": failedServices
											.map((s) => s.service.toLowerCase())
											.join(","),
									});
								}
								return response;
							}
						} catch (error) {
							const response = Response.json(
								{
									status: "KO",
									timestamp: new Date().toISOString(),
									service: config.openTelemetry.serviceName,
									version:
										config.openTelemetry.serviceVersion ||
										process.env.BUILD_VERSION ||
										"unknown",
									environment: process.env.NODE_ENV || "development",
									error:
										error instanceof Error ? error.message : "Unknown error",
								},
								{ status: 503, headers },
							);
							recordHttpResponseTime(performance.now() - startTime, route, 503);
							const span = trace.getActiveSpan();
							if (span) {
								span.setAttributes({
									"http.status_code": 503,
									"api.endpoint_type": "health",
									"health.status": "KO",
									"error.type":
										error instanceof Error ? error.constructor.name : "Error",
								});
							}
							return response;
						}
					} else if (url.pathname === "/api/validate-connections") {
						// Validate all external service connections
						const summary = await validateConnections();

						const response = Response.json(
							{
								...summary,
								formattedSummary: formatValidationSummary(summary),
							},
							{ headers },
						);
						recordHttpResponseTime(performance.now() - startTime, route, 200);
						const span = trace.getActiveSpan();
						if (span) {
							span.setAttributes({
								"http.status_code": 200,
								"api.endpoint_type": "validate_connections",
								"api.all_connected": summary.allConnected,
								"api.services_checked": summary.results.length,
							});
						}
						return response;
					} else if (url.pathname === "/" || url.pathname === "/api") {
						const response = Response.json(
							{
								message: "Synthetic Monitors API",
								endpoints: [
									"/health",
									"/api/validate-connections",
									config.api.invalidRecordsEndpoint,
									`${config.api.invalidRecordsEndpoint}?type=monitor_transformation`,
									`${config.api.invalidRecordsEndpoint}?monitor=monitor_name`,
									`${config.api.invalidRecordsEndpoint}/summary`,
									config.metrics.endpoint,
									config.api.uiEndpoint,
								],
							},
							{ headers },
						);
						recordHttpResponseTime(performance.now() - startTime, route, 200);
						const span = trace.getActiveSpan();
						if (span) {
							span.setAttributes({
								"http.status_code": 200,
								"api.endpoint_type": "root",
							});
						}
						return response;
					}

					const response = Response.json(
						{ error: "Not found" },
						{ status: 404, headers },
					);
					recordHttpResponseTime(performance.now() - startTime, route, 404);
					const span = trace.getActiveSpan();
					if (span) {
						span.setAttributes({
							"http.status_code": 404,
							"error.type": "NotFound",
						});
					}
					return response;
				} catch (error) {
					err("API error:", error);
					const response = Response.json(
						{
							error: "Internal server error",
							message: error instanceof Error ? error.message : String(error),
						},
						{ status: 500, headers },
					);
					recordHttpResponseTime(performance.now() - startTime, route, 500);
					const span = trace.getActiveSpan();
					if (span) {
						span.setAttributes({
							"http.status_code": 500,
							"error.type":
								error instanceof Error ? error.constructor.name : "Error",
							"error.message":
								error instanceof Error ? error.message : String(error),
						});
						if (error instanceof Error) {
							span.recordException(error);
						}
					}
					return response;
				}
			});
		},
	});

	log(`API server started on http://localhost:${port}`);
	return server;
}
