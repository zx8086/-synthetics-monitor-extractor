/* src/utils/connectionValidator.ts */

import { config } from "../config.js";
import { getElasticsearchClient } from "../elasticsearch.js";
import { checkKafkaConnection } from "../kafka.js";
import { err, log, warn } from "./logger.js";

interface ValidationResult {
	service: string;
	connected: boolean;
	details?: any;
	error?: string;
	latency?: number;
}

interface ValidationSummary {
	allConnected: boolean;
	results: ValidationResult[];
	timestamp: string;
}

/**
 * Validates Elasticsearch connection
 */
async function validateElasticsearch(): Promise<ValidationResult> {
	const startTime = Date.now();

	try {
		log("Validating Elasticsearch connection...");
		const client = getElasticsearchClient();

		// Perform cluster health check
		const health = await client.cluster.health();
		const info = await client.info();

		const latency = Date.now() - startTime;

		// Check cluster health status
		const isHealthy = health.status !== "red";

		if (!isHealthy) {
			warn("Elasticsearch cluster is not healthy", { status: health.status });
		}

		return {
			service: "Elasticsearch",
			connected: true,
			latency,
			details: {
				cluster_name: health.cluster_name,
				status: health.status,
				node_count: health.number_of_nodes,
				data_nodes: health.number_of_data_nodes,
				active_shards: health.active_shards,
				version: info.version.number,
				tagline: info.tagline,
			},
		};
	} catch (error: any) {
		err("Failed to validate Elasticsearch connection", {
			error: error.message,
			code: error.code,
			statusCode: error.statusCode,
		});

		return {
			service: "Elasticsearch",
			connected: false,
			error: error.message || "Unknown error",
			details: {
				endpoint: config.elasticsearch.node,
				error_type: error.name,
				status_code: error.statusCode,
			},
		};
	}
}

/**
 * Validates Kafka connection
 */
async function validateKafka(): Promise<ValidationResult> {
	const startTime = Date.now();

	try {
		log("Validating Kafka connection...");
		const result = await checkKafkaConnection();
		const latency = Date.now() - startTime;

		return {
			service: "Kafka",
			connected: result.connected,
			latency,
			details: {
				brokers: config.kafka.brokers,
				configured_topic: config.kafka.topicName,
				topic_exists: result.topics.includes(config.kafka.topicName),
			},
		};
	} catch (error: any) {
		err("Failed to validate Kafka connection", {
			error: error.message,
			code: error.code,
		});

		return {
			service: "Kafka",
			connected: false,
			error: error.message || "Unknown error",
			details: {
				brokers: config.kafka.brokers,
				error_type: error.name,
			},
		};
	}
}

/**
 * Validates OpenTelemetry endpoints
 */
async function validateOpenTelemetry(): Promise<ValidationResult[]> {
	const results: ValidationResult[] = [];

	if (!config.openTelemetry.enabled) {
		log("OpenTelemetry is disabled, skipping validation");
		return [
			{
				service: "OpenTelemetry",
				connected: false,
				details: { reason: "OpenTelemetry is disabled in configuration" },
			},
		];
	}

	// Validate each endpoint
	const endpoints = [
		{ name: "Traces", url: config.openTelemetry.tracesEndpoint },
		{ name: "Metrics", url: config.openTelemetry.metricsEndpoint },
		{ name: "Logs", url: config.openTelemetry.logsEndpoint },
	];

	for (const endpoint of endpoints) {
		const startTime = Date.now();

		try {
			log(`Validating OpenTelemetry ${endpoint.name} endpoint...`);

			// Validate the actual configured endpoint using HTTP/JSON
			// OTLP HTTP endpoints on port 4318 use JSON format
			const response = await fetch(endpoint.url, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
				},
				body: JSON.stringify({}), // Empty JSON body for validation
				signal: AbortSignal.timeout(5000), // 5 second timeout
			});

			const latency = Date.now() - startTime;

			// OTLP endpoints typically return 400 for empty/invalid payloads
			// 404/405 for wrong paths, 5xx for server errors
			// We consider <500 as "connected" since it means the endpoint is reachable
			const isConnected = response.status < 500;

			results.push({
				service: `OpenTelemetry ${endpoint.name}`,
				connected: isConnected,
				latency,
				details: {
					endpoint: endpoint.url,
					status_code: response.status,
					status_text: response.statusText,
				},
			});
		} catch (error: any) {
			const errorMessage = error.message || "Unknown error";

			// Check if it's a connection error vs other errors
			const isConnectionError =
				errorMessage.includes("ECONNREFUSED") ||
				errorMessage.includes("ENOTFOUND") ||
				errorMessage.includes("ETIMEDOUT") ||
				errorMessage.includes("fetch failed");

			results.push({
				service: `OpenTelemetry ${endpoint.name}`,
				connected: false,
				error: errorMessage,
				details: {
					endpoint: endpoint.url,
					error_type: error.name,
					is_connection_error: isConnectionError,
				},
			});
		}
	}

	return results;
}

/**
 * Validates all external service connections
 */
export async function validateConnections(): Promise<ValidationSummary> {
	log("Starting connection validation for all services...");

	const results: ValidationResult[] = [];

	// Validate Elasticsearch
	results.push(await validateElasticsearch());

	// Validate Kafka
	results.push(await validateKafka());

	// Validate OpenTelemetry endpoints
	const otelResults = await validateOpenTelemetry();
	results.push(...otelResults);

	// Determine if all critical services are connected
	const criticalServices = results.filter(
		(r) => r.service === "Elasticsearch" || r.service === "Kafka",
	);
	const allCriticalConnected = criticalServices.every((r) => r.connected);

	// Log summary
	const summary: ValidationSummary = {
		allConnected: allCriticalConnected,
		results,
		timestamp: new Date().toISOString(),
	};

	// Log results
	for (const result of results) {
		if (result.connected) {
			log(`✅ ${result.service} connected successfully`, {
				latency: result.latency,
				details: result.details,
			});
		} else {
			const logFn = result.service.includes("OpenTelemetry") ? warn : err;
			logFn(`❌ ${result.service} connection failed`, {
				error: result.error,
				details: result.details,
			});
		}
	}

	// Overall summary
	if (allCriticalConnected) {
		log("✅ All critical services connected successfully");
	} else {
		err("❌ Some critical services failed to connect");
	}

	return summary;
}

/**
 * Validates connections and exits if critical services are unavailable
 */
export async function validateConnectionsOrExit(): Promise<void> {
	const summary = await validateConnections();

	if (!summary.allConnected) {
		err("Critical service connections failed. Exiting...");
		process.exit(1);
	}
}

/**
 * Formats validation summary for display
 */
export function formatValidationSummary(summary: ValidationSummary): string {
	let output = `\nConnection Validation Summary (${summary.timestamp})\n`;
	output += "=".repeat(60) + "\n\n";

	for (const result of summary.results) {
		const status = result.connected ? "✅ CONNECTED" : "❌ FAILED";
		output += `${result.service}: ${status}\n`;

		if (result.latency) {
			output += `  Latency: ${result.latency}ms\n`;
		}

		if (result.error) {
			output += `  Error: ${result.error}\n`;
		}

		if (result.details) {
			output += `  Details: ${JSON.stringify(result.details, null, 2)}\n`;
		}

		output += "\n";
	}

	output += "=".repeat(60) + "\n";
	output += `Overall Status: ${summary.allConnected ? "✅ All critical services connected" : "❌ Some critical services failed"}\n`;

	return output;
}
