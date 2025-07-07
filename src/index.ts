/* src/index.ts */

// Initialize OpenTelemetry FIRST before any other imports
console.log("Starting application - initializing OpenTelemetry");

import { initializeOpenTelemetry } from "./instrumentation-nodesdk.js";

const otelSdk = initializeOpenTelemetry();
console.log("OpenTelemetry initialized");

import { Client, type estypes } from "@elastic/elasticsearch";
import { HttpConnection } from "@elastic/transport";
import { startApiServer } from "./api.js"; // Using the updated API server with OpenTelemetry logging
import { config } from "./config.js";
import { closeDatabase, initializeDatabase } from "./database.js";
import {
	checkElasticsearchHealth,
	closeElasticsearchClient,
	executeSearch,
	getElasticsearchClient,
} from "./elasticsearch.js";
import {
	checkKafkaConnection,
	closeKafkaProducer,
	getKafkaProducer,
	sendMonitorDataToKafka,
} from "./kafka.js";
import {
	initializeMetrics,
	kafkaMessageSizeHistogram,
	registry,
	startMetricsServer,
} from "./metrics.js";
import type { ElasticsearchHit, MonitorInfo, SourceDocument } from "./types";
import type { InvalidRecord } from "./types.js";
import {
	type BusinessContext,
	ElasticsearchSourceSchema,
	type SearchResponse,
	validateElasticsearchHits,
	validateMonitorInfo,
	writeInvalidRecords,
} from "./types.js";
import { ConfigManager } from "./utils/configManager.js";
import { debug, err, log, warn } from "./utils/logger.js";
import {
	ParallelProcessor,
	StreamingProcessor,
} from "./utils/parallelProcessor.js";
import {
	validateConnectionsOrExit,
	formatValidationSummary,
} from "./utils/connectionValidator.js";

log("Initializing metrics...");
console.log("Initializing metrics...");
initializeMetrics();
log("Metrics initialized");
console.log("Metrics initialized");

// Initialize configuration manager
const configManager = new ConfigManager({
	extraction: {
		batchProcessing: config.extraction.batchProcessing,
	},
	logging: {
		level: config.logging.level,
	},
});

// Set up configuration change listeners
configManager.onChange("extraction", (newConfig, oldConfig) => {
	log("Extraction configuration updated", {
		config_change: {
			section: "extraction",
			old: oldConfig,
			new: newConfig,
		},
	});
});

configManager.onChange("logging", (newConfig, oldConfig) => {
	log("Logging configuration updated", {
		config_change: {
			section: "logging",
			old: oldConfig,
			new: newConfig,
		},
	});
});

// Main function to extract and process monitor data
async function extractAndProcessMonitors() {
	try {
		log("Starting synthetic monitor data extraction...");

		// Only check connection if we haven't established one yet
		const client = getElasticsearchClient();
		const isHealthy = await checkElasticsearchHealth();

		if (!isHealthy) {
			err(
				"❌ Elasticsearch connection failed - skipping this extraction cycle",
			);
			return;
		}

		// Check Kafka connection and list topics
		const { connected: isKafkaConnected } = await checkKafkaConnection();
		if (!isKafkaConnected) {
			err("❌ Kafka connection failed - skipping this extraction cycle");
			return;
		}

		log("Connected to Kafka");

		// Fetch all synthetic monitor data
		const monitorData = await fetchAllMonitorData();

		if (!monitorData || monitorData.length === 0) {
			log("No monitor data found");
			return;
		}

		log(`Retrieved ${monitorData.length} monitor records`);

		// Process and transform the monitor data
		const transformedData = await transformMonitorData(monitorData);

		// Send to Kafka topic
		await sendMonitorDataToKafka(transformedData);

		log("Monitor extraction completed successfully");
	} catch (error) {
		err("Error in extraction process:", error);
	}
}

// Fetch all monitor data from Elasticsearch
async function fetchAllMonitorData() {
	const timeRange = config.extraction.timeRange;
	const size = config.extraction.maxResults;
	const monitorNameWildcard = config.extraction.monitorNamePattern;

	try {
		// Build the query to get all synthetic monitors
		const query: estypes.SearchRequest = {
			index: config.extraction.indexPattern,
			track_total_hits: true,
			timeout: config.extraction.timeout,
			size,
			sort: [{ "@timestamp": "desc" }],
			query: {
				bool: {
					must: [
						{
							wildcard: {
								"monitor.name": monitorNameWildcard,
							},
						},
						// Only get monitors that have a summary
						{ exists: { field: "summary" } },
						{
							range: {
								"@timestamp": {
									gte: timeRange,
								},
							},
						},
					],
					// Make sure we're only getting synthetic monitoring data
					filter: [
						{
							terms: {
								"data_stream.dataset": ["http", "browser", "icmp", "tcp"],
							},
						},
						{
							term: {
								"data_stream.type": "synthetics",
							},
						},
					],
				},
			},
			// Include all the fields we need
			_source: [
				"monitor",
				"url",
				"@timestamp",
				"tags",
				"http",
				"tcp",
				"icmp",
				"synthetics",
				"agent",
				"observer",
				"meta",
				"summary",
				"state",
				"data_stream",
				"ecs",
				"config_id",
			],
		};

		log("Executing Elasticsearch query...");
		log("Query parameters:", {
			timeRange,
			size,
			index: config.extraction.indexPattern,
			monitorNameWildcard,
		});

		// Use the client directly for more complex operations
		const client = getElasticsearchClient();
		const response = await client.search<ElasticsearchHit["_source"]>(query);

		log("Query response", {
			elasticsearch_query: {
				total: response.hits.total,
				took: response.took,
				timed_out: response.timed_out,
				hits: response.hits.hits.length,
			},
		});

		if (response.hits.hits.length === 0) {
			log("No hits found. Checking if index exists...");
			const indices = await client.cat.indices({ format: "json" });
			log(
				"Available indices:",
				indices.map((idx) => idx.index),
			);
		}

		// Pass through raw hits without transformation
		const rawHits = response.hits.hits.map((hit) => ({ _source: hit._source }));

		// Validate the Elasticsearch hits using the old validation method
		const validatedHits = await validateElasticsearchHits(rawHits);
		log(`Validated ${validatedHits.length} Elasticsearch hits`);

		// Count by dataset type
		const datasetCounts: Record<string, number> = {};
		validatedHits.forEach((hit) => {
			const dataset = hit._source.data_stream?.dataset || "unknown";
			datasetCounts[dataset] = (datasetCounts[dataset] || 0) + 1;
		});
		log("Hits by dataset type", {
			dataset_counts: datasetCounts,
		});

		return validatedHits;
	} catch (error: any) {
		err(`Error fetching monitor data`, {
			elasticsearch_error: error,
		});
		if (error.meta?.body) {
			err("Elasticsearch response", {
				elasticsearch_error_body: error.meta.body,
			});
		}
		if (error.meta?.statusCode) {
			err("Status code", {
				elasticsearch_status_code: error.meta.statusCode,
			});
		}
		return [];
	}
}

export function extractBusinessContext(
	source: ElasticsearchHit["_source"],
): BusinessContext {
	const tags = source.tags || [];
	const missingFields: string[] = [];
	let domain = "unknown";
	let department = "unknown";
	let criticality: "high" | "medium" | "low" = "medium";
	let environment = "unknown";

	for (const tag of tags) {
		if (tag.startsWith("domain:")) {
			domain = tag.replace("domain:", "").trim();
		} else if (tag.startsWith("department:")) {
			department = tag.replace("department:", "").trim();
		} else if (tag.startsWith("criticality:")) {
			const crit = tag.replace("criticality:", "").trim().toLowerCase();
			if (crit === "high" || crit === "medium" || crit === "low") {
				criticality = crit;
			}
		} else if (tag.startsWith("environment:")) {
			environment = tag.replace("environment:", "").trim();
		}
	}

	if (domain === "unknown") missingFields.push("domain");
	if (department === "unknown") missingFields.push("department");
	const hasCriticalityTag = tags.some((tag) => tag.startsWith("criticality:"));
	if (!hasCriticalityTag && criticality === "medium")
		missingFields.push("criticality");
	if (environment === "unknown") missingFields.push("environment");

	if (missingFields.length > 0) {
		throw new Error(
			`Missing business context fields: ${missingFields.join(", ")}`,
		);
	}

	return {
		domain,
		department,
		criticality,
		environment,
	};
}

// Transform a single monitor hit to MonitorInfo
async function transformSingleMonitor(
	hit: ElasticsearchHit,
): Promise<MonitorInfo> {
	const businessContext = extractBusinessContext(hit._source);

	// Only include HTTP if it has the required response structure
	const httpData = hit._source.http?.response?.status_code
		? {
				response: {
					status_code: hit._source.http.response.status_code,
					mime_type: hit._source.http.response.mime_type,
					headers: hit._source.http.response.headers,
					body: hit._source.http.response.body
						? {
								bytes: hit._source.http.response.body.bytes || 0,
								content: hit._source.http.response.body.content || "",
								hash: hit._source.http.response.body.hash || "",
							}
						: undefined,
				},
				...(hit._source.http.rtt ? { rtt: hit._source.http.rtt } : {}),
				...(hit._source.http.state
					? {
							state:
								typeof hit._source.http.state === "string"
									? hit._source.http.state
									: JSON.stringify(hit._source.http.state),
						}
					: {}),
			}
		: undefined;

	// Fix the port type issue by ensuring it's a number
	const url = hit._source.url || {
		scheme: undefined,
		domain: undefined,
		port: undefined,
		path: undefined,
		full: undefined,
	};

	// Convert port to number if it's a string
	if (url.port && typeof url.port === "string") {
		url.port = parseInt(url.port, 10);
	}

	// Only include data_stream if all required fields are present
	const dataStream =
		hit._source.data_stream?.namespace &&
		hit._source.data_stream?.type &&
		hit._source.data_stream?.dataset
			? {
					namespace: hit._source.data_stream.namespace,
					type: hit._source.data_stream.type,
					dataset: hit._source.data_stream.dataset,
				}
			: undefined;

	const transformedMonitor: MonitorInfo = {
		id: hit._source.monitor.id,
		name: hit._source.monitor.name,
		type: hit._source.monitor.type,
		url: {
			scheme: url.scheme,
			domain: url.domain,
			port: typeof url.port === "number" ? url.port : undefined,
			path: url.path,
			full: url.full,
		},
		timestamp: hit._source["@timestamp"],
		businessContext,
		tags: hit._source.tags || [],
		monitor: {
			id: hit._source.monitor.id,
			name: hit._source.monitor.name,
			type: hit._source.monitor.type,
			status: hit._source.monitor.status,
			duration: hit._source.monitor.duration,
			ip: hit._source.monitor.ip,
			origin: hit._source.monitor.origin,
			timespan: hit._source.monitor.timespan,
			fleet_managed: hit._source.monitor.fleet_managed,
			check_group: hit._source.monitor.check_group,
			project: hit._source.monitor.project,
		},
		...(httpData ? { http: httpData } : {}),
		...(hit._source.tcp ? { tcp: hit._source.tcp } : {}),
		...(hit._source.icmp ? { icmp: hit._source.icmp } : {}),
		...(hit._source.synthetics
			? { synthetics: { type: hit._source.synthetics.type } }
			: {}),
		...(hit._source.summary
			? {
					summary: {
						retry_group: hit._source.summary.retry_group || "",
						max_attempts: hit._source.summary.max_attempts || 0,
						up: hit._source.summary.up || 0,
						down: hit._source.summary.down || 0,
						attempt: hit._source.summary.attempt || 0,
						final_attempt: hit._source.summary.final_attempt || false,
						status: hit._source.summary.status || "",
					},
				}
			: {}),
		...(hit._source.state
			? {
					state: {
						duration_ms: hit._source.state.duration_ms || "",
						checks: hit._source.state.checks || 0,
						ends: hit._source.state.ends || null,
						started_at: hit._source.state.started_at || "",
						up: hit._source.state.up || 0,
						id: hit._source.state.id || "",
						down: hit._source.state.down || 0,
						flap_history: hit._source.state.flap_history || [],
						status: hit._source.state.status || "",
					},
				}
			: {}),
		...(hit._source.event
			? {
					event: {
						agent_id_status: hit._source.event.agent_id_status,
						ingested: hit._source.event.ingested,
						type: hit._source.event.type,
						dataset: hit._source.event.dataset,
					},
				}
			: {}),
		...(dataStream ? { data_stream: dataStream } : {}),
		...(hit._source.ecs && hit._source.ecs.version
			? { ecs: { version: hit._source.ecs.version } }
			: {}),
		...(hit._source.config_id ? { config_id: hit._source.config_id } : {}),
		...(hit._source.agent &&
		hit._source.agent.name &&
		hit._source.agent.id &&
		hit._source.agent.type &&
		hit._source.agent.version
			? {
					agent: {
						name: hit._source.agent.name,
						id: hit._source.agent.id,
						type: hit._source.agent.type,
						version: hit._source.agent.version,
						ephemeral_id: hit._source.agent.ephemeral_id || "",
					},
				}
			: {}),
		...(hit._source.observer && hit._source.observer.name
			? {
					observer: {
						name: hit._source.observer.name || "",
						...(hit._source.observer.geo && hit._source.observer.geo.name
							? { geo: { name: hit._source.observer.geo.name || "" } }
							: {}),
					},
				}
			: {}),
		...(hit._source.meta ? { meta: hit._source.meta } : {}),
	};

	return transformedMonitor;
}

// Transform monitor data with parallel processing
async function transformMonitorData(
	monitorData: ElasticsearchHit[],
): Promise<MonitorInfo[]> {
	if (!monitorData || monitorData.length === 0) {
		return [];
	}

	const batchConfig = configManager.getSection("extraction").batchProcessing;

	// Use streaming for large datasets, parallel processing for smaller ones
	if (monitorData.length >= batchConfig.streamingThreshold) {
		log(
			`Using streaming processing for ${monitorData.length} items (threshold: ${batchConfig.streamingThreshold})`,
		);

		const streamProcessor = new StreamingProcessor(
			transformSingleMonitor,
			undefined, // onResult - not needed for this use case
			(error, item) => {
				// Error handler for streaming
				const monitorName = item._source.monitor.name;
				err(`Streaming transformation error for monitor ${monitorName}`, {
					transformation_error: {
						monitor: monitorName,
						error: error.message,
					},
				});
				// Store error for database writing
				writeInvalidRecords(
					"monitor_transformation",
					[{ message: error.message }],
					monitorName,
				);
			},
			100, // Progress log every 100 items
		);

		return await streamProcessor.processAll(monitorData);
	} else if (batchConfig.enabled) {
		log(`Using parallel processing for ${monitorData.length} items`);

		const parallelProcessor = new ParallelProcessor<
			ElasticsearchHit,
			MonitorInfo
		>({
			batchSize: batchConfig.batchSize,
			maxConcurrency: batchConfig.maxConcurrency,
			retryAttempts: batchConfig.retryAttempts,
			retryDelay: 1000,
		});

		const { results, errors } = await parallelProcessor.processBatches(
			monitorData,
			transformSingleMonitor,
			(item, error) => {
				// Error handler for parallel processing
				const monitorName = item._source.monitor.name;
				return {
					monitorName,
					error: error.message,
				};
			},
		);

		// Write errors to database grouped by monitor
		if (errors.length > 0) {
			const errorsByMonitor: Record<string, string[]> = {};

			for (const error of errors) {
				if (!errorsByMonitor[error.monitorName]) {
					errorsByMonitor[error.monitorName] = [];
				}
				errorsByMonitor[error.monitorName].push(error.error);
			}

			// Write grouped errors to database
			for (const [monitorName, messages] of Object.entries(errorsByMonitor)) {
				await writeInvalidRecords(
					"monitor_transformation",
					messages.map((message) => ({ message })),
					monitorName,
				);
			}
		}

		return results;
	} else {
		// Fallback to sequential processing if batch processing is disabled
		log(
			`Using sequential processing for ${monitorData.length} items (batch processing disabled)`,
		);
		return await processSequentially(monitorData);
	}
}

// Fallback sequential processing function
async function processSequentially(
	monitorData: ElasticsearchHit[],
): Promise<MonitorInfo[]> {
	const transformedData: MonitorInfo[] = [];
	const errorsByMonitor: Record<string, Set<string>> = {};

	for (const hit of monitorData) {
		try {
			const transformedMonitor = await transformSingleMonitor(hit);
			transformedData.push(transformedMonitor);
		} catch (error) {
			const monitorName = hit._source.monitor.name;
			if (!errorsByMonitor[monitorName]) {
				errorsByMonitor[monitorName] = new Set();
			}
			const errorMessage =
				error instanceof Error ? error.message : String(error);
			errorsByMonitor[monitorName]?.add(errorMessage);
		}
	}

	// Write errors for each monitor separately
	for (const [monitorName, errorSet] of Object.entries(errorsByMonitor)) {
		if (errorSet.size > 0) {
			const errors = Array.from(errorSet).map((message) => ({ message }));
			await writeInvalidRecords("monitor_transformation", errors, monitorName);
		}
	}

	return transformedData;
}

// Start the extraction process on an interval
async function startExtractionProcess() {
	log("🚀 Starting synthetics monitor extractor...");

	try {
		// Initialize database
		initializeDatabase();

		// Start configuration hot-reload watching
		await configManager.startWatching();

		// Start API server (combines metrics and invalid records API)
		log(`Starting API server on port ${config.metrics.port}...`);
		console.log(
			`Attempting to start API server on port ${config.metrics.port}`,
		);
		const apiServer = startApiServer(config.metrics.port);
		log("API server started successfully");
		console.log("API server started successfully");

		// Validate all external service connections
		log("Validating connections to all external services...");
		console.log("Validating connections to all external services...");
		await validateConnectionsOrExit();

		// Initial run
		log("Running initial data extraction...");
		await extractAndProcessMonitors();

		// Set up interval for regular extraction
		const intervalMinutes = config.extraction.intervalMinutes;
		log(`Setting up extraction to run every ${intervalMinutes} minute(s)`);

		// Store the interval ID so we can clear it if needed
		const intervalId = setInterval(
			async () => {
				log(
					`\n🕒 Running scheduled extraction (every ${intervalMinutes} minutes)...`,
				);
				try {
					await extractAndProcessMonitors();
					log(`✅ Scheduled extraction completed successfully`);
				} catch (error) {
					err(`❌ Scheduled extraction failed:`, error);
				}
			},
			intervalMinutes * 60 * 1000,
		);

		log(
			`✅ Extraction interval set up successfully - will run every ${intervalMinutes} minute(s)`,
		);
		console.log(
			`Extraction interval configured for ${intervalMinutes} minute(s)`,
		);

		// Clean up interval on process termination
		process.on("SIGTERM", () => {
			log("Clearing extraction interval...");
			clearInterval(intervalId);
		});
	} catch (error) {
		console.error("ERROR in startExtractionProcess:", error);
		err("Failed to start extraction process:", error);
		throw error;
	}
}

// Ensure clean shutdown
process.on("SIGTERM", async () => {
	log("Received SIGTERM, shutting down gracefully");
	try {
		await closeElasticsearchClient();
		await closeKafkaProducer();
		closeDatabase(); // Close the database connection
		configManager.stopWatching(); // Stop configuration watching
		// Shut down OpenTelemetry if it was initialized
		if (otelSdk) {
			await otelSdk.shutdown();
		}
		log("All connections closed successfully");
	} catch (error) {
		err("Error during shutdown:", error);
	}
	log("Synthetics Monitor Extractor exited.");
	console.log("Synthetics Monitor Extractor exited.");
	process.exit(0);
});

// Also handle SIGINT (Ctrl+C) the same way
process.on("SIGINT", async () => {
	log("Received SIGINT, shutting down gracefully");
	try {
		await closeElasticsearchClient();
		await closeKafkaProducer();
		closeDatabase();
		configManager.stopWatching(); // Stop configuration watching
		if (otelSdk) {
			await otelSdk.shutdown();
		}
		log("All connections closed successfully");
	} catch (error) {
		err("Error during shutdown:", error);
	}
	log("Synthetics Monitor Extractor exited.");
	console.log("Synthetics Monitor Extractor exited.");
	process.exit(0);
});

// Run the extraction process if this file is executed directly
if (import.meta.main) {
	// Print to stdout (not via logger)
	log("Synthetics Monitor Extractor started.");
	console.log("Synthetics Monitor Extractor started.");
	(async () => {
		try {
			// Initialize metrics but don't start a separate server
			initializeMetrics();
			await startExtractionProcess();
		} catch (error) {
			err("Failed to start extraction process:", error);
			process.exit(1);
		}
	})();
}

process.on("exit", () => {
	log("Synthetics Monitor Extractor exited.");
	console.log("Synthetics Monitor Extractor exited.");
});

export default {
	extractAndProcessMonitors,
	fetchAllMonitorData,
	transformMonitorData,
	sendMonitorDataToKafka,
};
