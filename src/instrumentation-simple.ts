/* src/instrumentation-simple.ts - Minimal NodeSDK approach with environment variables */

import {
	type Counter,
	context,
	DiagConsoleLogger,
	DiagLogLevel,
	diag,
	type Histogram,
	type Meter,
	metrics,
	trace,
} from "@opentelemetry/api";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { PrometheusExporter } from "@opentelemetry/exporter-prometheus";
import { WinstonInstrumentation } from "@opentelemetry/instrumentation-winston";
import {
	MeterProvider,
	PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { NodeSDK } from "@opentelemetry/sdk-node";
import {
	ATTR_SERVICE_NAME,
	ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import { config } from "./config.js";

const INSTRUMENTATION_ENABLED = config.openTelemetry.enabled;

let sdk: NodeSDK | undefined;
let meter: Meter | undefined;
let httpRequestCounter: Counter | undefined;
let httpResponseTimeHistogram: Histogram | undefined;
let kafkaMessageCounter: Counter | undefined;
let kafkaMessageSizeHistogram: Histogram | undefined;
let isInitialized = false;
let prometheusExporter: any;

// Set OpenTelemetry environment variables for NodeSDK to pick up
function setOTelEnvironmentVariables() {
	if (!INSTRUMENTATION_ENABLED) return;

	// Resource configuration
	process.env.OTEL_RESOURCE_ATTRIBUTES = [
		`${ATTR_SERVICE_NAME}=${config.openTelemetry.serviceName}`,
		`${ATTR_SERVICE_VERSION}=${config.openTelemetry.serviceVersion}`,
		`deployment.environment=${config.openTelemetry.deploymentEnvironment}`,
	].join(",");

	// OTLP exporter configuration
	process.env.OTEL_EXPORTER_OTLP_PROTOCOL = "http/json";
	process.env.OTEL_EXPORTER_OTLP_TIMEOUT = "30000";
	process.env.OTEL_EXPORTER_OTLP_TRACES_TIMEOUT = "30000";
	process.env.OTEL_EXPORTER_OTLP_METRICS_TIMEOUT = "60000";
	process.env.OTEL_EXPORTER_OTLP_LOGS_TIMEOUT = "30000";

	// Endpoint configuration
	process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT =
		config.openTelemetry.tracesEndpoint;
	process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT =
		config.openTelemetry.metricsEndpoint;
	process.env.OTEL_EXPORTER_OTLP_LOGS_ENDPOINT =
		config.openTelemetry.logsEndpoint;

	// Headers
	process.env.OTEL_EXPORTER_OTLP_HEADERS = "content-type=application/json";

	// Metrics configuration
	process.env.OTEL_METRIC_EXPORT_INTERVAL =
		config.openTelemetry.metricReaderInterval.toString();

	// Batch processor configuration
	process.env.OTEL_BSP_EXPORT_TIMEOUT = "30000";
	process.env.OTEL_BLRP_EXPORT_TIMEOUT = "30000";
}

export function initializeHttpMetrics() {
	const { log, err } = require("./utils/logger.js");

	if (INSTRUMENTATION_ENABLED && meter) {
		log("Initializing HTTP and Kafka metrics");
		try {
			httpRequestCounter = meter.createCounter("http_requests_total", {
				description: "Count of HTTP requests",
			});
			log("HTTP request counter created");

			httpResponseTimeHistogram = meter.createHistogram(
				"http_response_time_seconds",
				{
					description: "HTTP response time in seconds",
				},
			);
			log("HTTP response time histogram created");

			// Initialize Kafka metrics
			kafkaMessageCounter = meter.createCounter("kafka_messages_sent_total", {
				description: "Total number of messages sent to Kafka",
			});
			log("Kafka message counter created");

			kafkaMessageSizeHistogram = meter.createHistogram(
				"kafka_message_size_bytes",
				{
					description: "Size of Kafka messages in bytes",
					unit: "bytes",
				},
			);
			log("Kafka message size histogram created");

			log("HTTP and Kafka metrics initialized successfully");
		} catch (error) {
			err("Error initializing metrics:", error);
		}
	} else {
		log(
			"Metrics initialization skipped (instrumentation disabled or meter not available)",
		);
	}
}

async function setupPrometheusMetrics() {
	const { log } = require("./utils/logger.js");

	// Create Prometheus exporter for local metrics
	prometheusExporter = new PrometheusExporter({
		preventServerStart: true,
		endpoint: "/otel-metrics",
	});

	// Get the global meter provider created by NodeSDK
	const globalMeterProvider = metrics.getMeterProvider();

	// If it's our custom provider, add Prometheus reader
	if (
		globalMeterProvider &&
		typeof globalMeterProvider.addMetricReader === "function"
	) {
		(globalMeterProvider as any).addMetricReader(prometheusExporter);
		log("Added Prometheus reader to existing MeterProvider");
	} else {
		// Fallback: create a separate meter provider for Prometheus
		const { OTLPMetricExporter } = await import(
			"@opentelemetry/exporter-metrics-otlp-http"
		);

		const otlpMetricExporter = new OTLPMetricExporter({
			url: config.openTelemetry.metricsEndpoint,
			headers: { "Content-Type": "application/json" },
			timeoutMillis: 60000,
		});

		const periodicReader = new PeriodicExportingMetricReader({
			exporter: otlpMetricExporter,
			exportIntervalMillis: config.openTelemetry.metricReaderInterval,
			exportTimeoutMillis: 60000,
		});

		const { resourceFromAttributes, defaultResource } = await import(
			"@opentelemetry/resources"
		);
		const resource = (await defaultResource()).merge(
			resourceFromAttributes({
				[ATTR_SERVICE_NAME]: config.openTelemetry.serviceName,
				[ATTR_SERVICE_VERSION]: config.openTelemetry.serviceVersion,
				["deployment.environment"]: config.openTelemetry.deploymentEnvironment,
			}),
		);

		const meterProvider = new MeterProvider({
			resource,
			readers: [periodicReader, prometheusExporter],
		});

		metrics.setGlobalMeterProvider(meterProvider);
		log("Created custom MeterProvider with both OTLP and Prometheus readers");
	}

	// Get meter for custom metrics
	meter = metrics.getMeter(
		config.openTelemetry.serviceName,
		config.openTelemetry.serviceVersion,
	);
	log("Metrics Meter created successfully");
}

async function initializeOpenTelemetryInternal() {
	const { log, err } = require("./utils/logger.js");

	if (INSTRUMENTATION_ENABLED) {
		try {
			log("Initializing OpenTelemetry SDK...");
			diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.INFO);

			// Set environment variables for NodeSDK
			setOTelEnvironmentVariables();

			// Create NodeSDK with minimal configuration - let it handle exporters
			sdk = new NodeSDK({
				instrumentations: [
					getNodeAutoInstrumentations({
						"@opentelemetry/instrumentation-aws-lambda": { enabled: false },
						"@opentelemetry/instrumentation-fs": { enabled: false },
						"@opentelemetry/instrumentation-winston": { enabled: false },
						"@opentelemetry/instrumentation-runtime-node": { enabled: false },
					}),
					new WinstonInstrumentation({
						enabled: true,
						disableLogSending: true,
					}),
				],
			});

			sdk.start();
			log("OpenTelemetry SDK started with auto-instrumentation");

			// Setup Prometheus metrics after SDK initialization
			await setupPrometheusMetrics();

			initializeHttpMetrics();

			process.on("SIGTERM", () => {
				const shutdownTimeout = setTimeout(() => {
					err("SDK shutdown timed out, forcing exit");
					process.exit(1);
				}, 5000);

				sdk
					?.shutdown()
					.then(() => {
						clearTimeout(shutdownTimeout);
						log("SDK shut down successfully");
						setTimeout(() => process.exit(0), 1000);
					})
					.catch((error) => {
						clearTimeout(shutdownTimeout);
						err("Error shutting down SDK", error);
						process.exit(1);
					});
			});
		} catch (error) {
			err("Error initializing OpenTelemetry SDK:", error);
		}
	} else {
		log("OpenTelemetry instrumentation is disabled");
	}

	if (INSTRUMENTATION_ENABLED) {
		if (!httpRequestCounter || !httpResponseTimeHistogram) {
			err("HTTP metrics not properly initialized");
		} else {
			log("HTTP metrics initialized successfully");
		}
	}

	isInitialized = true;
}

export function initializeOpenTelemetry() {
	const { log } = require("./utils/logger.js");

	if (!INSTRUMENTATION_ENABLED) {
		log(
			"OpenTelemetry instrumentation is disabled (instrumentation_event: false)",
		);
		return {
			shutdown: async () => {
				log(
					"OpenTelemetry shutdown completed (instrumentation_event: shutdown)",
				);
			},
		};
	}

	initializeOpenTelemetryInternal().catch(console.error);

	return {
		shutdown: async () => {
			if (sdk) {
				await sdk.shutdown();
			}
			log("OpenTelemetry shutdown completed (instrumentation_event: shutdown)");
		},
	};
}

export const otelSDK = sdk;

export function getMeter(): Meter | undefined {
	return meter;
}

export function getPrometheusExporter(): any {
	return prometheusExporter;
}

export async function getOpenTelemetryMetrics(): Promise<string> {
	const { log, err } = require("./utils/logger.js");

	if (!prometheusExporter) {
		log("PrometheusExporter not initialized");
		return "";
	}

	try {
		// Force collection of current metrics
		await prometheusExporter.forceFlush();
		log("PrometheusExporter forceFlush completed");

		// The PrometheusExporter should have a server
		if (
			prometheusExporter._server &&
			prometheusExporter._server.getMetricsRequestHandler
		) {
			log("Using server's getMetricsRequestHandler");
			const handler = prometheusExporter._server.getMetricsRequestHandler();

			return new Promise((resolve) => {
				const chunks: string[] = [];
				const mockResponse = {
					setHeader: () => {},
					write: (chunk: string) => chunks.push(chunk),
					end: (chunk?: string) => {
						if (chunk) chunks.push(chunk);
						const result = chunks.join("");
						log("OpenTelemetry metrics retrieved, length:", result.length);
						resolve(result);
					},
				};

				handler({}, mockResponse);
			});
		} else {
			log("PrometheusExporter server or handler not available");
			log("Available methods:", Object.getOwnPropertyNames(prometheusExporter));

			// Try to access the underlying registry
			if (prometheusExporter._registry) {
				log("Using PrometheusExporter internal registry");
				const metrics = await prometheusExporter._registry.metrics();
				return metrics;
			}

			return "";
		}
	} catch (error) {
		err("Error getting OpenTelemetry metrics:", error);
		return "";
	}
}

export function recordHttpRequest(method: string, route: string) {
	const { debug, warn, err } = require("./utils/logger.js");

	if (INSTRUMENTATION_ENABLED) {
		if (httpRequestCounter) {
			httpRequestCounter.add(1, { method, route });
			debug(`Recorded HTTP request: method=${method}, route=${route}`);
		} else {
			err("HTTP request counter not initialized");
		}
	} else {
		warn(`Skipped recording HTTP request: instrumentation disabled`);
		warn(
			"OPEN_TELEMETRY_ENABLED env var:",
			process.env["OPEN_TELEMETRY_ENABLED"],
		);
		warn("INSTRUMENTATION_ENABLED:", INSTRUMENTATION_ENABLED);
	}
}

export function recordHttpResponseTime(
	duration: number,
	route?: string,
	statusCode?: number,
) {
	if (INSTRUMENTATION_ENABLED && isInitialized && httpResponseTimeHistogram) {
		const activeContext = context.active();
		const span = trace.getSpan(activeContext);
		const traceId = span?.spanContext().traceId;
		const attributes: any = { traceId };
		if (route) attributes.route = route;
		if (statusCode) attributes.statusCode = statusCode.toString();
		httpResponseTimeHistogram.record(duration / 1000, attributes);
	}
}

export function recordKafkaMessage(topic: string, messageSize: number) {
	const { debug, err } = require("./utils/logger.js");

	if (INSTRUMENTATION_ENABLED && isInitialized) {
		if (kafkaMessageCounter) {
			kafkaMessageCounter.add(1, { topic });
			debug(`Recorded Kafka message: topic=${topic}`);
		} else {
			err("Kafka message counter not initialized");
		}

		if (kafkaMessageSizeHistogram) {
			kafkaMessageSizeHistogram.record(messageSize, { topic });
			debug(`Recorded Kafka message size: topic=${topic}, size=${messageSize}`);
		} else {
			err("Kafka message size histogram not initialized");
		}
	}
}
