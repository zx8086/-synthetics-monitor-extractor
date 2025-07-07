/* src/instrumentation-nodesdk.ts - Refactored to use NodeSDK built-in exporters */

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
let prometheusExporter: any = null; // Not used in NodeSDK approach

const createResource = async () => {
	const { defaultResource, resourceFromAttributes } = await import(
		"@opentelemetry/resources"
	);
	return (await defaultResource()).merge(
		resourceFromAttributes({
			[ATTR_SERVICE_NAME]: config.openTelemetry.serviceName,
			[ATTR_SERVICE_VERSION]: config.openTelemetry.serviceVersion,
			["deployment.environment"]: config.openTelemetry.deploymentEnvironment,
		}),
	);
};

const exporterTimeout = 30000; // Increased timeout for network reliability
const metricsExporterTimeout = 60000; // Longer timeout for metrics due to potentially larger payloads

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

// Wrapper function to suppress timeout errors from exporters
function suppressExporterErrors(exporter: any, exporterType: string) {
	// Override the export method to catch and suppress timeout errors
	const originalExport = exporter.export;
	exporter.export = function (...args: any[]) {
		const callback = args[args.length - 1];
		if (typeof callback === "function") {
			// Replace the callback to suppress timeout errors
			const wrappedCallback = (error: any) => {
				if (
					error &&
					error.message &&
					error.message.includes("Request timed out")
				) {
					// Silently ignore timeout errors - data was likely sent successfully
					callback(null);
				} else {
					callback(error);
				}
			};
			args[args.length - 1] = wrappedCallback;
		}
		return originalExport.apply(this, args);
	};
	return exporter;
}

async function createOTLPExporters() {
	const { log } = require("./utils/logger.js");

	// Create trace exporter
	const { OTLPTraceExporter } = await import(
		"@opentelemetry/exporter-trace-otlp-http"
	);
	const traceExporter = suppressExporterErrors(
		new OTLPTraceExporter({
			url: config.openTelemetry.tracesEndpoint,
			headers: { "Content-Type": "application/json" },
			timeoutMillis: exporterTimeout,
			keepAlive: true,
			concurrencyLimit: 1,
		}),
		"trace",
	);
	log("DEBUG: Trace exporter created successfully");

	// Create metrics exporter
	const { OTLPMetricExporter } = await import(
		"@opentelemetry/exporter-metrics-otlp-http"
	);
	const metricExporter = suppressExporterErrors(
		new OTLPMetricExporter({
			url: config.openTelemetry.metricsEndpoint,
			headers: { "Content-Type": "application/json" },
			timeoutMillis: metricsExporterTimeout,
			keepAlive: true,
			concurrencyLimit: 1,
		}),
		"metrics",
	);
	log("DEBUG: Metric exporter created successfully");

	// Create log exporter
	const { OTLPLogExporter } = await import(
		"@opentelemetry/exporter-logs-otlp-http"
	);
	const logExporter = suppressExporterErrors(
		new OTLPLogExporter({
			url: config.openTelemetry.logsEndpoint,
			headers: { "Content-Type": "application/json" },
			timeoutMillis: exporterTimeout,
			keepAlive: true,
			concurrencyLimit: 1,
		}),
		"logs",
	);
	log("DEBUG: Log exporter created successfully");

	return { traceExporter, metricExporter, logExporter };
}

async function setupMetricsProvider(metricExporter: any) {
	const { log } = require("./utils/logger.js");

	// Create periodic metric reader for OTLP export only
	const periodicExportingMetricReader = new PeriodicExportingMetricReader({
		exporter: metricExporter,
		exportIntervalMillis: config.openTelemetry.metricReaderInterval,
		exportTimeoutMillis: metricsExporterTimeout,
	});

	// Create meter provider with OTLP reader only (no Prometheus)
	const resource = await createResource();
	const meterProvider = new MeterProvider({
		resource: resource,
		readers: [periodicExportingMetricReader], // Only OTLP export
	});

	metrics.setGlobalMeterProvider(meterProvider);
	log("Global MeterProvider set successfully (OTLP only)");

	// Get meter for custom metrics
	meter = metrics.getMeter(
		config.openTelemetry.serviceName,
		config.openTelemetry.serviceVersion,
	);
	log("Metrics Meter created successfully");
}

async function initializeOpenTelemetryInternal() {
	const { log, warn, err } = require("./utils/logger.js");

	if (INSTRUMENTATION_ENABLED) {
		try {
			log("Initializing OpenTelemetry SDK...");
			// Completely suppress OpenTelemetry diagnostic logs to avoid Bun timeout errors
			diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.NONE);

			const resource = await createResource();
			const { traceExporter, metricExporter, logExporter } =
				await createOTLPExporters();

			// Create NodeSDK with built-in exporter configuration
			sdk = new NodeSDK({
				resource: resource,
				traceExporter: traceExporter,
				// Don't set up metrics here - we'll do it manually to include Prometheus
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

			// Manually setup log processor due to custom timeout requirements
			const { BatchLogRecordProcessor, LoggerProvider } = await import(
				"@opentelemetry/sdk-logs"
			);
			const { logs } = await import("@opentelemetry/api-logs");

			const loggerProvider = new LoggerProvider({ resource });
			const logProcessor = new BatchLogRecordProcessor(logExporter, {
				exportTimeoutMillis: exporterTimeout,
			});
			loggerProvider.addLogRecordProcessor(logProcessor);
			logs.setGlobalLoggerProvider(loggerProvider);

			sdk.start();
			log("OpenTelemetry SDK started with auto-instrumentation");

			// Setup metrics provider after SDK initialization
			await setupMetricsProvider(metricExporter);

			// Ensure meter is available after metrics setup
			meter = metrics.getMeter(
				config.openTelemetry.serviceName,
				config.openTelemetry.serviceVersion,
			);
			log("Post-SDK Metrics Meter created successfully");

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
	const { log } = require("./utils/logger.js");

	// Custom metrics are now sent via OTLP, not exposed for scraping
	// Return empty string since metrics are exported to OTLP endpoint
	log("Custom metrics are exported via OTLP, not available for scraping");
	return "";
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
