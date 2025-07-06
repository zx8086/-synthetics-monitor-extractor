/* src/instrumentation.ts */

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
import * as api from "@opentelemetry/api-logs";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { PrometheusExporter } from "@opentelemetry/exporter-prometheus";
import { WinstonInstrumentation } from "@opentelemetry/instrumentation-winston";
import type { LogRecordExporter } from "@opentelemetry/sdk-logs";
import {
	BatchLogRecordProcessor,
	LoggerProvider,
} from "@opentelemetry/sdk-logs";
import type { PushMetricExporter } from "@opentelemetry/sdk-metrics";
import {
	MeterProvider,
	PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { NodeSDK } from "@opentelemetry/sdk-node";
import type { SpanExporter } from "@opentelemetry/sdk-trace-base";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
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

const commonConfig = {
	timeoutMillis: exporterTimeout,
	concurrencyLimit: 1, // Single request to avoid overwhelming endpoint
	keepAlive: true, // Keep connections alive to prevent timeout issues
};

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

async function initializeOpenTelemetryInternal() {
	const { log, warn, err } = require("./utils/logger.js");

	if (INSTRUMENTATION_ENABLED) {
		try {
			log("Initializing OpenTelemetry SDK...");
			diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.INFO);

			const resource = await createResource();

			log(
				"DEBUG: Creating trace exporter with endpoint:",
				config.openTelemetry.tracesEndpoint,
			);
			const { OTLPTraceExporter } = await import(
				"@opentelemetry/exporter-trace-otlp-http"
			);
			const traceExporter = new OTLPTraceExporter({
				url: config.openTelemetry.tracesEndpoint,
				headers: { "Content-Type": "application/json" },
				...commonConfig,
			}) as unknown as SpanExporter;
			log("DEBUG: Trace exporter created successfully");

			log(
				"Skipping OTLP metric exporter creation - using existing Prometheus metrics",
			);

			log(
				"DEBUG: Creating log exporter with endpoint:",
				config.openTelemetry.logsEndpoint,
			);
			const { OTLPLogExporter } = await import(
				"@opentelemetry/exporter-logs-otlp-http"
			);
			const logExporter = new OTLPLogExporter({
				url: config.openTelemetry.logsEndpoint,
				timeoutMillis: exporterTimeout,
				headers: { "Content-Type": "application/json" },
			}) as unknown as LogRecordExporter;
			log("DEBUG: Log exporter created successfully");

			log("All OTLP exporters created with 10s timeout configuration");

			log("DEBUG: Registering log provider with exporter");
			const loggerProvider = new LoggerProvider({
				resource: resource,
			});
			const logProcessor = new BatchLogRecordProcessor(logExporter, {
				exportTimeoutMillis: exporterTimeout,
				// Use default batch sizes for optimal performance
			});
			loggerProvider.addLogRecordProcessor(logProcessor);
			log(
				"DEBUG: Log provider registered successfully with BatchLogRecordProcessor timeout:",
				exporterTimeout,
				"ms",
			);

			api.logs.setGlobalLoggerProvider(loggerProvider);

			log(
				"Creating PeriodicExportingMetricReader with timeout:",
				exporterTimeout,
				"and interval:",
				config.openTelemetry.metricReaderInterval,
			);

			log(
				"DEBUG: Creating OTLP metric exporter with endpoint:",
				config.openTelemetry.metricsEndpoint,
			);
			const { OTLPMetricExporter } = await import(
				"@opentelemetry/exporter-metrics-otlp-http"
			);
			const metricExporter = new OTLPMetricExporter({
				url: config.openTelemetry.metricsEndpoint,
				timeoutMillis: metricsExporterTimeout,
				headers: { "Content-Type": "application/json" },
				...commonConfig,
			}) as unknown as PushMetricExporter;
			log("DEBUG: OTLP metric exporter created successfully");

			const periodicExportingMetricReader = new PeriodicExportingMetricReader({
				exporter: metricExporter,
				exportIntervalMillis: config.openTelemetry.metricReaderInterval,
				exportTimeoutMillis: metricsExporterTimeout,
			});

			log(
				"Creating Prometheus metric reader for OpenTelemetry metrics integration",
			);
			prometheusExporter = new PrometheusExporter({
				preventServerStart: true, // Don't start a separate HTTP server
				endpoint: "/otel-metrics", // Different endpoint to avoid conflicts
			});

			log(
				"Creating MeterProvider with both OTLP and Prometheus metric readers",
			);
			const meterProvider = new MeterProvider({
				resource: resource,
				readers: [periodicExportingMetricReader, prometheusExporter], // Both OTLP push and Prometheus scraping
			});
			log("MeterProvider created successfully");

			metrics.setGlobalMeterProvider(meterProvider);
			log("Global MeterProvider set successfully");

			try {
				meter = metrics.getMeter(
					config.openTelemetry.serviceName,
					config.openTelemetry.serviceVersion,
				);
				if (!meter) {
					warn("Failed to get meter from global MeterProvider");
				} else {
					log("Metrics Meter created successfully");
				}
			} catch (error) {
				err("Error getting meter:", error);
			}

			log("DEBUG: Registering trace provider with exporter");
			const batchSpanProcessor = new BatchSpanProcessor(traceExporter, {
				exportTimeoutMillis: exporterTimeout,
				// Use default batch sizes for optimal performance
			});
			log(
				"DEBUG: Trace provider registered successfully with BatchSpanProcessor timeout:",
				exporterTimeout,
				"ms",
			);

			sdk = new NodeSDK({
				resource: resource,
				traceExporter,
				spanProcessors: [batchSpanProcessor],
				logRecordProcessors: [
					new BatchLogRecordProcessor(logExporter, {
						exportTimeoutMillis: exporterTimeout,
					}),
				],
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
