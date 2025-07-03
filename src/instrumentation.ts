/* src/instrumentation.ts */

import { trace, context } from "@opentelemetry/api";

import {
  diag,
  DiagConsoleLogger,
  DiagLogLevel,
  metrics,
  type Meter,
  type Counter,
  type Histogram,
} from "@opentelemetry/api";
import { NodeSDK } from "@opentelemetry/sdk-node";
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { MonitoredOTLPTraceExporter } from "./otlp/MonitoredOTLPTraceExporter.js";
import { MonitoredOTLPMetricExporter } from "./otlp/MonitoredOTLPMetricExporter.js";
import { MonitoredOTLPLogExporter } from "./otlp/MonitoredOTLPLogExporter.js";
import type { SpanExporter } from "@opentelemetry/sdk-trace-base";
import type { PushMetricExporter } from "@opentelemetry/sdk-metrics";
import type { LogRecordExporter } from "@opentelemetry/sdk-logs";
import {
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import {
  LoggerProvider,
  BatchLogRecordProcessor,
} from "@opentelemetry/sdk-logs";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { WinstonInstrumentation } from "@opentelemetry/instrumentation-winston";
import * as api from "@opentelemetry/api-logs";
import { config } from "./config.js";

const INSTRUMENTATION_ENABLED = config.openTelemetry.enabled;

let sdk: NodeSDK | undefined;
let meter: Meter | undefined;
let httpRequestCounter: Counter | undefined;
let httpResponseTimeHistogram: Histogram | undefined;
let isInitialized = false;

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

const exporterTimeout = 10000;

const commonConfig = {
  timeoutMillis: exporterTimeout,
  concurrencyLimit: 100,
  keepAlive: true,
};

export function initializeHttpMetrics() {
  const { log, err } = require("./utils/logger.js");
  
  if (INSTRUMENTATION_ENABLED && meter) {
    log("Initializing HTTP metrics");
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

      log("HTTP metrics initialized successfully");
    } catch (error) {
      err("Error initializing HTTP metrics:", error);
    }
  } else {
    log(
      "HTTP metrics initialization skipped (instrumentation disabled or meter not available)",
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

      log("DEBUG: Creating trace exporter with endpoint:", config.openTelemetry.tracesEndpoint);
      log("DEBUG: Trace exporter timeout configuration:", exporterTimeout, "ms");
      const traceExporter = new MonitoredOTLPTraceExporter(
        {
          url: config.openTelemetry.tracesEndpoint,
          headers: { "Content-Type": "application/json" },
          ...commonConfig,
        },
        exporterTimeout,
      ) as unknown as SpanExporter;
      log("DEBUG: Trace exporter created successfully");

      log("Skipping OTLP metric exporter creation - using existing Prometheus metrics");

      log("DEBUG: Creating log exporter with endpoint:", config.openTelemetry.logsEndpoint);
      log("DEBUG: Log exporter timeout configuration:", exporterTimeout, "ms");
      const logExporter = new MonitoredOTLPLogExporter(
        {
          url: config.openTelemetry.logsEndpoint,
          headers: { "Content-Type": "application/json" },
          ...commonConfig,
        },
        exporterTimeout,
      ) as unknown as LogRecordExporter;
      log("DEBUG: Log exporter created successfully");

      log("All OTLP exporters created with 10s timeout configuration");

      log("DEBUG: Registering log provider with exporter");
      const loggerProvider = new LoggerProvider({
        resource: resource,
      });
      loggerProvider.addLogRecordProcessor(new BatchLogRecordProcessor(logExporter, {
        exportTimeoutMillis: exporterTimeout,
        maxExportBatchSize: 512,
        maxQueueSize: 2048,
        scheduledDelayMillis: 5000,
      }));
      log("DEBUG: Log provider registered successfully with BatchLogRecordProcessor timeout:", exporterTimeout, "ms");

      api.logs.setGlobalLoggerProvider(loggerProvider);

      log("Creating PeriodicExportingMetricReader with timeout:", exporterTimeout, "and interval:", config.openTelemetry.metricReaderInterval);
      
      log("OTLP metrics export disabled to avoid conflict with existing Prometheus metrics system");
      log("Traces and logs will continue to be exported to OTLP endpoints");
      const otlpMetricReader = null;
      

      log("Creating MeterProvider without OTLP metrics reader (using existing Prometheus metrics)");
      const meterProvider = new MeterProvider({
        resource: resource,
        readers: [], // No OTLP metric readers - using existing Prometheus metrics
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
        maxExportBatchSize: 512,
        maxQueueSize: 2048,
        scheduledDelayMillis: 5000,
      });
      log("DEBUG: Trace provider registered successfully with BatchSpanProcessor timeout:", exporterTimeout, "ms");

      sdk = new NodeSDK({
        resource: resource,
        traceExporter,
        spanProcessors: [batchSpanProcessor],
        logRecordProcessors: [new BatchLogRecordProcessor(logExporter)],
        instrumentations: [
          getNodeAutoInstrumentations({
            "@opentelemetry/instrumentation-aws-lambda": { enabled: false },
            "@opentelemetry/instrumentation-fs": { enabled: false },
            "@opentelemetry/instrumentation-winston": { enabled: false },
            "@opentelemetry/instrumentation-runtime-node": { enabled: false }
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
    log("OpenTelemetry instrumentation is disabled (instrumentation_event: false)");
    return {
      shutdown: async () => {
        log("OpenTelemetry shutdown completed (instrumentation_event: shutdown)");
      }
    };
  }

  initializeOpenTelemetryInternal().catch(console.error);
  
  return {
    shutdown: async () => {
      if (sdk) {
        await sdk.shutdown();
      }
      log("OpenTelemetry shutdown completed (instrumentation_event: shutdown)");
    }
  };
}

export const otelSDK = sdk;

export function getMeter(): Meter | undefined {
  return meter;
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
    warn("OPEN_TELEMETRY_ENABLED env var:", process.env["OPEN_TELEMETRY_ENABLED"]);
    warn("INSTRUMENTATION_ENABLED:", INSTRUMENTATION_ENABLED);
  }
}

export function recordHttpResponseTime(duration: number, route?: string, statusCode?: number) {
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
