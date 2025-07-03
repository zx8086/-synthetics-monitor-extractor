/* src/instrumentation.ts */

import { metrics } from "@opentelemetry/api";
import { config } from "./config.js";
import { log, err } from "./utils/logger.js";

const INSTRUMENTATION_ENABLED = config.openTelemetry.enabled && config.nodeEnv !== "development";
log(`OpenTelemetry Instrumentation Enabled (instrumentation_event: ${INSTRUMENTATION_ENABLED})`);

// Export necessary meters and counters
export let httpRequestCounter: any = null;
export let httpResponseTimeHistogram: any = null;

export function initializeOpenTelemetry() {
    if (!INSTRUMENTATION_ENABLED) {
        log("OpenTelemetry instrumentation is disabled (instrumentation_event: false)");
        return null;
    }

    log("Initializing OpenTelemetry... (instrumentation_event: true)");

    try {
        // Create a simple meter for basic metrics
        const meter = metrics.getMeter("synthetics-monitor-extractor");

        // Initialize HTTP metrics
        httpRequestCounter = meter.createCounter("http_requests_total", {
            description: "Count of HTTP requests",
        });

        httpResponseTimeHistogram = meter.createHistogram("http_response_time_seconds", {
            description: "HTTP response time in seconds",
        });

        log("OpenTelemetry metrics initialized successfully (instrumentation_event: true)");
        
        // Return a simple object with shutdown method for compatibility
        return {
            shutdown: async () => {
                log("OpenTelemetry shutdown completed (instrumentation_event: shutdown)");
            }
        };
    } catch (error) {
        err(`Failed to initialize OpenTelemetry (instrumentation_error: ${error})`);
        log("Continuing without OpenTelemetry instrumentation (instrumentation_event: false)");
        return null;
    }
}

// Helper functions to record metrics
export function recordHttpRequest(method: string, route: string) {
    if (INSTRUMENTATION_ENABLED && httpRequestCounter) {
        try {
            httpRequestCounter.add(1, { method, route });
        } catch (error) {
            err(`Failed to record HTTP request metric (instrumentation_error: ${error})`);
        }
    }
}

export function recordHttpResponseTime(duration: number, route: string, statusCode: number) {
    if (INSTRUMENTATION_ENABLED && httpResponseTimeHistogram) {
        try {
            httpResponseTimeHistogram.record(duration / 1000, {
                route,
                statusCode: statusCode.toString()
            });
        } catch (error) {
            err(`Failed to record HTTP response time metric (instrumentation_error: ${error})`);
        }
    }
}