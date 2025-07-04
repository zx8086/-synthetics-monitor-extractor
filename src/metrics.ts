/* src/metrics.ts */

import * as promClient from "prom-client";
import { config } from "./config.js";
import { log } from "./utils/logger.js";

export let registry: promClient.Registry;
export let kafkaMessageSizeHistogram: promClient.Histogram;

export function initializeMetrics() {
	registry = new promClient.Registry();

	promClient.collectDefaultMetrics({
		register: registry,
		prefix: config.metrics.prefix,
	});

	kafkaMessageSizeHistogram = new promClient.Histogram({
		name: "kafka_message_size_bytes",
		help: "Size of Kafka messages in bytes",
		labelNames: ["topic"],
		buckets: [100, 1000, 10000, 100000, 1000000],
		registers: [registry],
	});
}

// Note: This is kept for backward compatibility but the actual server is started in api.ts
export function startMetricsServer() {
	if (!config.metrics.enabled) {
		log("Metrics disabled, skipping metrics server startup");
		return;
	}

	log(
		`Metrics will be available at ${config.metrics.endpoint} on the API server`,
	);
	// The actual server is started in api.ts to avoid port conflicts
	return null;
}
