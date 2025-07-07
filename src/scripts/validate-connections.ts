#!/usr/bin/env bun

/* scripts/validate-connections.ts */

import {
	validateConnections,
	formatValidationSummary,
} from "../utils/connectionValidator.js";
import { config } from "../config.js";
import { closeElasticsearchClient } from "../elasticsearch.js";
import { closeKafkaProducer } from "../kafka.js";

async function runValidation() {
	console.log("🔍 Synthetics Monitor Extractor - Connection Validator");
	console.log("=".repeat(60));
	console.log();

	console.log("Configuration:");
	console.log(`  Elasticsearch: ${config.elasticsearch.node}`);
	console.log(`  Kafka Brokers: ${config.kafka.brokers.join(", ")}`);
	console.log(
		`  OpenTelemetry: ${config.openTelemetry.enabled ? "Enabled" : "Disabled"}`,
	);
	console.log();

	try {
		// Run validation
		const summary = await validateConnections();

		// Print formatted summary
		console.log(formatValidationSummary(summary));

		// Exit with appropriate code
		if (!summary.allConnected) {
			console.error("\n❌ Some critical services are not available");
			process.exit(1);
		} else {
			console.log("\n✅ All critical services are connected and ready");
			process.exit(0);
		}
	} catch (error) {
		console.error("\n💥 Unexpected error during validation:", error);
		process.exit(2);
	} finally {
		// Clean up connections
		try {
			await closeElasticsearchClient();
			await closeKafkaProducer();
		} catch (cleanupError) {
			console.error("Error during cleanup:", cleanupError);
		}
	}
}

// Run validation
runValidation();
