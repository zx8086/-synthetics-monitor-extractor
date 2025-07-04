/* src/validation.ts */

import { Client, HttpConnection } from "@elastic/elasticsearch";
import { Admin } from "@platformatic/kafka";
import { config } from "./config.js";
import { log, err } from "./utils/logger.js";

interface ValidationResult {
	valid: boolean;
	errors: string[];
	warnings?: string[];
}

export async function validateConnections(): Promise<ValidationResult> {
	const results = await Promise.allSettled([
		validateElasticsearchConnection(),
		validateKafkaConnection(),
	]);

	const allErrors: string[] = [];
	const allWarnings: string[] = [];

	results.forEach((result, index) => {
		if (result.status === "fulfilled") {
			const { errors, warnings } = result.value;
			allErrors.push(...errors);
			if (warnings) allWarnings.push(...warnings);
		} else {
			const serviceName = index === 0 ? "Elasticsearch" : "Kafka";
			allErrors.push(`${serviceName} validation failed: ${result.reason}`);
		}
	});

	return {
		valid: allErrors.length === 0,
		errors: allErrors,
		warnings: allWarnings.length > 0 ? allWarnings : undefined,
	};
}

async function validateElasticsearchConnection(): Promise<ValidationResult> {
	const _errors: string[] = [];
	const warnings: string[] = [];

	try {
		log("Testing Elasticsearch connection (validation_event: elasticsearch)");

		const client = new Client({
			node: config.elasticsearch.node,
			...(config.elasticsearch.apiKeyId &&
				config.elasticsearch.apiKey && {
					auth: {
						apiKey: {
							id: config.elasticsearch.apiKeyId,
							api_key: config.elasticsearch.apiKey,
						},
					},
				}),
			maxRetries: config.elasticsearch.maxRetries,
			requestTimeout: config.elasticsearch.requestTimeout,
			compression: config.elasticsearch.compression,
			sniffOnStart: config.elasticsearch.sniffOnStart,
			tls: {
				rejectUnauthorized: config.elasticsearch.rejectUnauthorized,
			},
			name: config.elasticsearch.name,
			opaqueIdPrefix: config.elasticsearch.opaqueIdPrefix,
		});

		const response = await client.ping();
		if (!response) {
			_errors.push("Elasticsearch ping failed");
		}

		// Test cluster health
		const health = await client.cluster.health();
		if (health.status === "red") {
			warnings.push("Elasticsearch cluster health is RED");
		} else if (health.status === "yellow") {
			warnings.push("Elasticsearch cluster health is YELLOW");
		}

		// Test index access
		const indices = await client.cat.indices({ format: "json" });
		const syntheticsIndices = indices.filter((idx: any) =>
			idx.index.includes("synthetics"),
		);

		if (syntheticsIndices.length === 0) {
			warnings.push("No synthetics indices found");
		} else {
			log(
				`Found ${syntheticsIndices.length} synthetics indices (validation_event: elasticsearch)`,
			);
		}

		await client.close();

		return {
			valid: true,
			errors: _errors,
			warnings,
		};
	} catch (error: any) {
		err(`Elasticsearch connection error (validation_error: ${error})`);
		return {
			valid: false,
			errors: [
				`Elasticsearch connection failed: ${error.message || "Unknown error"}`,
			],
			warnings,
		};
	}
}

async function validateKafkaConnection(): Promise<ValidationResult> {
	const _errors: string[] = [];
	const warnings: string[] = [];

	try {
		log("Testing Kafka connection (validation_event: kafka)");

		const admin = new Admin({
			clientId: config.kafka.clientId,
			bootstrapBrokers: config.kafka.brokers,

			...(config.kafka.ssl && {
				tls: {
					rejectUnauthorized: true,
				},
			}),

			...(config.kafka.ssl &&
				config.kafka.username &&
				config.kafka.password && {
					sasl: {
						mechanism: "PLAIN" as const,
						username: config.kafka.username,
						password: config.kafka.password,
					},
				}),
		});

		// Test connection by listing topics
		const metadata = await admin.metadata({ topics: [] });
		const topics = Array.from(metadata.topics.keys());
		const monitoringTopics = topics.filter(
			(topic: string) => topic.includes("monitoring") || topic.includes("raw"),
		);

		if (monitoringTopics.length === 0) {
			warnings.push("No monitoring-related topics found");
		}

		// Check if our target topic exists
		const targetTopicExists = topics.includes(config.kafka.topicName);
		if (!targetTopicExists) {
			warnings.push(`Target topic '${config.kafka.topicName}' not found`);
		}

		await admin.close();

		return {
			valid: true,
			errors: _errors,
			warnings,
		};
	} catch (error: any) {
		return {
			valid: false,
			errors: [`Kafka connection failed: ${error.message || "Unknown error"}`],
			warnings,
		};
	}
}
