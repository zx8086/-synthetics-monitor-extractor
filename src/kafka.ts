/* src/kafka.ts */

import {
	Admin,
	ProduceAcks,
	Producer,
	stringSerializers,
} from "@platformatic/kafka";
import { trace } from "@opentelemetry/api";
import { z } from "zod";
import { config } from "./config.js";
import { recordKafkaMessage } from "./instrumentation-nodesdk.js";
import { kafkaMessageSizeHistogram } from "./metrics.js";
import type { MonitorInfo } from "./types.js";
import { writeInvalidRecords } from "./types.js";
import { CircuitBreaker, ExponentialBackoff } from "./utils/circuitBreaker.js";
import { err, log } from "./utils/logger.js";

// Define Zod schemas for Kafka messages
const KafkaHeaderSchema = z.record(z.string());
const KafkaMessageSchema = z.object({
	topic: z.string(),
	key: z.string(),
	value: z.string(),
	headers: z.union([z.record(z.string()), z.map(z.string(), z.string())]),
});

type KafkaMessage = z.infer<typeof KafkaMessageSchema>;
type KafkaProducer = Producer<string, string, string, string>;

// Singleton producer instance - let @platformatic/kafka handle the connection lifecycle
let producerInstance: KafkaProducer | null = null;

// Circuit breaker for Kafka operations
const kafkaCircuitBreaker = new CircuitBreaker({
	failureThreshold: 3,
	resetTimeout: 30000, // 30 seconds (shorter than Elasticsearch)
	halfOpenMaxCalls: 2,
	successThreshold: 1,
	name: "Kafka",
});

// Exponential backoff for Kafka retries
const kafkaBackoff = new ExponentialBackoff({
	baseDelay: 500,
	maxDelay: 10000,
	jitterFactor: 0.2,
	maxAttempts: 3,
});

/**
 * Creates a properly configured Kafka producer
 * Note: @platformatic/kafka handles connection management automatically
 */
export function getKafkaProducer(): KafkaProducer {
	if (!producerInstance) {
		log("Creating new Kafka producer");
		log(
			`Kafka configuration: brokers=${JSON.stringify(config.kafka.brokers)}, clientId=${config.kafka.clientId}`,
		);

		producerInstance = new Producer({
			clientId: config.kafka.clientId,
			bootstrapBrokers: config.kafka.brokers,
			serializers: stringSerializers,
			compression: "gzip",

			// @platformatic/kafka already handles:
			// - Connection pooling
			// - Automatic reconnection
			// - Load balancing between brokers
			// - Socket keep-alive

			// We just need to provide the configuration options
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
		}) as KafkaProducer;

		// Clean up on process exit - let the library handle the connection lifecycle
		process.on("SIGTERM", () => {
			closeKafkaProducer().catch((err) =>
				err("Error closing Kafka producer:", err),
			);
		});
	}

	return producerInstance;
}

/**
 * Creates an admin client for Kafka administration tasks
 */
export function getKafkaAdmin(): Admin {
	return new Admin({
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
}

/**
 * Validates and creates a Kafka message
 */
function createKafkaMessage(
	topic: string,
	key: string,
	value: string,
	headers: Map<string, string> | Record<string, string>,
): KafkaMessage {
	const message = {
		topic,
		key,
		value,
		headers,
	};

	return KafkaMessageSchema.parse(message);
}

// Add sanitizeKey function
function sanitizeKey(key: string): string {
	return key
		.toLowerCase()
		.replace(/\s+/g, "") // Remove all spaces
		.replace(/[^a-z0-9:-]/g, ""); // Remove all except lowercase letters, numbers, colon, dash
}

function createMessageKey(monitor: MonitorInfo): string {
	const domain = monitor.businessContext.domain;
	const timestamp = new Date(monitor.timestamp).getTime();
	const rawKey = `raw::${domain}::${monitor.name}::${timestamp}`;
	return sanitizeKey(rawKey);
}

function createMessageHeaders(monitor: MonitorInfo): Map<string, string> {
	const headers = new Map<string, string>();

	// Only add headers if the values exist
	if (monitor.type) headers.set("monitor-type", monitor.type);
	if (monitor.monitor.status) headers.set("status", monitor.monitor.status);
	if (monitor.businessContext.domain)
		headers.set("domain", monitor.businessContext.domain);
	if (monitor.businessContext.department)
		headers.set("department", monitor.businessContext.department);
	if (monitor.businessContext.environment)
		headers.set("environment", monitor.businessContext.environment);
	if (monitor.data_stream?.dataset)
		headers.set("dataset", monitor.data_stream.dataset);
	if (monitor.businessContext.criticality)
		headers.set("criticality", monitor.businessContext.criticality);

	return headers;
}

// Create tracer for Kafka module
const tracer = trace.getTracer("kafka-producer", "1.0.0");

/**
 * Send monitor data to Kafka topic
 */
export async function sendMonitorDataToKafka(
	monitorData: MonitorInfo[],
): Promise<void> {
	return tracer.startActiveSpan("sendMonitorDataToKafka", async (span) => {
		try {
			if (!monitorData || monitorData.length === 0) {
				span.setAttributes({
					"kafka.message_count": 0,
					"kafka.status": "no_data",
				});
				return;
			}

			const producer = getKafkaProducer();
			const topicName = config.kafka.topicName;

			span.setAttributes({
				"kafka.topic": topicName,
				"kafka.message_count": monitorData.length,
			});

			return kafkaCircuitBreaker
				.execute(async () => {
					return kafkaBackoff.execute(async () => {
						// Prepare all messages for the single topic
						const messages: any[] = [];

						for (const item of monitorData) {
							const domain = item.businessContext.domain || "unknown";
							const timestamp = new Date(item.timestamp).getTime();
							const rawKey = `raw::${domain}::${item.name}::${timestamp}`;
							const uniqueKey = sanitizeKey(rawKey);

							// Track message size metrics
							const messageSize = Buffer.byteLength(JSON.stringify(item));

							// Record in both prom-client (for backward compatibility) and OpenTelemetry
							if (kafkaMessageSizeHistogram) {
								kafkaMessageSizeHistogram.observe(
									{ topic: topicName },
									messageSize,
								);
							}

							// Record in OpenTelemetry metrics
							recordKafkaMessage(topicName, messageSize);

							// Create headers as a plain object instead of a Map
							const headers = createMessageHeaders(item);

							// Log the message being sent
							log(
								`Sending message to Kafka: - ${uniqueKey} (type: ${item.type})`,
							);

							const kafkaMessage = createKafkaMessage(
								topicName,
								uniqueKey,
								JSON.stringify(item),
								headers,
							);

							messages.push(kafkaMessage);
						}

						// Send all messages to the single topic
						log(`Sending ${messages.length} messages to topic: ${topicName}`);

						await producer.send({
							messages,
							acks: ProduceAcks.LEADER, // Only wait for leader acknowledgment
						});

						log(
							`Successfully sent ${messages.length} messages to ${topicName}`,
						);

						span.setAttributes({
							"kafka.messages_sent": messages.length,
							"kafka.status": "success",
						});
					});
				})
				.catch((error: any) => {
					// Enhanced error logging with circuit breaker context
					const errorInfo = {
						kafka_error: {
							message: error.message,
							code: error.code,
							name: error.name,
						},
						circuit_breaker_state: kafkaCircuitBreaker.getState(),
						circuit_breaker_metrics: kafkaCircuitBreaker.getMetrics(),
						retry_attempt: kafkaBackoff.getAttempts(),
						topic: topicName,
						message_count: monitorData.length,
					};

					if (error.code === "PLT_KFK_PRODUCER_ERROR") {
						err(`Kafka producer error`, errorInfo);
					} else if (error.code === "PLT_KFK_CONNECTION_ERROR") {
						err(`Kafka connection error`, errorInfo);
					} else {
						err(`Kafka send error`, errorInfo);
					}

					span.recordException(error);
					span.setAttributes({
						"kafka.error": error.message,
						"kafka.error_code": error.code || "unknown",
						"kafka.status": "error",
					});
					throw error;
				});
		} finally {
			span.end();
		}
	});
}

// Helper function to find differences between objects
function findDifferences(obj1: any, obj2: any, path: string = ""): string[] {
	const differences: string[] = [];

	// Skip comparison if either object is undefined
	if (obj1 === undefined || obj2 === undefined) {
		return differences;
	}

	// Check if both are objects
	if (
		typeof obj1 === "object" &&
		typeof obj2 === "object" &&
		obj1 !== null &&
		obj2 !== null
	) {
		// Get all keys from both objects
		const allKeys = new Set([...Object.keys(obj1), ...Object.keys(obj2)]);

		for (const key of allKeys) {
			const newPath = path ? `${path}.${key}` : key;

			// Skip _source field as it's a special case
			if (key === "_source") continue;

			// Check if key exists in both objects
			if (!(key in obj1)) {
				differences.push(
					`${newPath} is missing in original Elasticsearch document`,
				);
				continue;
			}
			if (!(key in obj2)) {
				differences.push(`${newPath} is missing in Kafka message`);
				continue;
			}

			// Recursively check nested objects
			if (
				typeof obj1[key] === "object" &&
				typeof obj2[key] === "object" &&
				obj1[key] !== null &&
				obj2[key] !== null
			) {
				differences.push(...findDifferences(obj1[key], obj2[key], newPath));
			} else if (JSON.stringify(obj1[key]) !== JSON.stringify(obj2[key])) {
				differences.push(
					`${newPath} has different values: ${JSON.stringify(obj1[key])} vs ${JSON.stringify(obj2[key])}`,
				);
			}
		}
	} else if (JSON.stringify(obj1) !== JSON.stringify(obj2)) {
		differences.push(
			`${path} has different values: ${JSON.stringify(obj1)} vs ${JSON.stringify(obj2)}`,
		);
	}

	return differences;
}

// Helper function to validate monitor section
function validateMonitorSection(original: any, transformed: any): string[] {
	const differences: string[] = [];
	const monitorFields = [
		"id",
		"name",
		"type",
		"status",
		"duration",
		"ip",
		"origin",
		"timespan",
		"fleet_managed",
		"check_group",
		"project",
	];

	// Check if monitor section exists in both
	if (!original.monitor && !transformed.monitor) {
		return differences;
	}
	if (!original.monitor) {
		differences.push(
			"monitor section is missing in original Elasticsearch document",
		);
		return differences;
	}
	if (!transformed.monitor) {
		differences.push("monitor section is missing in Kafka message");
		return differences;
	}

	// Check each monitor field
	for (const field of monitorFields) {
		if (
			JSON.stringify(original.monitor[field]) !==
			JSON.stringify(transformed.monitor[field])
		) {
			differences.push(
				`monitor.${field} has different values: ${JSON.stringify(original.monitor[field])} vs ${JSON.stringify(transformed.monitor[field])}`,
			);
		}
	}

	return differences;
}

/**
 * Check Kafka connection and list topics with circuit breaker protection
 */
export async function checkKafkaConnection(): Promise<{
	connected: boolean;
	topics: string[];
	monitoringTopics: string[];
}> {
	try {
		log(`Checking Kafka connection at: ${config.kafka.brokers.join(",")}`);

		const result = await kafkaCircuitBreaker.execute(async () => {
			const admin = getKafkaAdmin();

			try {
				// Use listTopics() method to get all topics
				const topics = await admin.listTopics();

				// Ensure topics is an array
				const topicList = Array.isArray(topics) ? topics : [];

				// Check if our configured topic exists
				const topicExists = topicList.includes(config.kafka.topicName);

				if (topicExists) {
					log(`✅ Topic '${config.kafka.topicName}' exists`);
				} else {
					warn(`⚠️ Topic '${config.kafka.topicName}' does not exist`);
				}

				// Filter for monitoring topics
				const monitoringTopics = topicList.filter(
					(topic) =>
						typeof topic === "string" && topic.startsWith("monitoring."),
				);

				return { topics: topicList, monitoringTopics };
			} finally {
				await admin.close();
			}
		});

		log("✅ Successfully connected to Kafka");
		log(`📋 Found ${result.topics.length} topics`);

		return {
			connected: true,
			...result,
		};
	} catch (error: any) {
		err("❌ Failed to connect to Kafka", {
			kafka_error: {
				message: error?.message || "Unknown error",
				name: error?.name,
				code: error?.code,
			},
			circuit_breaker_state: kafkaCircuitBreaker.getState(),
			circuit_breaker_metrics: kafkaCircuitBreaker.getMetrics(),
			brokers: config.kafka.brokers,
		});

		return {
			connected: false,
			topics: [],
			monitoringTopics: [],
		};
	}
}

/**
 * Gracefully close the Kafka producer connection
 */
export async function closeKafkaProducer(): Promise<void> {
	if (producerInstance) {
		log("Closing Kafka producer");
		await producerInstance.close();
		producerInstance = null;
	}
}
