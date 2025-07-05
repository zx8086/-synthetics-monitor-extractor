/* src/kafka.ts */

import {
	Producer,
	Admin,
	stringSerializers,
	ProduceAcks,
} from "@platformatic/kafka";
import { config } from "./config.js";
import { kafkaMessageSizeHistogram } from "./metrics.js";
import type { MonitorInfo } from "./types.js";
import { writeInvalidRecords } from "./types.js";
import { z } from "zod";
import { log, err } from "./utils/logger.js";
import { CircuitBreaker, ExponentialBackoff } from "./utils/circuitBreaker.js";

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

/**
 * Send monitor data to Kafka topic
 */
export async function sendMonitorDataToKafka(
	monitorData: MonitorInfo[],
): Promise<void> {
	if (!monitorData || monitorData.length === 0) {
		return;
	}

	const producer = getKafkaProducer();
	const topicName = config.kafka.topicName;

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
					if (kafkaMessageSizeHistogram) {
						const messageSize = Buffer.byteLength(JSON.stringify(item));
						kafkaMessageSizeHistogram.observe(
							{ topic: topicName },
							messageSize,
						);
					}

					// Create headers as a plain object instead of a Map
					const headers = createMessageHeaders(item);

					// Log the message being sent
					log(`Sending message to Kafka: - ${uniqueKey} (type: ${item.type})`);

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

				log(`Successfully sent ${messages.length} messages to ${topicName}`);
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
			throw error;
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
				// Get metadata for all topics
				const metadata = await admin.metadata({ topics: [] });

				let topics: string[] = [];

				// Extract topics from metadata
				if (metadata.topics instanceof Map) {
					topics = Array.from(metadata.topics.keys());
				} else if (typeof metadata.topics === "object") {
					topics = Object.keys(metadata.topics);
				}

				// Filter for monitoring topics
				const monitoringTopics = topics.filter(
					(topic) =>
						typeof topic === "string" && topic.startsWith("monitoring."),
				);

				return { topics, monitoringTopics };
			} finally {
				await admin.close();
			}
		});

		log("✅ Successfully connected to Kafka");
		log(`📋 Available monitoring topics: ${result.monitoringTopics}`);

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

/**
 * Simulates an HTTP message for validation testing
 */
function simulateHttpMessage(): MonitorInfo {
	return {
		id: "test-monitor-123",
		name: "Test HTTP Monitor",
		type: "http",
		url: {
			scheme: "https",
			domain: "api.example.com",
			port: 443,
			path: "/health",
			full: "https://api.example.com/health",
		},
		timestamp: "2024-03-20T10:00:00.000Z",
		businessContext: {
			domain: "test-domain",
			department: "test-department",
			criticality: "high",
			environment: "production",
		},
		tags: [
			"test",
			"http",
			"domain:test-domain",
			"department:test-department",
			"criticality:high",
			"environment:production",
		],
		// environment property removed as it's not in the MonitorInfo interface
		monitor: {
			id: "test-monitor-123",
			name: "Test HTTP Monitor",
			type: "http",
			status: "up",
			duration: { us: 150 },
			ip: "10.0.0.1",
			origin: "project",
			timespan: {
				gte: "2024-03-20T10:00:00.000Z",
				lt: "2024-03-20T10:01:00.000Z",
			},
			fleet_managed: true,
			check_group: "test-check-group-123",
			project: {
				name: "test-project",
				id: "test-project",
			},
		},
		http: {
			response: {
				status_code: 200,
				mime_type: "application/json",
				headers: {
					"Content-Type": "application/json",
					Server: "nginx/1.18.0",
				},
				body: {
					bytes: 45,
					content: '{"status":"ok","message":"Service is healthy"}',
					hash: "abc123hash",
				},
			},
			rtt: {
				total: { us: 150 },
			},
			state: "up",
		},
		tls: {
			established: true,
			version: "1.3",
			cipher: "TLS_AES_128_GCM_SHA256",
			certificate_not_valid_before: "2024-01-01T00:00:00.000Z",
			certificate_not_valid_after: "2025-01-01T00:00:00.000Z",
			version_protocol: "tls",
			server: {
				x509: {
					not_after: "2025-01-01T00:00:00.000Z",
					not_before: "2024-01-01T00:00:00.000Z",
					subject: {
						distinguished_name: "CN=api.example.com",
						common_name: "api.example.com",
					},
					issuer: {
						distinguished_name: "CN=Let's Encrypt Authority X3",
						common_name: "Let's Encrypt Authority X3",
					},
					public_key_algorithm: "RSA",
					signature_algorithm: "SHA256-RSA",
					public_key_size: 2048,
					public_key_exponent: 65537,
					serial_number: "1234567890",
				},
				hash: {
					sha1: "abc123sha1",
					sha256: "abc123sha256",
				},
			},
		},
		summary: {
			retry_group: "test-retry-group",
			max_attempts: 3,
			up: 1,
			down: 0,
			attempt: 1,
			final_attempt: true,
			status: "up",
		},
		state: {
			duration_ms: "60000",
			checks: 1,
			ends: null,
			started_at: "2024-03-20T10:00:00.000Z",
			up: 1,
			id: "test-state-123",
			down: 0,
			flap_history: [],
			status: "up",
		},
		event: {
			dataset: "http",
			type: "info",
			agent_id_status: "verified",
			ingested: "2024-03-20T10:00:00.000Z",
		},
		data_stream: {
			namespace: "default",
			type: "synthetics",
			dataset: "http",
		},
		ecs: {
			version: "8.0.0",
		},
		config_id: "test-config-123",
		agent: {
			name: "test-agent",
			id: "test-agent-id",
			type: "heartbeat",
			version: "8.0.0",
			ephemeral_id: "test-ephemeral-id",
		},
		observer: {
			name: "test-observer",
			geo: {
				name: "eu-west-1",
			},
		},
		meta: {
			space_id: "default",
		},
		// Remove top-level project, timespan, check_group, fleet_managed, and origin
		// as they're not in the MonitorInfo interface but inside monitor object
		ip: "10.0.0.1",
	};
}

// Example usage in sendMonitorDataToKafka:
// const testMessage = simulateHttpMessage();
// const messages = [testMessage].map((item) => {
//   // ... existing message processing code ...
// });
