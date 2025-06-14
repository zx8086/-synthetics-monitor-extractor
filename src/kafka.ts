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

// Define Zod schemas for Kafka messages
const KafkaHeaderSchema = z.record(z.string());
const KafkaMessageSchema = z.object({
  topic: z.string(),
  key: z.string(),
  value: z.string(),
  headers: z.union([
    z.record(z.string()),
    z.map(z.string(), z.string())
  ]),
});

type KafkaMessage = z.infer<typeof KafkaMessageSchema>;
type KafkaProducer = Producer<string, string, string, string>;

// Singleton producer instance - let @platformatic/kafka handle the connection lifecycle
let producerInstance: KafkaProducer | null = null;

/**
 * Creates a properly configured Kafka producer
 * Note: @platformatic/kafka handles connection management automatically
 */
export function getKafkaProducer(): KafkaProducer {
  if (!producerInstance) {
    console.log("Creating new Kafka producer");

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
        console.error("Error closing Kafka producer:", err),
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
  if (monitor.status) headers.set("status", monitor.status);
  if (monitor.businessContext.domain) headers.set("domain", monitor.businessContext.domain);
  if (monitor.businessContext.department) headers.set("department", monitor.businessContext.department);
  if (monitor.businessContext.environment) headers.set("environment", monitor.businessContext.environment);
  if (monitor.dataset) headers.set("dataset", monitor.dataset);
  if (monitor.businessContext.criticality) headers.set("criticality", monitor.businessContext.criticality);

  return headers;
}

/**
 * Send monitor data to a single Kafka topic
 * Domain-specific routing will be handled downstream with Kafka Streams
 */
export async function sendMonitorDataToKafka(
  monitorData: MonitorInfo[],
): Promise<void> {
  if (!monitorData || monitorData.length === 0) {
    return;
  }

  const producer = getKafkaProducer();
  const singleTopicName = config.kafka.topicName;

  try {
    // Send all data to a single topic with domain info in headers
    console.log(
      `Sending ${monitorData.length} messages to topic: ${singleTopicName}`,
    );

    const messages = monitorData.map((item) => {
      const domain = item.businessContext.domain || "unknown";
      const department = item.businessContext.department || "unknown";
      const timestamp = new Date(item.timestamp).getTime();
      const rawKey = `raw::${domain}::${item.name}::${timestamp}`;
      const uniqueKey = sanitizeKey(rawKey);

      // Track message size metrics
      if (kafkaMessageSizeHistogram) {
        const messageSize = Buffer.byteLength(JSON.stringify(item));
        kafkaMessageSizeHistogram.observe(
          { topic: singleTopicName },
          messageSize,
        );
      }

      // Validate message content is exactly identical to original
      const originalContent = JSON.stringify(item);
      const kafkaContent = JSON.stringify(item);
      
      if (originalContent !== kafkaContent) {
        console.error('Message content mismatch detected:', {
          original: originalContent,
          kafka: kafkaContent,
          differences: findDifferences(JSON.parse(originalContent), JSON.parse(kafkaContent))
        });
        throw new Error('Message content validation failed - content is not identical');
      }

      // Log the message being sent
      console.log('Sending message to Kafka:', {
        key: uniqueKey,
        type: item.type
      });

      // Create headers as a plain object instead of a Map
      const headers = createMessageHeaders(item);

      return createKafkaMessage(
        singleTopicName,
        uniqueKey,
        kafkaContent,
        headers,
      );
    });

    await producer.send({
      messages,
      acks: ProduceAcks.LEADER, // Only wait for leader acknowledgment
    });

    console.log(
      `Successfully sent ${messages.length} messages to ${singleTopicName}`,
    );
  } catch (error: any) {
    if (error.code === "PLT_KFK_PRODUCER_ERROR") {
      console.error("Producer error:", error.message);
    } else if (error.code === "PLT_KFK_CONNECTION_ERROR") {
      console.error("Connection error:", error.message);
    } else {
      console.error("Error sending to Kafka:", error);
    }
    throw error;
  }
}

// Helper function to find differences between objects
function findDifferences(obj1: any, obj2: any, path: string = ''): string[] {
  const differences: string[] = [];
  
  // Check if both are objects
  if (typeof obj1 === 'object' && typeof obj2 === 'object' && obj1 !== null && obj2 !== null) {
    // Get all keys from both objects
    const allKeys = new Set([...Object.keys(obj1), ...Object.keys(obj2)]);
    
    for (const key of allKeys) {
      const newPath = path ? `${path}.${key}` : key;
      
      // Check if key exists in both objects
      if (!(key in obj1)) {
        differences.push(`${newPath} is missing in original`);
        continue;
      }
      if (!(key in obj2)) {
        differences.push(`${newPath} is missing in Kafka message`);
        continue;
      }
      
      // Recursively check nested objects
      if (typeof obj1[key] === 'object' && typeof obj2[key] === 'object' && obj1[key] !== null && obj2[key] !== null) {
        differences.push(...findDifferences(obj1[key], obj2[key], newPath));
      } else if (obj1[key] !== obj2[key]) {
        differences.push(`${newPath} has different values: ${JSON.stringify(obj1[key])} vs ${JSON.stringify(obj2[key])}`);
      }
    }
  } else if (obj1 !== obj2) {
    differences.push(`${path} has different values: ${JSON.stringify(obj1)} vs ${JSON.stringify(obj2)}`);
  }
  
  return differences;
}

/**
 * Check Kafka connection and list topics
 */
export async function checkKafkaConnection(): Promise<{
  connected: boolean;
  topics: string[];
  monitoringTopics: string[];
}> {
  try {
    console.log(
      "Checking Kafka connection at:",
      config.kafka.brokers.join(","),
    );

    const admin = getKafkaAdmin();

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
      (topic) => typeof topic === "string" && topic.startsWith("monitoring."),
    );

    console.log("✅ Successfully connected to Kafka");
    console.log("📋 Available monitoring topics:", monitoringTopics);

    await admin.close();

    return {
      connected: true,
      topics,
      monitoringTopics,
    };
  } catch (error: any) {
    console.error("❌ Failed to connect to Kafka");
    console.error("Error details:", error?.message || "Unknown error");

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
    console.log("Closing Kafka producer");
    await producerInstance.close();
    producerInstance = null;
  }
}
