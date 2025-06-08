/* src/kafka.ts */

import { Producer, Admin, stringSerializers, ProduceAcks } from '@platformatic/kafka';
import { config } from './config.js';
import { kafkaMessageSizeHistogram } from './metrics.js';
import type { MonitorInfo } from './types.js';
import { z } from 'zod';

// Define Zod schemas for Kafka messages
const KafkaHeaderSchema = z.record(z.string());
const KafkaMessageSchema = z.object({
  topic: z.string(),
  key: z.string(),
  value: z.string(),
  headers: z.union([
    z.record(z.string()),
    z.map(z.string(), z.string())
  ])
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
      
      ...(config.kafka.ssl && config.kafka.username && config.kafka.password && {
        sasl: {
          mechanism: "PLAIN" as const,
          username: config.kafka.username,
          password: config.kafka.password,
        },
      }),
    }) as KafkaProducer;
    
    // Clean up on process exit - let the library handle the connection lifecycle
    process.on("SIGTERM", () => {
      closeKafkaProducer().catch(err => 
        console.error("Error closing Kafka producer:", err)
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
    
    ...(config.kafka.ssl && config.kafka.username && config.kafka.password && {
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
  headers: Map<string, string> | Record<string, string>
): KafkaMessage {
  const message = {
    topic,
    key,
    value,
    headers
  };
  
  return KafkaMessageSchema.parse(message);
}

/**
 * Send monitor data to Kafka topics
 * Handles sending to both the raw events topic and domain-specific topics
 */
export async function sendMonitorDataToKafka(
  monitorData: MonitorInfo[]
): Promise<void> {
  if (!monitorData || monitorData.length === 0) {
    return;
  }
  
  const producer = getKafkaProducer();
  
  // Batch the messages by domain
  const messagesByDomain: Record<string, KafkaMessage[]> = {};

  for (const item of monitorData) {
    const domain = item.businessContext.domain || "unknown";
    const department = item.businessContext.department || "unknown";
    const timestamp = new Date(item.timestamp).getTime();
    const uniqueKey = `raw::${domain}::${item.id}::${timestamp}`;
    
    // Track message size metrics
    if (kafkaMessageSizeHistogram) {
      const messageSize = Buffer.byteLength(JSON.stringify(item));
      kafkaMessageSizeHistogram.observe({ topic: "monitoring.raw.events" }, messageSize);
    }

    // Group by domain for domain-specific topics
    if (!messagesByDomain[domain]) {
      messagesByDomain[domain] = [];
    }

    const topicName = `monitoring.${domain.toLowerCase().replace(/[^a-z0-9]/g, "-")}.events`;
    const headers = new Map([
      ["monitor-type", item.type],
      ["status", item.status],
      ["domain", domain],
      ["department", department],
      ["environment", item.environment],
    ]);

    messagesByDomain[domain].push(
      createKafkaMessage(
        topicName,
        uniqueKey,
        JSON.stringify(item),
        headers
      )
    );
  }

  console.log("Messages grouped by domain:", Object.keys(messagesByDomain));

  try {
    // Send to raw events topic first
    console.log(`Sending ${monitorData.length} messages to topic: monitoring.raw.events`);
    await producer.send({
      messages: monitorData.map((item) => {
        const domain = item.businessContext.domain || "unknown";
        const timestamp = new Date(item.timestamp).getTime();
        const uniqueKey = `raw::${domain}::${item.id}::${timestamp}`;
        const headers = new Map([
          ["monitor-type", item.type],
          ["status", item.status],
          ["domain", domain],
          ["department", item.businessContext.department],
          ["environment", item.environment],
        ]);

        return createKafkaMessage(
          "monitoring.raw.events",
          uniqueKey,
          JSON.stringify(item),
          headers
        );
      }),
      acks: ProduceAcks.LEADER // Only wait for leader acknowledgment
    });
    
    console.log(`Successfully sent messages to monitoring.raw.events`);

    // Send to domain-specific topics
    for (const [domain, messages] of Object.entries(messagesByDomain)) {
      const topicName = `monitoring.${domain.toLowerCase().replace(/[^a-z0-9]/g, "-")}.events`;
      console.log(`Sending ${messages.length} messages to topic: ${topicName}`);

      // Track message sizes for domain-specific topics
      if (kafkaMessageSizeHistogram) {
        messages.forEach(msg => {
          const messageSize = Buffer.byteLength(msg.value);
          kafkaMessageSizeHistogram.observe({ topic: topicName }, messageSize);
        });
      }

      await producer.send({
        messages,
        acks: ProduceAcks.LEADER
      });
      
      console.log(`Successfully sent messages to ${topicName}`);
    }

    console.log(`Successfully sent all messages to Kafka`);
  } catch (error: any) {
    if (error.code === 'PLT_KFK_PRODUCER_ERROR') {
      console.error("Producer error:", error.message);
    } else if (error.code === 'PLT_KFK_CONNECTION_ERROR') {
      console.error("Connection error:", error.message);
    } else {
      console.error("Error sending to Kafka:", error);
    }
    throw error;
  }
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
    } else if (typeof metadata.topics === 'object') {
      topics = Object.keys(metadata.topics);
    }
    
    // Filter for monitoring topics
    const monitoringTopics = topics.filter(topic => 
      typeof topic === 'string' && topic.startsWith('monitoring.')
    );
    
    console.log("✅ Successfully connected to Kafka");
    console.log("📋 Available monitoring topics:", monitoringTopics);

    await admin.close();
    
    return {
      connected: true,
      topics,
      monitoringTopics
    };
  } catch (error: any) {
    console.error("❌ Failed to connect to Kafka");
    console.error("Error details:", error?.message || "Unknown error");
    
    return {
      connected: false,
      topics: [],
      monitoringTopics: []
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