
/* src/validation.ts */

import { Client } from "@elastic/elasticsearch";
import { Admin } from "@platformatic/kafka";
import { config } from "./config.js";
import { HttpConnection } from "@elastic/elasticsearch";

interface ValidationResult {
  valid: boolean;
  errors: string[];
  warnings?: string[];
}

export async function validateConnections(): Promise<ValidationResult> {
  const errors: string[] = [];
  const warnings: string[] = [];

  // Validate Elasticsearch connection
  const elasticResult = await validateElasticsearchConnection();
  if (!elasticResult.valid) {
    errors.push(...elasticResult.errors);
  }
  if (elasticResult.warnings) {
    warnings.push(...elasticResult.warnings);
  }

  // Validate Kafka connection
  const kafkaResult = await validateKafkaConnection();
  if (!kafkaResult.valid) {
    errors.push(...kafkaResult.errors);
  }
  if (kafkaResult.warnings) {
    warnings.push(...kafkaResult.warnings);
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings: warnings.length > 0 ? warnings : undefined,
  };
}

async function validateElasticsearchConnection(): Promise<ValidationResult> {
  const errors: string[] = [];
  const warnings: string[] = [];

  try {
    const client = new Client({
      node: config.elasticsearch.node,
      auth: {
        apiKey: {
          id: config.elasticsearch.apiKeyId || "",
          api_key: config.elasticsearch.apiKey || "",
        },
      },
      Connection: HttpConnection,
      compression: true,
      maxRetries: 5,
      requestTimeout: 30000,
      sniffOnStart: false,
      name: "synthetics-extractor",
      opaqueIdPrefix: "synthetics-extractor::",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate",
      },
      context: {
        userAgent: "synthetics-extractor/1.0.0 (bun)",
      },
      redaction: {
        type: "replace",
        additionalKeys: ["authorization", "x-elastic-client-meta"],
      },
      tls: {
        rejectUnauthorized: config.nodeEnv === "production",
      },
    });

    console.log("Testing Elasticsearch connection...");
    
    // First try a simple ping
    const pingResponse = await client.ping();
    if (!pingResponse) {
      return {
        valid: false,
        errors: ["Elasticsearch ping failed"],
      };
    }

    // If ping succeeds, get cluster info
    const info = await client.info();
    if (!info || !info.version) {
      return {
        valid: false,
        errors: ["Invalid response from Elasticsearch"],
      };
    }

    const serverVersion = info.version?.number || "0.0.0";
    const majorVersion = parseInt(serverVersion.split('.')[0] || "0");
    
    if (majorVersion >= 9) {
      warnings.push(`Server version ${serverVersion} detected - using modern Elasticsearch features`);
    } else if (majorVersion >= 8) {
      warnings.push(`Server version ${serverVersion} - full feature support`);
    } else {
      warnings.push(`Server version ${serverVersion} - some features may be limited`);
    }

    warnings.push(`Connected to Elasticsearch cluster: ${info.cluster_name}`);

    return {
      valid: true,
      errors: [],
      warnings,
    };
  } catch (error: any) {
    console.error("Elasticsearch connection error:", error);
    return {
      valid: false,
      errors: [`Failed to connect to Elasticsearch: ${error.message}`],
    };
  }
}

async function validateKafkaConnection(): Promise<ValidationResult> {
  const errors: string[] = [];
  const warnings: string[] = [];

  try {
    const admin = new Admin({
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

    const metadata = await admin.metadata({ topics: [] });
    const topics = Array.from(metadata.topics.keys());
    const monitoringTopics = topics.filter((topic: string) => topic.startsWith('monitoring.'));

    if (monitoringTopics.length === 0) {
      warnings.push("No monitoring topics found in Kafka cluster");
    } else {
      warnings.push(`Found ${monitoringTopics.length} monitoring topics`);
    }

    await admin.close();

    return {
      valid: true,
      errors: [],
      warnings,
    };
  } catch (error: any) {
    return {
      valid: false,
      errors: [`Failed to connect to Kafka: ${error.message}`],
    };
  }
}
