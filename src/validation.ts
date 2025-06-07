import { Client } from "@elastic/elasticsearch";
import { Kafka } from "kafkajs";
import { config } from "./config.js";

interface ValidationResult {
  valid: boolean;
  errors: string[];
  warnings?: string[];
}

export async function checkElasticsearchConnection(): Promise<ValidationResult> {
  const errors: string[] = [];
  const warnings: string[] = [];

  try {
    const client = new Client({
      node: config.elasticsearch.node,
      ...(config.elasticsearch.apiKeyId && config.elasticsearch.apiKey && {
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
    });

    console.log("Testing Elasticsearch connection...");
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
  } catch (error) {
    let errorMessage = "Unknown error occurred";
    
    if (error instanceof Error) {
      errorMessage = error.message;
      
      if (errorMessage.includes("ECONNREFUSED")) {
        errors.push("Connection refused - check if Elasticsearch is running and accessible");
      } else if (errorMessage.includes("ENOTFOUND") || errorMessage.includes("getaddrinfo")) {
        errors.push("Host not found - check the ELASTIC_NODE configuration");
      } else if (errorMessage.includes("401") || errorMessage.includes("Unauthorized")) {
        errors.push("Authentication failed - check your API key credentials");
      } else if (errorMessage.includes("403") || errorMessage.includes("Forbidden")) {
        errors.push("Access denied - check your permissions");
      } else if (errorMessage.includes("certificate") || errorMessage.includes("SSL") || errorMessage.includes("TLS")) {
        errors.push("TLS/SSL certificate issue detected");
      } else if (errorMessage.includes("timeout")) {
        errors.push("Connection timeout - the server may be slow or unreachable");
      } else {
        errors.push(`Failed to connect to Elasticsearch: ${errorMessage}`);
      }
    } else {
      errors.push(`Failed to connect to Elasticsearch: ${String(error)}`);
    }

    return {
      valid: false,
      errors,
      warnings,
    };
  }
}

export async function checkKafkaConnection(): Promise<ValidationResult> {
  const errors: string[] = [];
  const warnings: string[] = [];

  try {
    const kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      ssl: config.kafka.ssl,
      ...(config.kafka.ssl && config.kafka.username && config.kafka.password && {
        sasl: {
          mechanism: "plain",
          username: config.kafka.username,
          password: config.kafka.password,
        },
      }),
      connectionTimeout: config.kafka.connectionTimeout,
      authenticationTimeout: config.kafka.authenticationTimeout,
    });

    console.log("Testing Kafka connection...");
    const admin = kafka.admin();
    
    await admin.connect();
    const topics = await admin.listTopics();
    
    const monitoringTopics = topics.filter((topic) =>
      topic.startsWith("monitoring.")
    );
    
    warnings.push(`Connected to Kafka cluster with ${topics.length} topics`);
    warnings.push(`Found ${monitoringTopics.length} monitoring topics`);
    
    await admin.disconnect();

    return {
      valid: true,
      errors: [],
      warnings,
    };
  } catch (error) {
    let errorMessage = "Unknown error occurred";
    
    if (error instanceof Error) {
      errorMessage = error.message;
      
      if (errorMessage.includes("ECONNREFUSED")) {
        errors.push("Connection refused - check if Kafka is running and accessible");
      } else if (errorMessage.includes("ENOTFOUND") || errorMessage.includes("getaddrinfo")) {
        errors.push("Host not found - check the KAFKA_BROKERS configuration");
      } else if (errorMessage.includes("authentication") || errorMessage.includes("SASL")) {
        errors.push("Authentication failed - check your Kafka credentials");
      } else if (errorMessage.includes("timeout")) {
        errors.push("Connection timeout - check network connectivity to Kafka");
      } else {
        errors.push(`Failed to connect to Kafka: ${errorMessage}`);
      }
    } else {
      errors.push(`Failed to connect to Kafka: ${String(error)}`);
    }

    return {
      valid: false,
      errors,
      warnings,
    };
  }
}

export async function validateConnections(): Promise<ValidationResult> {
  const allErrors: string[] = [];
  const allWarnings: string[] = [];

  console.log("🔍 Validating service connections...");

  const elasticResult = await checkElasticsearchConnection();
  if (!elasticResult.valid) {
    allErrors.push(...elasticResult.errors);
  }
  if (elasticResult.warnings) {
    allWarnings.push(...elasticResult.warnings);
  }

  const kafkaResult = await checkKafkaConnection();
  if (!kafkaResult.valid) {
    allErrors.push(...kafkaResult.errors);
  }
  if (kafkaResult.warnings) {
    allWarnings.push(...kafkaResult.warnings);
  }

  return {
    valid: allErrors.length === 0,
    errors: allErrors,
    warnings: allWarnings,
  };
}
