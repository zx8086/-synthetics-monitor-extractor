/* src/index.ts */

import { Client } from "@elastic/elasticsearch";
import { Kafka } from "kafkajs";
import fs from "fs";
import path from "path";
import strict from "assert/strict";

// Create Elasticsearch client
const elasticClient = new Client({
  node: Bun.env.ELASTIC_NODE || "https://elasticsearch:9200",
  auth: {
    username: Bun.env.ELASTIC_USERNAME,
    password: Bun.env.ELASTIC_PASSWORD,
    // Alternatively, use API key
    // apiKey: process.env.ELASTIC_API_KEY,
  },
  tls: {
    rejectUnauthorized: Bun.NODE_ENV === "production", // False for dev environments
  },
});

// Create Kafka client
const kafka = new Kafka({
  clientId: "synthetics-extractor",
  brokers: (Bun.env.KAFKA_BROKERS || "192.168.178.10:9092").split(","),
  ssl: Bun.env.KAFKA_SSL === "true",
  ...(Bun.env.KAFKA_SSL === "true" && {
    sasl: {
      mechanism: "plain",
      username: Bun.env.KAFKA_USERNAME,
      password: Bun.env.KAFKA_PASSWORD,
    },
  }),
});

const producer = kafka.producer();

// Monitor configuration - could be loaded from a database or config file
const monitorConfig = loadMonitorConfig();

// Main function to extract and process monitor data
async function extractAndProcessMonitors() {
  try {
    console.log("Starting synthetic monitor data extraction...");

    // Connect to Kafka
    await producer.connect();
    console.log("Connected to Kafka");

    // Fetch all synthetic monitor data
    const monitorData = await fetchAllMonitorData();

    if (!monitorData || monitorData.length === 0) {
      console.log("No monitor data found");
      return;
    }

    console.log(`Retrieved ${monitorData.length} monitor records`);

    // Process and transform the monitor data
    const transformedData = transformMonitorData(monitorData);

    // Send to Kafka topics
    await sendToKafka(transformedData);

    console.log("Monitor extraction completed successfully");
  } catch (error) {
    console.error("Error in extraction process:", error);
  }
}

// Fetch all monitor data from Elasticsearch
async function fetchAllMonitorData() {
  const timeRange = "now-5m";
  const size = 1000; // Adjust based on your monitor count

  try {
    // Build the query to get all synthetic monitors
    const query = {
      index: "synthetics-*",
      body: {
        query: {
          bool: {
            must: [
              // Time range filter
              {
                range: {
                  "@timestamp": {
                    gte: timeRange,
                  },
                },
              },
            ],
          },
        },
        sort: [{ "@timestamp": { order: "desc" } }],
        size: size,
      },
    };

    // Execute the search
    const response = await elasticClient.search(query);

    return response.hits.hits;
  } catch (error) {
    console.error(`Error fetching monitor data:`, error);
    return [];
  }
}

// Transform monitor data into our standardized format
function transformMonitorData(monitorData) {
  return monitorData.map((hit) => {
    const source = hit._source;

    // Extract business context directly from tags
    const businessContext = extractBusinessContext(source);

    // Extract base monitor info
    const monitorInfo = {
      id: source.monitor.id,
      name: source.monitor.name,
      type: source.monitor.type,
      url: source.url?.full,
      timestamp: source["@timestamp"],
      status: source.monitor.status,
      duration: source.monitor.duration?.us / 1000, // Convert to ms
      businessContext: businessContext,
      tags: source.tags || [],
      environment: businessContext.environment,
    };

    // Add HTTP-specific details if available
    if (source.http) {
      monitorInfo.http = {
        statusCode: source.http.response?.status_code,
        responseTime: source.http.rtt?.total?.us / 1000, // Convert to ms
      };
    }

    // Add TLS-specific details if available
    if (source.tls) {
      monitorInfo.tls = {
        established: source.tls.established,
        version: source.tls.version,
      };
    }

    // Add service-specific metadata
    monitorInfo.service = extractServiceInfo(source);

    return monitorInfo;
  });
}

// Extract environment information from monitor data
function extractEnvironment(source) {
  // Try to extract from tags first
  if (source.tags) {
    const envTags = ["production", "development", "staging", "testing", "qa"];
    for (const tag of source.tags) {
      const normalizedTag = tag.toLowerCase();
      if (envTags.includes(normalizedTag)) {
        return normalizedTag;
      }

      // Check for abbreviated environments
      if (normalizedTag === "prod") return "production";
      if (normalizedTag === "dev") return "development";
      if (normalizedTag === "test") return "testing";
    }
  }

  // Try to extract from monitor name
  if (source.monitor?.name) {
    const name = source.monitor.name.toLowerCase();
    if (name.includes("prod") || name.includes("prd")) return "production";
    if (name.includes("dev")) return "development";
    if (name.includes("stage") || name.includes("stg")) return "staging";
    if (name.includes("test") || name.includes("qa")) return "testing";
  }

  // Try to extract from URL
  if (source.url?.domain) {
    const domain = source.url.domain.toLowerCase();
    if (domain.includes("prod") || domain.includes("prd")) return "production";
    if (domain.includes("dev")) return "development";
    if (domain.includes("stage") || domain.includes("stg")) return "staging";
    if (domain.includes("test") || domain.includes("qa")) return "testing";
  }

  // Default
  return "unknown";
}

// Extract business context directly from monitor tags
function extractBusinessContext(source) {
  const context = {
    domain: "unknown",
    department: "unknown",
    criticality: "medium", // Default criticality
    environment: "unknown",
  };

  // Extract from tags if available
  if (source.tags && Array.isArray(source.tags)) {
    for (const tag of source.tags) {
      // Check for domain tag
      if (tag.toLowerCase().startsWith("domain:")) {
        context.domain = tag.split(":")[1].trim().toLowerCase();
      }

      // Check for department tag
      if (tag.toLowerCase().startsWith("department:")) {
        context.department = tag.split(":")[1].trim().toLowerCase();
      }

      // Check for criticality/priority tags
      if (
        tag.toLowerCase().startsWith("criticality:") ||
        tag.toLowerCase().startsWith("priority:")
      ) {
        const value = tag.split(":")[1].trim().toLowerCase();
        context.criticality =
          value === "high" || value === "critical"
            ? "high"
            : value === "medium"
              ? "medium"
              : "low";
      }

      // Check for environment tags
      if (tag.toLowerCase() === "production" || tag.toLowerCase() === "prod") {
        context.environment = "production";
      } else if (
        tag.toLowerCase() === "development" ||
        tag.toLowerCase() === "dev"
      ) {
        context.environment = "development";
      } else if (
        tag.toLowerCase() === "staging" ||
        tag.toLowerCase() === "stage"
      ) {
        context.environment = "staging";
      } else if (
        tag.toLowerCase() === "testing" ||
        tag.toLowerCase() === "test"
      ) {
        context.environment = "testing";
      }
    }
  }

  // Try to extract environment from monitor name if not found in tags
  if (context.environment === "unknown" && source.monitor?.name) {
    const name = source.monitor.name.toLowerCase();
    if (name.includes("prod") || name.includes("prd")) {
      context.environment = "production";
    } else if (name.includes("dev")) {
      context.environment = "development";
    } else if (name.includes("stage") || name.includes("stg")) {
      context.environment = "staging";
    } else if (name.includes("test") || name.includes("qa")) {
      context.environment = "testing";
    }
  }

  // Try to extract from URL if environment still unknown
  if (context.environment === "unknown" && source.url?.domain) {
    const domain = source.url.domain.toLowerCase();
    if (domain.includes("prod") || domain.includes("prd")) {
      context.environment = "production";
    } else if (domain.includes("dev")) {
      context.environment = "development";
    } else if (domain.includes("stage") || domain.includes("stg")) {
      context.environment = "staging";
    } else if (domain.includes("test") || domain.includes("qa")) {
      context.environment = "testing";
    }
  }

  return context;
}

// Extract service information from monitor data
function extractServiceInfo(source) {
  // Start with default service info
  const serviceInfo = {
    name: "unknown",
    endpoint: "/",
  };

  // Check for explicit service tag
  if (source.tags && Array.isArray(source.tags)) {
    for (const tag of source.tags) {
      if (tag.toLowerCase().startsWith("service:")) {
        serviceInfo.name = tag.split(":")[1].trim();
        break;
      }
    }
  }

  // If no service tag found, extract from URL if available
  if (serviceInfo.name === "unknown" && source.url) {
    // Try to derive service name from domain
    if (source.url.domain) {
      const domainParts = source.url.domain.split(".");
      serviceInfo.name = domainParts[0];

      // Clean up common prefixes/suffixes
      serviceInfo.name = serviceInfo.name
        .replace(/-api$/, "")
        .replace(/^api-/, "")
        .replace(/-service$/, "");
    }

    // Get endpoint from path
    if (source.url.path) {
      serviceInfo.endpoint = source.url.path;
    }
  }

  // If still unknown, extract from monitor name if possible
  if (serviceInfo.name === "unknown" && source.monitor?.name) {
    // Parse monitor name patterns like "DS - API Health - prd | Process-api"
    const parts = source.monitor.name.split("|");
    if (parts.length > 1) {
      serviceInfo.name = parts[1]
        .trim()
        .replace(/-api$/, "")
        .replace(/^api-/, "")
        .replace(/-service$/, "");
    }
  }

  return serviceInfo;
}

// Send transformed monitor data to Kafka
async function sendToKafka(transformedData) {
  if (!transformedData || transformedData.length === 0) {
    return;
  }

  // Batch the messages by domain
  const messagesByDomain = {};

  for (const item of transformedData) {
    const domain = item.businessContext.domain || "unknown";
    const department = item.businessContext.department || "unknown";

    // Create domain key for kafka topics
    const domainKey = `${domain}`;

    if (!messagesByDomain[domainKey]) {
      messagesByDomain[domainKey] = [];
    }

    messagesByDomain[domainKey].push({
      key: item.id,
      value: JSON.stringify(item),
      headers: {
        "monitor-type": item.type,
        status: item.status,
        domain: domain,
        department: department,
        environment: item.environment,
      },
    });
  }

  // Send to domain-specific topics and the raw events topic
  const sendPromises = [];

  // Send to raw events topic
  sendPromises.push(
    producer.send({
      topic: "monitoring.raw.events",
      messages: transformedData.map((item) => ({
        key: item.id,
        value: JSON.stringify(item),
        headers: {
          "monitor-type": item.type,
          status: item.status,
          domain: item.businessContext.domain,
          department: item.businessContext.department,
          environment: item.environment,
        },
      })),
    }),
  );

  // Send to domain-specific topics
  for (const [domain, messages] of Object.entries(messagesByDomain)) {
    // Create a valid Kafka topic name (lowercase, replace spaces/special chars)
    const topicName = `monitoring.${domain.toLowerCase().replace(/[^a-z0-9]/g, "-")}.events`;
    sendPromises.push(
      producer.send({
        topic: topicName,
        messages,
      }),
    );
  }

  try {
    await Promise.all(sendPromises);
    console.log(
      `Successfully sent ${transformedData.length} messages to Kafka`,
    );
  } catch (error) {
    console.error("Error sending to Kafka:", error);
  }
}

// Start the extraction process on an interval
async function startExtractionProcess() {
  // Initial run
  await extractAndProcessMonitors();

  // Set up interval for regular extraction
  const intervalMinutes = parseInt(
    process.env.EXTRACTION_INTERVAL_MINUTES || "1",
    10,
  );
  console.log(
    `Setting up extraction to run every ${intervalMinutes} minute(s)`,
  );

  setInterval(extractAndProcessMonitors, intervalMinutes * 60 * 1000);
}

// Clean shutdown handler
process.on("SIGTERM", async () => {
  console.log("Received SIGTERM, shutting down gracefully");
  await producer.disconnect();
  process.exit(0);
});

// Run the extraction process if this file is executed directly
if (require.main === module) {
  startExtractionProcess().catch((error) => {
    console.error("Failed to start extraction process:", error);
    process.exit(1);
  });
}

export default {
  extractAndProcessMonitors,
  fetchAllMonitorData,
  transformMonitorData,
  sendToKafka,
};
