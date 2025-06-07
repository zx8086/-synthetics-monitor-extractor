/* src/index.ts */

import { Client, estypes } from "@elastic/elasticsearch";
import { Kafka } from "kafkajs";
import { HttpConnection } from "@elastic/transport";
import {
  type BusinessContext,
  type ServiceInfo,
  type MonitorInfo,
  type ElasticsearchHit,
  type SearchResponse,
  validateElasticsearchHits,
  validateMonitorInfo,
  ElasticsearchSourceSchema,
} from "./types.js";
import { config } from "./config.js";
import { validateConnections } from "./validation.js";

// Create Kafka client using configuration
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
  retry: {
    initialRetryTime: config.kafka.initialRetryTime,
    retries: config.kafka.retries,
  },
  connectionTimeout: config.kafka.connectionTimeout,
  authenticationTimeout: config.kafka.authenticationTimeout,
  requestTimeout: config.kafka.requestTimeout,
});

const producer = kafka.producer();

// Check Kafka connection and list topics
async function checkKafkaConnection() {
  try {
    console.log(
      "Attempting to connect to Kafka at:",
      config.kafka.brokers.join(","),
    );

    // Create admin client for topic operations
    const admin = kafka.admin();

    // Connect to Kafka with retry logic
    let retries = 3;
    while (retries > 0) {
      try {
        await admin.connect();
        console.log("✅ Successfully connected to Kafka");
        await new Promise((resolve) => setTimeout(resolve, 1000));
        break;
      } catch (error: any) {
        retries--;
        if (retries === 0) throw error;
        console.log(
          `Retrying Kafka connection... (${retries} attempts remaining)`,
        );
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }

    // List all topics
    const topics = await admin.listTopics();

    // Filter for monitoring topics
    const monitoringTopics = topics.filter((topic) =>
      topic.startsWith("monitoring."),
    );
    console.log("📋 Available monitoring topics:", monitoringTopics);

    // Get topic details for monitoring topics only
    const topicDetails = await admin.fetchTopicMetadata({
      topics: monitoringTopics,
    });

    console.log("📊 Monitoring topic details:");
    topicDetails.topics.forEach((topic) => {
      console.log(`  - ${topic.name}:`);
      console.log(`    Partitions: ${topic.partitions.length}`);
      console.log(
        `    Replication Factor: ${topic.partitions[0]?.replicas.length || "unknown"}`,
      );
    });

    // Disconnect admin client
    await admin.disconnect();

    return true;
  } catch (error: any) {
    console.error("❌ Failed to connect to Kafka");
    console.error("Error details:", error?.message || "Unknown error");
    if (error?.stack) {
      console.error("Stack trace:", error.stack);
    }
    return false;
  }
}

// Create a singleton Elasticsearch client
let elasticClientInstance: Client | null = null;

function getElasticsearchClient(): Client {
  if (!elasticClientInstance) {
    console.log("Creating new Elasticsearch client instance");
    
    const clientConfig: any = {
      node: config.elasticsearch.node,
      Connection: HttpConnection,
      compression: config.elasticsearch.compression,
      maxRetries: config.elasticsearch.maxRetries,
      requestTimeout: config.elasticsearch.requestTimeout,
      sniffOnStart: config.elasticsearch.sniffOnStart,
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
        rejectUnauthorized: config.elasticsearch.rejectUnauthorized,
      },
    };

    if (config.elasticsearch.apiKeyId && config.elasticsearch.apiKey) {
      clientConfig.auth = {
        apiKey: {
          id: config.elasticsearch.apiKeyId,
          api_key: config.elasticsearch.apiKey,
        },
      };
    }

    elasticClientInstance = new Client(clientConfig);
  }
  return elasticClientInstance;
}

// Check Elasticsearch connection with retry logic
async function checkElasticsearchConnection() {
  const maxRetries = 3;
  let retries = maxRetries;

  while (retries > 0) {
    try {
      const client = getElasticsearchClient();
      console.log(
        `Attempting to connect to Elasticsearch (${maxRetries - retries + 1}/${maxRetries})...`,
      );

      // Use a simple ping request instead of info() for initial connection test
      const pingResponse = await client.ping();

      if (pingResponse) {
        // If ping succeeds, then get cluster info
        const info = await client.info();

        console.log("✅ Successfully connected to Elasticsearch", {
          version: info.version?.number,
          clusterName: info.cluster_name,
          clusterUuid: info.cluster_uuid,
          luceneVersion: info.version?.lucene_version,
        });

        // Store version info for feature detection
        const serverVersion = info.version?.number ?? "0.0.0";
        const majorVersion = parseInt(serverVersion.split(".")[0] ?? "0");

        if (majorVersion >= 9) {
          console.log(
            `🆕 Connected to Elasticsearch ${serverVersion} - using modern client features`,
          );
        } else if (majorVersion >= 8) {
          console.log(
            `⚡ Connected to Elasticsearch ${serverVersion} - full feature support`,
          );
        } else {
          console.warn(
            `⚠️ Connected to older Elasticsearch ${serverVersion} - some features may be limited`,
          );
        }

        return true;
      }

      return false;
    } catch (error: any) {
      retries--;
      if (retries === 0) {
        console.error(
          "❌ Failed to connect to Elasticsearch after all retries",
        );
        console.error("Error details:", error?.message || "Unknown error");
        if (error?.meta?.body) {
          console.error("Response:", error.meta.body);
        }
        if (error?.meta?.statusCode) {
          console.error("Status code:", error.meta.statusCode);
        }
        if (error?.meta?.headers) {
          console.error("Response headers:", error.meta.headers);
        }
        return false;
      }
      console.log(
        `Retrying Elasticsearch connection... (${retries} attempts remaining)`,
      );
      await new Promise((resolve) => setTimeout(resolve, 2000)); // Wait 2 seconds between retries
    }
  }
  return false;
}

// Main function to extract and process monitor data
async function extractAndProcessMonitors() {
  try {
    console.log("Starting synthetic monitor data extraction...");

    // Check Elasticsearch connection first
    const isElasticConnected = await checkElasticsearchConnection();
    if (!isElasticConnected) {
      throw new Error("Failed to connect to Elasticsearch");
    }

    // Check Kafka connection and list topics
    const isKafkaConnected = await checkKafkaConnection();
    if (!isKafkaConnected) {
      throw new Error("Failed to connect to Kafka");
    }

    // Connect to Kafka producer
    await producer.connect();
    console.log("Connected to Kafka producer");

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
  const timeRange = config.extraction.timeRange;
  const size = config.extraction.maxResults;
  const client = getElasticsearchClient();

  try {
    // Build the query to get all synthetic monitors
    const query: estypes.SearchRequest = {
      index: config.extraction.indexPattern,
      track_total_hits: true,
      timeout: config.extraction.timeout,
      size,
      sort: [{ "@timestamp": "desc" }],
      query: {
        bool: {
          must: [
            {
              wildcard: {
                "monitor.name": "*",
              },
            },
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
    };

    console.log("Executing Elasticsearch query...");
    console.log("Query parameters:", {
      timeRange,
      size,
      index: config.extraction.indexPattern,
    });
    console.log("Full query:", JSON.stringify(query, null, 2));

    // Execute the search with timeout handling
    const response = await client
      .search<ElasticsearchHit["_source"]>(query)
      .catch((error) => {
        if (error.name === "TimeoutError") {
          console.error(
            "Elasticsearch query timed out. Retrying with longer timeout...",
          );
          // Retry with longer timeout
          return client.search<ElasticsearchHit["_source"]>({
            ...query,
            timeout: "120s",
          });
        }
        throw error;
      });

    console.log("Query response:", {
      total: response.hits.total,
      took: response.took,
      timed_out: response.timed_out,
      hits: response.hits.hits.length,
    });

    if (response.hits.hits.length === 0) {
      console.log("No hits found. Checking if index exists...");
      const indices = await client.cat.indices({ format: "json" });
      console.log(
        "Available indices:",
        indices.map((idx) => idx.index),
      );
    }

    const rawHits = response.hits.hits.map((hit) => ({
      _source: hit._source,
    }));

    const validatedHits = validateElasticsearchHits(rawHits);
    console.log(`Validated ${validatedHits.length} Elasticsearch hits`);

    return validatedHits;
  } catch (error: any) {
    console.error(`Error fetching monitor data:`, error);
    if (error.meta?.body) {
      console.error("Elasticsearch response:", error.meta.body);
    }
    if (error.meta?.statusCode) {
      console.error("Status code:", error.meta.statusCode);
    }
    return [];
  }
}

// Transform monitor data into our standardized format
function transformMonitorData(monitorData: ElasticsearchHit[]): MonitorInfo[] {
  const transformedData = monitorData.map((hit) => {
    const source = hit._source;

    // Extract business context directly from tags
    const businessContext = extractBusinessContext(source);

    // Extract base monitor info
    const monitorInfo: MonitorInfo = {
      id: source.monitor.id,
      name: source.monitor.name,
      type: source.monitor.type,
      url: source.url?.full,
      timestamp: source["@timestamp"],
      status: source.monitor.status,
      duration: source.monitor.duration?.us
        ? source.monitor.duration.us / 1000
        : 0, // Convert to ms
      businessContext: businessContext,
      tags: source.tags || [],
      environment: businessContext.environment,
      service: extractServiceInfo(source),
      agent: source.agent
        ? {
            name: source.agent.name,
            id: source.agent.id,
            type: source.agent.type,
            version: source.agent.version,
          }
        : undefined,
      observer: source.observer
        ? {
            name: source.observer.name,
            geo: source.observer.geo?.name,
          }
        : undefined,
      meta: source.meta,
      http: source.http
        ? {
            statusCode: source.http.response?.status_code,
            responseTime: source.http.rtt?.total?.us
              ? source.http.rtt.total.us / 1000
              : undefined,
            body: source.http.response?.body
              ? {
                  bytes: source.http.response.body.bytes,
                  content: source.http.response.body.content,
                }
              : undefined,
          }
        : undefined,
    };

    // Add TLS-specific details if available
    if (source.tls) {
      monitorInfo.tls = {
        established: source.tls.established,
        version: source.tls.version,
      };
    }

    return monitorInfo;
  });

  const validatedData = validateMonitorInfo(transformedData);
  console.log(`Validated ${validatedData.length} transformed monitor records`);

  return validatedData;
}

// Extract business context directly from monitor tags
function extractBusinessContext(
  source: ElasticsearchHit["_source"],
): BusinessContext {
  const validatedSource = ElasticsearchSourceSchema.parse(source);
  
  const context: BusinessContext = {
    domain: "unknown",
    department: "unknown",
    criticality: "medium", // Default criticality
    environment: "unknown",
  };

  // Extract from tags if available
  if (validatedSource.tags && Array.isArray(validatedSource.tags)) {
    for (const tag of validatedSource.tags) {
      const lowerTag = tag.toLowerCase();

      // Check for domain tag
      if (lowerTag.startsWith("domain:")) {
        context.domain = tag.split(":")[1]?.trim().toLowerCase() || "unknown";
      }

      // Check for department tag
      if (lowerTag.startsWith("department:")) {
        context.department =
          tag.split(":")[1]?.trim().toLowerCase() || "unknown";
      }

      // Check for criticality/priority tags
      if (
        lowerTag.startsWith("criticality:") ||
        lowerTag.startsWith("priority:")
      ) {
        const value = tag.split(":")[1]?.trim().toLowerCase() || "medium";
        context.criticality =
          value === "high" || value === "critical"
            ? "high"
            : value === "medium"
              ? "medium"
              : "low";
      }

      // Check for environment tags
      if (lowerTag.startsWith("environment:")) {
        const env = tag.split(":")[1]?.trim().toLowerCase() || "unknown";
        context.environment = env;
      } else if (lowerTag === "production" || lowerTag === "prod") {
        context.environment = "production";
      } else if (lowerTag === "development" || lowerTag === "dev") {
        context.environment = "development";
      } else if (lowerTag === "staging" || lowerTag === "stage") {
        context.environment = "staging";
      } else if (lowerTag === "testing" || lowerTag === "test") {
        context.environment = "testing";
      }
    }
  }

  // Try to extract environment from monitor name if not found in tags
  if (context.environment === "unknown" && validatedSource.monitor?.name) {
    const name = validatedSource.monitor.name.toLowerCase();
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
  if (context.environment === "unknown" && validatedSource.url?.domain) {
    const domain = validatedSource.url.domain.toLowerCase();
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
function extractServiceInfo(source: ElasticsearchHit["_source"]): ServiceInfo {
  const validatedSource = ElasticsearchSourceSchema.parse(source);
  
  // Start with default service info
  const serviceInfo: ServiceInfo = {
    name: "unknown",
    endpoint: "/",
  };

  // Check for explicit service tag
  if (validatedSource.tags && Array.isArray(validatedSource.tags)) {
    for (const tag of validatedSource.tags) {
      if (tag.toLowerCase().startsWith("service:")) {
        serviceInfo.name = tag.split(":")[1]?.trim() || "unknown";
        break;
      }
    }
  }

  // If no service tag found, extract from URL if available
  if (serviceInfo.name === "unknown" && validatedSource.url) {
    // Try to derive service name from domain
    if (validatedSource.url.domain) {
      const domainParts = validatedSource.url.domain.split(".");
      serviceInfo.name = domainParts[0] || "unknown";

      // Clean up common prefixes/suffixes
      serviceInfo.name = serviceInfo.name
        .replace(/-api$/, "")
        .replace(/^api-/, "")
        .replace(/-service$/, "");
    }

    // Get endpoint from path
    if (validatedSource.url.path) {
      serviceInfo.endpoint = validatedSource.url.path;
    }
  }

  // If still unknown, extract from monitor name if possible
  if (serviceInfo.name === "unknown" && validatedSource.monitor?.name) {
    // Parse monitor name patterns like "DS - API Health - prd | Process-api"
    const parts = validatedSource.monitor.name.split("|");
    if (parts.length > 1) {
      serviceInfo.name =
        parts[1]
          ?.trim()
          .replace(/-api$/, "")
          .replace(/^api-/, "")
          .replace(/-service$/, "") || "unknown";
    }
  }

  return serviceInfo;
}

// Send transformed monitor data to Kafka
async function sendToKafka(transformedData: MonitorInfo[]) {
  if (!transformedData || transformedData.length === 0) {
    return;
  }

  const validatedData = validateMonitorInfo(transformedData);
  console.log(`Final validation: ${validatedData.length} records ready for Kafka`);

  // Batch the messages by domain
  const messagesByDomain: Record<string, any[]> = {};

  for (const item of validatedData) {
    const domain = item.businessContext.domain || "unknown";
    const department = item.businessContext.department || "unknown";
    const timestamp = new Date(item.timestamp).getTime();

    // Generate a unique document ID that includes timestamp
    const uniqueKey = `raw::${domain}::${item.id}::${timestamp}`;

    // Create domain key for kafka topics
    const domainKey = `${domain}`;

    if (!messagesByDomain[domainKey]) {
      messagesByDomain[domainKey] = [];
    }

    messagesByDomain[domainKey].push({
      key: uniqueKey, // Use the unique key here
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

  console.log("Messages grouped by domain:", Object.keys(messagesByDomain));

  // Send to domain-specific topics and the raw events topic
  const sendPromises = [];

  // Send to raw events topic
  console.log(
    `Sending ${validatedData.length} messages to topic: monitoring.raw.events`,
  );
  sendPromises.push(
    producer
      .send({
        topic: "monitoring.raw.events",
        messages: validatedData.map((item) => {
          const domain = item.businessContext.domain || "unknown";
          const timestamp = new Date(item.timestamp).getTime();
          const uniqueKey = `raw::${domain}::${item.id}::${timestamp}`;

          return {
            key: uniqueKey, // Use the unique key here too
            value: JSON.stringify(item),
            headers: {
              "monitor-type": item.type,
              status: item.status,
              domain: domain,
              department: item.businessContext.department,
              environment: item.environment,
            },
          };
        }),
      })
      .then(() => {
        console.log(`Successfully sent messages to monitoring.raw.events`);
      })
      .catch((error) => {
        console.error(
          `Failed to send messages to monitoring.raw.events:`,
          error,
        );
      }),
  );

  // Send to domain-specific topics
  for (const [domain, messages] of Object.entries(messagesByDomain)) {
    // Create a valid Kafka topic name (lowercase, replace spaces/special chars)
    const topicName = `monitoring.${domain.toLowerCase().replace(/[^a-z0-9]/g, "-")}.events`;
    console.log(`Sending ${messages.length} messages to topic: ${topicName}`);

    sendPromises.push(
      producer
        .send({
          topic: topicName,
          messages,
        })
        .then(() => {
          console.log(`Successfully sent messages to ${topicName}`);
        })
        .catch((error) => {
          console.error(`Failed to send messages to ${topicName}:`, error);
        }),
    );
  }

  try {
    await Promise.all(sendPromises);
    console.log(
      `Successfully sent ${validatedData.length} messages to Kafka`,
    );
  } catch (error) {
    console.error("Error sending to Kafka:", error);
  }
}

// Start the extraction process on an interval
async function startExtractionProcess() {
  console.log("🚀 Starting synthetics monitor extractor...");
  const connectionValidation = await validateConnections();
  
  if (!connectionValidation.valid) {
    console.error("❌ Connection validation failed:", connectionValidation.errors.join(', '));
    process.exit(1);
  }
  
  if (connectionValidation.warnings && connectionValidation.warnings.length > 0) {
    console.log("✅ Connections validated:", connectionValidation.warnings.join(', '));
  }

  // Initial run
  await extractAndProcessMonitors();

  // Set up interval for regular extraction
  const intervalMinutes = config.extraction.intervalMinutes;
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
if (import.meta.main) {
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
