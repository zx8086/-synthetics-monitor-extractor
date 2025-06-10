/* src/index.ts */

import { Client, estypes } from "@elastic/elasticsearch";
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
import { 
  initializeMetrics, 
  startMetricsServer, 
  registry,
  kafkaMessageSizeHistogram
} from './metrics';
import { 
  getElasticsearchClient, 
  executeSearch, 
  closeElasticsearchClient,
  checkElasticsearchHealth
} from "./elasticsearch.js";
import {
  getKafkaProducer,
  sendMonitorDataToKafka,
  checkKafkaConnection,
  closeKafkaProducer
} from './kafka.js';

initializeMetrics();

// Main function to extract and process monitor data
async function extractAndProcessMonitors() {
  try {
    console.log("Starting synthetic monitor data extraction...");

    // Only check connection if we haven't established one yet
    const client = getElasticsearchClient();
    const isHealthy = await checkElasticsearchHealth();
    
    if (!isHealthy) {
      console.error("❌ Elasticsearch connection failed - skipping this extraction cycle");
      return;
    }

    // Check Kafka connection and list topics
    const { connected: isKafkaConnected } = await checkKafkaConnection();
    if (!isKafkaConnected) {
      console.error("❌ Kafka connection failed - skipping this extraction cycle");
      return;
    }

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

    // Send to Kafka topics using our service
    await sendMonitorDataToKafka(transformedData);

    console.log("Monitor extraction completed successfully");
  } catch (error) {
    console.error("Error in extraction process:", error);
  }
}

// Fetch all monitor data from Elasticsearch
async function fetchAllMonitorData() {
  const timeRange = config.extraction.timeRange;
  const size = config.extraction.maxResults;

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
                "monitor.name": config.extraction.monitorNamePattern,
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

    
    // Use the client directly for more complex operations
    const client = getElasticsearchClient();
    const response = await client.search<ElasticsearchHit["_source"]>(query);

    console.log("Query response:", {
      total: response.hits.total,
      took: response.took,
      timed_out: response.timed_out,
      hits: response.hits.hits.length,
    });

    if (response.hits.hits.length === 0) {
      console.log("No hits found. Checking if index exists...");
      const indices = await client.cat.indices({ format: "json" });

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
      status: source.monitor.status || "unknown",
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
    context.environment = extractEnvironmentFromSource(validatedSource.monitor.name);
  }

  // Try to extract from URL if environment still unknown
  if (context.environment === "unknown" && validatedSource.url?.domain) {
    context.environment = extractEnvironmentFromSource(validatedSource.url.domain);
  }

  return context;
}

function cleanServiceName(name: string): string {
  return name
    .replace(/-api$/, "")
    .replace(/^api-/, "")
    .replace(/-service$/, "");
}

function extractEnvironmentFromSource(source: string): string {
  const lowerSource = source.toLowerCase();
  if (lowerSource.includes("prod") || lowerSource.includes("prd")) {
    return "production";
  } else if (lowerSource.includes("dev")) {
    return "development";
  } else if (lowerSource.includes("stage") || lowerSource.includes("stg")) {
    return "staging";
  } else if (lowerSource.includes("test") || lowerSource.includes("qa")) {
    return "testing";
  }
  return "unknown";
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
      serviceInfo.name = cleanServiceName(serviceInfo.name);
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
      serviceInfo.name = cleanServiceName(
        parts[1]?.trim() || "unknown"
      );
    }
  }

  return serviceInfo;
}

// Start the extraction process on an interval
async function startExtractionProcess() {
  console.log("🚀 Starting synthetics monitor extractor...");
  
  try {
    // Initial connection validation
    const connectionValidation = await validateConnections();
    
    if (!connectionValidation.valid) {
      console.error("❌ Connection validation failed:", connectionValidation.errors.join(', '));
      console.log("⚠️ Continuing with interval setup - connections will be retried during extraction cycles");
    }
    
    if (connectionValidation.warnings && connectionValidation.warnings.length > 0) {
      console.log("✅ Connections validated:", connectionValidation.warnings.join(', '));
    }

    // Initial run
    console.log("Running initial data extraction...");
    await extractAndProcessMonitors();

    // Set up interval for regular extraction
    const intervalMinutes = config.extraction.intervalMinutes;
    console.log(
      `Setting up extraction to run every ${intervalMinutes} minute(s)`,
    );

    // Store the interval ID so we can clear it if needed
    const intervalId = setInterval(async () => {
      console.log(`\n🕒 Running scheduled extraction (every ${intervalMinutes} minutes)...`);
      try {
        await extractAndProcessMonitors();
        console.log(`✅ Scheduled extraction completed successfully`);
      } catch (error) {
        console.error(`❌ Scheduled extraction failed:`, error);
      }
    }, intervalMinutes * 60 * 1000);

    console.log(`✅ Extraction interval set up successfully - will run every ${intervalMinutes} minute(s)`);

    // Clean up interval on process termination
    process.on('SIGTERM', () => {
      console.log('Clearing extraction interval...');
      clearInterval(intervalId);
    });
  } catch (error) {
    console.error("Failed to start extraction process:", error);
    throw error;
  }
}

// Ensure clean shutdown
process.on("SIGTERM", async () => {
  console.log("Received SIGTERM, shutting down gracefully");
  try {
    await closeElasticsearchClient();
    await closeKafkaProducer();
    console.log("All connections closed successfully");
  } catch (error) {
    console.error("Error during shutdown:", error);
  }
  process.exit(0);
});

// Run the extraction process if this file is executed directly
if (import.meta.main) {
  (async () => {
    try {
      const metricsServer = await startMetricsServer();
      await startExtractionProcess();
    } catch (error) {
      console.error("Failed to start extraction process:", error);
      process.exit(1);
    }
  })();
}

export default {
  extractAndProcessMonitors,
  fetchAllMonitorData,
  transformMonitorData,
  sendMonitorDataToKafka,
};
