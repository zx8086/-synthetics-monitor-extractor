/* src/index.ts */

import { Client, estypes } from "@elastic/elasticsearch";
import { HttpConnection } from "@elastic/transport";
import {
  type BusinessContext,
  type MonitorInfo,
  type ElasticsearchHit,
  type SearchResponse,
  validateElasticsearchHits,
  validateMonitorInfo,
  ElasticsearchSourceSchema,
  writeInvalidRecords,
  clearInvalidRecordsBuffer,
} from "./types.js";
import { config } from "./config.js";
import { validateConnections } from "./validation.js";
import {
  initializeMetrics,
  startMetricsServer,
  registry,
  kafkaMessageSizeHistogram,
} from "./metrics";
import {
  getElasticsearchClient,
  executeSearch,
  closeElasticsearchClient,
  checkElasticsearchHealth,
} from "./elasticsearch.js";
import {
  getKafkaProducer,
  sendMonitorDataToKafka,
  checkKafkaConnection,
  closeKafkaProducer,
} from "./kafka.js";

initializeMetrics();

// Main function to extract and process monitor data
async function extractAndProcessMonitors() {
  try {
    console.log("Starting synthetic monitor data extraction...");

    // Clear the invalid records buffer at the start of each extraction
    clearInvalidRecordsBuffer();

    // Only check connection if we haven't established one yet
    const client = getElasticsearchClient();
    const isHealthy = await checkElasticsearchHealth();

    if (!isHealthy) {
      console.error(
        "❌ Elasticsearch connection failed - skipping this extraction cycle",
      );
      return;
    }

    // Check Kafka connection and list topics
    const { connected: isKafkaConnected } = await checkKafkaConnection();
    if (!isKafkaConnected) {
      console.error(
        "❌ Kafka connection failed - skipping this extraction cycle",
      );
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
    const transformedData = await transformMonitorData(monitorData);

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
  const monitorNameWildcard = config.extraction.monitorNamePattern;

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
                "monitor.name": monitorNameWildcard,
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
      monitorNameWildcard,
    });

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
      console.log(
        "Available indices:",
        indices.map((idx) => idx.index),
      );
    }

    // Pass through raw hits without transformation
    const rawHits = response.hits.hits.map(hit => ({ _source: hit._source }));

    const validatedHits = await validateElasticsearchHits(rawHits);
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



export function extractBusinessContext(source: ElasticsearchHit["_source"]): BusinessContext {
  const tags = source.tags || [];
  const missingFields: string[] = [];
  let domain = "unknown";
  let department = "unknown";
  let criticality: "high" | "medium" | "low" = "medium";
  let environment = "unknown";

  for (const tag of tags) {
    if (tag.startsWith("domain:")) {
      domain = tag.replace("domain:", "").trim();
    } else if (tag.startsWith("department:")) {
      department = tag.replace("department:", "").trim();
    } else if (tag.startsWith("criticality:")) {
      const crit = tag.replace("criticality:", "").trim().toLowerCase();
      if (crit === "high" || crit === "medium" || crit === "low") {
        criticality = crit;
      }
    } else if (tag.startsWith("environment:")) {
      environment = tag.replace("environment:", "").trim();
    }
  }

  if (domain === "unknown") missingFields.push("domain");
  if (department === "unknown") missingFields.push("department");
  const hasCriticalityTag = tags.some(tag => tag.startsWith("criticality:"));
  if (!hasCriticalityTag && criticality === "medium") missingFields.push("criticality");
  if (environment === "unknown") missingFields.push("environment");

  if (missingFields.length > 0) {
    throw new Error(`Missing business context fields: ${missingFields.join(", ")}`);
  }

  return {
    domain,
    department,
    criticality,
    environment,
  };
}

// Transform monitor data with comprehensive field extraction
async function transformMonitorData(monitorData: ElasticsearchHit[]): Promise<MonitorInfo[]> {
  const transformedData: MonitorInfo[] = [];
  const errorsByMonitor: Record<string, Set<string>> = {};

  for (const hit of monitorData) {
    try {
      const businessContext = extractBusinessContext(hit._source);
      const transformedMonitor: MonitorInfo = {
        id: hit._source.monitor.id,
        name: hit._source.monitor.name,
        type: hit._source.monitor.type,
        url: hit._source.url?.full,
        timestamp: hit._source["@timestamp"],
        status: hit._source.monitor.status,
        duration: hit._source.monitor.duration?.us || 0,
        businessContext,
        tags: hit._source.tags || [],
        environment: hit._source.observer?.geo?.name || "unknown",
        
        http: hit._source.http
          ? {
              statusCode: hit._source.http.response?.status_code,
              responseTime: hit._source.http.rtt?.total?.us,
              body: hit._source.http.response?.body
                ? {
                    bytes: hit._source.http.response.body.bytes,
                    content: hit._source.http.response.body.content,
                    hash: hit._source.http.response.body.hash,
                  }
                : undefined,
              response: hit._source.http.response,
              rtt: hit._source.http.rtt,
              state: hit._source.http.state,
            }
          : undefined,
          
        tls: hit._source.tls
          ? {
              established: hit._source.tls.established,
              version: hit._source.tls.version,
              cipher: hit._source.tls.cipher,
              certificate_not_valid_before: hit._source.tls.certificate_not_valid_before,
              certificate_not_valid_after: hit._source.tls.certificate_not_valid_after,
              version_protocol: hit._source.tls.version_protocol,
              server: hit._source.tls.server,
            }
          : undefined,
          
        tcp: hit._source.tcp,
          
        icmp: hit._source.icmp,
          
        synthetics: hit._source.synthetics,
        
        summary: hit._source.summary,
        
        state: hit._source.state,
        
        event: hit._source.event,
        
        data_stream: hit._source.data_stream,
        
        ecs: hit._source.ecs,
        
        config_id: hit._source.config_id,
        
        agent: hit._source.agent
          ? {
              name: hit._source.agent.name,
              id: hit._source.agent.id,
              type: hit._source.agent.type,
              version: hit._source.agent.version,
              ephemeral_id: hit._source.agent.ephemeral_id,
            }
          : undefined,
          
        observer: hit._source.observer
          ? {
              name: hit._source.observer.name || "",
              geo: hit._source.observer.geo?.name,
            }
          : undefined,
          
        meta: hit._source.meta
          ? {
              space_id: hit._source.meta.space_id || "",
            }
          : undefined,
          
        project: hit._source.monitor.project,
        timespan: hit._source.monitor.timespan,
        check_group: hit._source.monitor.check_group,
        fleet_managed: hit._source.monitor.fleet_managed,
        origin: hit._source.monitor.origin,
        ip: hit._source.monitor.ip,
      };

      transformedData.push(transformedMonitor);
    } catch (error) {
      const monitorName = hit._source.monitor.name;
      if (!errorsByMonitor[monitorName]) {
        errorsByMonitor[monitorName] = new Set();
      }
      const errorMessage = error instanceof Error ? error.message : String(error);
      errorsByMonitor[monitorName].add(errorMessage);
    }
  }

  // Write errors for each monitor separately
  for (const [monitorName, errorSet] of Object.entries(errorsByMonitor)) {
    if (errorSet.size > 0) {
      const errors = Array.from(errorSet).map(message => ({ message }));
      await writeInvalidRecords('monitor_transformation', errors, monitorName);
    }
  }

  return transformedData;
}

// Start the extraction process on an interval
async function startExtractionProcess() {
  console.log("🚀 Starting synthetics monitor extractor...");

  try {
    // Initial connection validation
    const connectionValidation = await validateConnections();

    if (!connectionValidation.valid) {
      console.error(
        "❌ Connection validation failed:",
        connectionValidation.errors.join(", "),
      );
      console.log(
        "⚠️ Continuing with interval setup - connections will be retried during extraction cycles",
      );
    }

    if (
      connectionValidation.warnings &&
      connectionValidation.warnings.length > 0
    ) {
      console.log(
        "✅ Connections validated:",
        connectionValidation.warnings.join(", "),
      );
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
    const intervalId = setInterval(
      async () => {
        console.log(
          `\n🕒 Running scheduled extraction (every ${intervalMinutes} minutes)...`,
        );
        try {
          await extractAndProcessMonitors();
          console.log(`✅ Scheduled extraction completed successfully`);
        } catch (error) {
          console.error(`❌ Scheduled extraction failed:`, error);
        }
      },
      intervalMinutes * 60 * 1000,
    );

    console.log(
      `✅ Extraction interval set up successfully - will run every ${intervalMinutes} minute(s)`,
    );

    // Clean up interval on process termination
    process.on("SIGTERM", () => {
      console.log("Clearing extraction interval...");
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
