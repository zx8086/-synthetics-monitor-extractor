/* src/index.ts */

import { Client, estypes } from "@elastic/elasticsearch";
import { HttpConnection } from "@elastic/transport";
import type { MonitorInfo, SourceDocument, ElasticsearchHit } from "./types";
import {
  type BusinessContext,
  type SearchResponse,
  validateElasticsearchHits,
  validateMonitorInfo,
  ElasticsearchSourceSchema,
  writeInvalidRecords,
  clearInvalidRecordsBuffer,
} from "./types.js";
import type { InvalidRecord } from "./types.js";
import { config } from "./config.js";
import { validateConnections } from "./validation.js";
import {
  initializeMetrics,
  startMetricsServer,
  registry,
  kafkaMessageSizeHistogram,
} from "./metrics.js";
import { initializeDatabase, closeDatabase } from "./database.js";
import { startApiServer } from "./api.js";
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
              "wildcard": {
                "monitor.name": monitorNameWildcard,
              },
            },
            // Only get monitors that have a summary
            { "exists": { "field": "summary" } },
            {
              "range": {
                "@timestamp": {
                  gte: timeRange,
                },
              },
            },
          ],
          // Make sure we're only getting synthetic monitoring data
          filter: [
            {
              terms: {
                "data_stream.dataset": ["http", "browser", "icmp", "tcp"],
              },
            },
            {
              term: {
                "data_stream.type": "synthetics",
              },
            },
          ],
        },
      },
      // Include all the fields we need
      _source: [
        "monitor",
        "url",
        "@timestamp",
        "tags",
        "http",
        "tcp",
        "icmp",
        "synthetics",
        "agent",
        "observer",
        "meta",
        "summary",
        "state",
        "data_stream",
        "ecs",
        "config_id",
      ],
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

    // Validate the Elasticsearch hits using the old validation method
    const validatedHits = await validateElasticsearchHits(rawHits);
    console.log(`Validated ${validatedHits.length} Elasticsearch hits`);

    // Count by dataset type
    const datasetCounts: Record<string, number> = {};
    validatedHits.forEach(hit => {
      const dataset = hit._source.data_stream?.dataset || "unknown";
      datasetCounts[dataset] = (datasetCounts[dataset] || 0) + 1;
    });
    console.log("Hits by dataset type:", datasetCounts);

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
      
      const httpWithTls = hit._source.http
        ? {
            ...hit._source.http,
            ...(hit._source.http.tls ? { tls: hit._source.http.tls } : {}),
          }
        : undefined;
      const transformedMonitor: MonitorInfo = {
        id: hit._source.monitor.id,
        name: hit._source.monitor.name,
        type: hit._source.monitor.type,
        url: hit._source.url || {
          scheme: undefined,
          domain: undefined,
          port: undefined,
          path: undefined,
          full: undefined
        },
        timestamp: hit._source["@timestamp"],
        businessContext,
        tags: hit._source.tags || [],
        monitor: {
          id: hit._source.monitor.id,
          name: hit._source.monitor.name,
          type: hit._source.monitor.type,
          status: hit._source.monitor.status,
          duration: hit._source.monitor.duration,
          ip: hit._source.monitor.ip,
          origin: hit._source.monitor.origin,
          timespan: hit._source.monitor.timespan,
          fleet_managed: hit._source.monitor.fleet_managed,
          check_group: hit._source.monitor.check_group,
          project: hit._source.monitor.project
        },
        ...(httpWithTls ? { http: httpWithTls } : {}),
        ...(hit._source.tcp ? { tcp: hit._source.tcp } : {}),
        ...(hit._source.icmp ? { icmp: hit._source.icmp } : {}),
        ...(hit._source.synthetics ? { synthetics: { type: hit._source.synthetics.type } } : {}),
        ...(hit._source.summary ? { summary: {
          retry_group: hit._source.summary.retry_group || "",
          max_attempts: hit._source.summary.max_attempts || 0,
          up: hit._source.summary.up || 0,
          down: hit._source.summary.down || 0,
          attempt: hit._source.summary.attempt || 0,
          final_attempt: hit._source.summary.final_attempt || false,
          status: hit._source.summary.status || ""
        } } : {}),
        ...(hit._source.state ? { state: {
          duration_ms: hit._source.state.duration_ms || "",
          checks: hit._source.state.checks || 0,
          ends: hit._source.state.ends || null,
          started_at: hit._source.state.started_at || "",
          up: hit._source.state.up || 0,
          id: hit._source.state.id || "",
          down: hit._source.state.down || 0,
          flap_history: hit._source.state.flap_history || [],
          status: hit._source.state.status || ""
        } } : {}),
        ...(hit._source.event ? { event: {
          agent_id_status: hit._source.event.agent_id_status,
          ingested: hit._source.event.ingested,
          type: hit._source.event.type,
          dataset: hit._source.event.dataset
        } } : {}),
        ...(hit._source.data_stream ? { data_stream: hit._source.data_stream } : {}),
        ...(hit._source.ecs ? { ecs: hit._source.ecs } : {}),
        ...(hit._source.config_id ? { config_id: hit._source.config_id } : {}),
        ...(hit._source.agent ? { agent: hit._source.agent } : {}),
        ...(hit._source.observer ? { observer: hit._source.observer } : {}),
        ...(hit._source.meta ? { meta: hit._source.meta } : {})
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
    // Initialize database
    initializeDatabase();
    
    // Start API server (combines metrics and invalid records API)
    console.log(`Starting API server on port ${config.metrics.port}...`);
    const apiServer = startApiServer(config.metrics.port);
    
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
      `Setting up extraction to run every ${intervalMinutes} minute(s)`
    );

    // Store the interval ID so we can clear it if needed
    const intervalId = setInterval(
      async () => {
        console.log(
          `\n🕒 Running scheduled extraction (every ${intervalMinutes} minutes)...`
        );
        try {
          await extractAndProcessMonitors();
          console.log(`✅ Scheduled extraction completed successfully`);
        } catch (error) {
          console.error(`❌ Scheduled extraction failed:`, error);
        }
      },
      intervalMinutes * 60 * 1000
    );

    console.log(
      `✅ Extraction interval set up successfully - will run every ${intervalMinutes} minute(s)`
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
    closeDatabase(); // Close the database connection
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
      // Initialize metrics but don't start a separate server
      initializeMetrics();
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