import { config, validateEnvironment } from "./src/config.js";

console.log("🧪 Testing configuration system...");

const envValidation = validateEnvironment();
console.log("Environment validation:", envValidation);

console.log("Loaded configuration:", {
  elasticsearch: {
    node: config.elasticsearch.node,
    hasApiKey: !!config.elasticsearch.apiKey,
    maxRetries: config.elasticsearch.maxRetries,
  },
  kafka: {
    clientId: config.kafka.clientId,
    brokers: config.kafka.brokers,
    ssl: config.kafka.ssl,
    hasAuth: !!config.kafka.username,
  },
  extraction: {
    intervalMinutes: config.extraction.intervalMinutes,
    timeRange: config.extraction.timeRange,
    indexPattern: config.extraction.indexPattern,
  },
  nodeEnv: config.nodeEnv,
});

console.log("✅ Configuration test completed successfully!");
