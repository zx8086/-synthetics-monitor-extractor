/* src/config.ts */

import { z } from "zod";

const ElasticsearchConfigSchema = z
  .object({
    node: z.string().url().min(1).default("https://elasticsearch:9200"),
    apiKeyId: z.string().optional(),
    apiKey: z.string().optional(),
    maxRetries: z.number().min(0).max(10).default(5),
    requestTimeout: z.number().min(1000).max(60000).default(30000),
    compression: z.boolean().default(true),
    sniffOnStart: z.boolean().default(false),
    rejectUnauthorized: z.boolean().default(true),
    name: z.string().default("synthetics-extractor"),
    opaqueIdPrefix: z.string().default("synthetics-extractor::"),
  })
  .refine(
    (data) => {
      if (data.apiKeyId) {
        return !!data.apiKey;
      }

      if (data.apiKey) {
        return !!data.apiKeyId;
      }

      return true;
    },
    {
      message:
        "Both ELASTIC_API_KEY_ID and ELASTIC_API_KEY must be provided together, or neither for local development",
      path: ["apiKeyId", "apiKey"],
    },
  );

const KafkaConfigSchema = z
  .object({
    clientId: z.string().min(1).default("synthetics-extractor"),
    brokers: z.array(z.string().min(1)).min(1),
    ssl: z.boolean().default(false),
    username: z.string().optional(),
    password: z.string().optional(),
    connectionTimeout: z.number().min(1000).max(30000).default(3000),
    authenticationTimeout: z.number().min(1000).max(30000).default(3000),
    requestTimeout: z.number().min(1000).max(60000).default(30000),
    initialRetryTime: z.number().min(100).max(10000).default(1000),
    retries: z.number().min(0).max(20).default(8),
  })
  .refine(
    (data) => {
      if (data.ssl) {
        return !!data.username && !!data.password;
      }
      return true;
    },
    {
      message:
        "When KAFKA_SSL is enabled, both KAFKA_USERNAME and KAFKA_PASSWORD should be provided",
      path: ["username", "password"],
    },
  );

const ExtractionConfigSchema = z.object({
  intervalMinutes: z.number().min(1).max(60).default(1),
  timeRange: z.string().default("now-5m"),
  maxResults: z.number().min(1).max(10000).default(1000),
  timeout: z.string().default("30s"),
  indexPattern: z.string().default("synthetics-*"),
  monitorNamePattern: z.string().default("*"),
});

const LoggingConfigSchema = z.object({
  level: z.enum(["debug", "info", "warn", "error"]).default("info"),
  format: z.enum(["json", "text"]).default("text"),
  includeTimestamp: z.boolean().default(true),
});

const ConfigSchema = z.object({
  elasticsearch: ElasticsearchConfigSchema,
  kafka: KafkaConfigSchema,
  extraction: ExtractionConfigSchema,
  logging: LoggingConfigSchema,
  nodeEnv: z.enum(["development", "production", "staging"]).default("development"),
});

export type Config = z.infer<typeof ConfigSchema>;

const defaultConfig: Config = {
  elasticsearch: {
    node: "https://elasticsearch:9200",
    maxRetries: 5,
    requestTimeout: 30000,
    compression: true,
    sniffOnStart: false,
    rejectUnauthorized: true,
    name: "synthetics-extractor",
    opaqueIdPrefix: "synthetics-extractor::",
  },
  kafka: {
    clientId: "synthetics-extractor",
    brokers: ["localhost:9092"],
    ssl: false,
    connectionTimeout: 3000,
    authenticationTimeout: 3000,
    requestTimeout: 30000,
    initialRetryTime: 1000,
    retries: 8,
  },
  extraction: {
    intervalMinutes: 1,
    timeRange: "now-5m",
    maxResults: 1000,
    timeout: "30s",
    indexPattern: "synthetics-*",
    monitorNamePattern: "*",
  },
  logging: {
    level: "info",
    format: "text",
    includeTimestamp: true,
  },
  nodeEnv: "development",
};

const envVarMapping = {
  elasticsearch: {
    node: "ELASTIC_NODE",
    apiKeyId: "ELASTIC_API_KEY_ID",
    apiKey: "ELASTIC_API_KEY",
    maxRetries: "ELASTIC_MAX_RETRIES",
    requestTimeout: "ELASTIC_REQUEST_TIMEOUT",
    compression: "ELASTIC_COMPRESSION",
    sniffOnStart: "ELASTIC_SNIFF_ON_START",
    rejectUnauthorized: "ELASTIC_REJECT_UNAUTHORIZED",
    name: "ELASTIC_NAME",
    opaqueIdPrefix: "ELASTIC_OPAQUE_ID_PREFIX",
  },
  kafka: {
    clientId: "KAFKA_CLIENT_ID",
    brokers: "KAFKA_BROKERS",
    ssl: "KAFKA_SSL",
    username: "KAFKA_USERNAME",
    password: "KAFKA_PASSWORD",
    connectionTimeout: "KAFKA_CONNECTION_TIMEOUT",
    authenticationTimeout: "KAFKA_AUTHENTICATION_TIMEOUT",
    requestTimeout: "KAFKA_REQUEST_TIMEOUT",
    initialRetryTime: "KAFKA_INITIAL_RETRY_TIME",
    retries: "KAFKA_RETRIES",
  },
  extraction: {
    intervalMinutes: "EXTRACTION_INTERVAL_MINUTES",
    timeRange: "EXTRACTION_TIME_RANGE",
    maxResults: "EXTRACTION_MAX_RESULTS",
    timeout: "EXTRACTION_TIMEOUT",
    indexPattern: "EXTRACTION_INDEX_PATTERN",
    monitorNamePattern: "EXTRACTION_MONITOR_NAME_PATTERN",
  },
  logging: {
    level: "LOG_LEVEL",
    format: "LOG_FORMAT",
    includeTimestamp: "LOG_INCLUDE_TIMESTAMP",
  },
  nodeEnv: "NODE_ENV",
} as const;

function parseEnvVar(
  value: string | undefined,
  type: "string" | "number" | "boolean" | "array",
): unknown {
  if (value === undefined) return undefined;
  if (type === "number") return Number(value);
  if (type === "boolean") return value.toLowerCase() === "true";
  if (type === "array")
    return value
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
  return value;
}

function getEnvValue(envVar: string): string | undefined {
  return (
    (typeof Bun !== "undefined" && Bun.env ? Bun.env[envVar] : undefined) ||
    process.env[envVar]
  );
}

function loadConfigFromEnv(): Partial<Config> {
  const envConfig: Record<string, any> = {};

  // Helper function to safely parse environment variables
  const getEnvConfig = (key: string, type: "string" | "number" | "boolean" | "array") => {
    const value = getEnvValue(key);
    if (value === undefined) return undefined;
    return parseEnvVar(value, type);
  };

  // Load Elasticsearch config
  envConfig.elasticsearch = {
    node: getEnvConfig(envVarMapping.elasticsearch.node, "string"),
    apiKeyId: getEnvConfig(envVarMapping.elasticsearch.apiKeyId, "string"),
    apiKey: getEnvConfig(envVarMapping.elasticsearch.apiKey, "string"),
    maxRetries: getEnvConfig(envVarMapping.elasticsearch.maxRetries, "number"),
    requestTimeout: getEnvConfig(envVarMapping.elasticsearch.requestTimeout, "number"),
    compression: getEnvConfig(envVarMapping.elasticsearch.compression, "boolean"),
    sniffOnStart: getEnvConfig(envVarMapping.elasticsearch.sniffOnStart, "boolean"),
    rejectUnauthorized: getEnvConfig(envVarMapping.elasticsearch.rejectUnauthorized, "boolean"),
    name: getEnvConfig(envVarMapping.elasticsearch.name, "string"),
    opaqueIdPrefix: getEnvConfig(envVarMapping.elasticsearch.opaqueIdPrefix, "string"),
  };

  // Load Kafka config
  envConfig.kafka = {
    clientId: getEnvConfig(envVarMapping.kafka.clientId, "string"),
    brokers: getEnvConfig(envVarMapping.kafka.brokers, "array"),
    ssl: getEnvConfig(envVarMapping.kafka.ssl, "boolean"),
    username: getEnvConfig(envVarMapping.kafka.username, "string"),
    password: getEnvConfig(envVarMapping.kafka.password, "string"),
    connectionTimeout: getEnvConfig(envVarMapping.kafka.connectionTimeout, "number"),
    authenticationTimeout: getEnvConfig(envVarMapping.kafka.authenticationTimeout, "number"),
    requestTimeout: getEnvConfig(envVarMapping.kafka.requestTimeout, "number"),
    initialRetryTime: getEnvConfig(envVarMapping.kafka.initialRetryTime, "number"),
    retries: getEnvConfig(envVarMapping.kafka.retries, "number"),
  };

  // Load Extraction config
  envConfig.extraction = {
    intervalMinutes: getEnvConfig(envVarMapping.extraction.intervalMinutes, "number"),
    timeRange: getEnvConfig(envVarMapping.extraction.timeRange, "string"),
    maxResults: getEnvConfig(envVarMapping.extraction.maxResults, "number"),
    timeout: getEnvConfig(envVarMapping.extraction.timeout, "string"),
    indexPattern: getEnvConfig(envVarMapping.extraction.indexPattern, "string"),
    monitorNamePattern: getEnvConfig(envVarMapping.extraction.monitorNamePattern, "string"),
  };

  // Load Logging config
  envConfig.logging = {
    level: getEnvConfig(envVarMapping.logging.level, "string"),
    format: getEnvConfig(envVarMapping.logging.format, "string"),
    includeTimestamp: getEnvConfig(envVarMapping.logging.includeTimestamp, "boolean"),
  };

  // Load NodeEnv
  envConfig.nodeEnv = getEnvConfig(envVarMapping.nodeEnv, "string");

  // Use Zod to validate and transform the config
  const result = ConfigSchema.safeParse({
    elasticsearch: { ...defaultConfig.elasticsearch, ...envConfig.elasticsearch },
    kafka: { ...defaultConfig.kafka, ...envConfig.kafka },
    extraction: { ...defaultConfig.extraction, ...envConfig.extraction },
    logging: { ...defaultConfig.logging, ...envConfig.logging },
    nodeEnv: envConfig.nodeEnv || defaultConfig.nodeEnv,
  });

  if (!result.success) {
    console.error("Configuration validation failed:", result.error.format());
    throw new Error("Invalid configuration: " + JSON.stringify(result.error.format(), null, 2));
  }

  return result.data;
}

export function validateEnvironment(): {
  valid: boolean;
  errors: string[];
  warnings?: string[];
} {
  const requiredVars = ["ELASTIC_NODE", "KAFKA_BROKERS"];
  const errors: string[] = [];
  const warnings: string[] = [];

  for (const varName of requiredVars) {
    if (!getEnvValue(varName)) {
      errors.push(`Missing required environment variable: ${varName}`);
    }
  }

  if (getEnvValue("ELASTIC_NODE")) {
    try {
      const url = new URL(getEnvValue("ELASTIC_NODE")!);
      if (!url.protocol.startsWith("http")) {
        errors.push("ELASTIC_NODE must use http or https protocol");
      }
    } catch (e) {
      errors.push("ELASTIC_NODE is not a valid URL format");
    }
  }

  if (getEnvValue("KAFKA_BROKERS")) {
    const brokers = getEnvValue("KAFKA_BROKERS")!.split(",");
    for (const broker of brokers) {
      const trimmed = broker.trim();
      if (!trimmed.includes(":")) {
        warnings.push(
          `Kafka broker "${trimmed}" should include port (e.g., host:port)`,
        );
      }
    }
  }

  const hasElasticApiKeyId = !!getEnvValue("ELASTIC_API_KEY_ID");
  const hasElasticApiKey = !!getEnvValue("ELASTIC_API_KEY");

  if (hasElasticApiKeyId && !hasElasticApiKey) {
    errors.push("ELASTIC_API_KEY_ID provided but ELASTIC_API_KEY is missing");
  }

  if (hasElasticApiKey && !hasElasticApiKeyId) {
    errors.push("ELASTIC_API_KEY provided but ELASTIC_API_KEY_ID is missing");
  }

  if (!hasElasticApiKeyId && !hasElasticApiKey) {
    warnings.push(
      "No Elasticsearch authentication configured. This may be fine for local development but should be set for production.",
    );
  }

  const kafkaSSL = getEnvValue("KAFKA_SSL")?.toLowerCase() === "true";
  const hasKafkaUsername = !!getEnvValue("KAFKA_USERNAME");
  const hasKafkaPassword = !!getEnvValue("KAFKA_PASSWORD");

  if (kafkaSSL && (!hasKafkaUsername || !hasKafkaPassword)) {
    warnings.push(
      "KAFKA_SSL is enabled but KAFKA_USERNAME or KAFKA_PASSWORD is missing. This may cause authentication issues.",
    );
  }

  const nodeEnv = getEnvValue("NODE_ENV");
  if (nodeEnv && !["development", "production", "test"].includes(nodeEnv)) {
    warnings.push(
      `NODE_ENV value "${nodeEnv}" is not standard. Expected: development, production, or test.`,
    );
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

let config: Config;

try {
  const envValidation = validateEnvironment();
  if (!envValidation.valid) {
    console.error(
      "❌ Environment validation failed:",
      envValidation.errors.join(", "),
    );
    if (envValidation.warnings && envValidation.warnings.length > 0) {
      console.warn(
        "⚠️ Environment warnings:",
        envValidation.warnings.join(", "),
      );
    }
    process.exit(1);
  }

  if (envValidation.warnings && envValidation.warnings.length > 0) {
    console.warn("⚠️ Environment warnings:", envValidation.warnings.join(", "));
  }

  const envConfig = loadConfigFromEnv();
  const mergedConfig = {
    elasticsearch: {
      ...defaultConfig.elasticsearch,
      ...envConfig.elasticsearch,
    },
    kafka: { ...defaultConfig.kafka, ...envConfig.kafka },
    extraction: { ...defaultConfig.extraction, ...envConfig.extraction },
    logging: { ...defaultConfig.logging, ...envConfig.logging },
    nodeEnv: envConfig.nodeEnv || defaultConfig.nodeEnv,
  };

  config = ConfigSchema.parse(mergedConfig);

  if (
    config.nodeEnv === "development" ||
    getEnvValue("LOG_CONFIG") === "true"
  ) {
    console.log(
      "✅ Configuration loaded successfully:",
      JSON.stringify(
        {
          elasticsearch: {
            node: config.elasticsearch.node,
            hasApiKey: !!config.elasticsearch.apiKey,
            maxRetries: config.elasticsearch.maxRetries,
            requestTimeout: config.elasticsearch.requestTimeout,
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
          logging: {
            level: config.logging.level,
            format: config.logging.format,
          },
          nodeEnv: config.nodeEnv,
        },
        null,
        2,
      ),
    );
  }
} catch (error) {
  console.error(
    "💥 Configuration validation failed:",
    error instanceof Error ? error.message : String(error),
  );
  throw new Error(
    "Invalid configuration: " +
      (error instanceof Error ? error.message : String(error)),
  );
}

export { config, envVarMapping, defaultConfig };

export function getConfigDocumentation(): Record<string, any> {
  return {
    environmentVariables: envVarMapping,
    defaults: defaultConfig,
    schemas: {
      elasticsearch: ElasticsearchConfigSchema.describe(
        "Elasticsearch connection configuration",
      ),
      kafka: KafkaConfigSchema.describe(
        "Kafka connection and producer configuration",
      ),
      extraction: ExtractionConfigSchema.describe(
        "Data extraction and processing configuration",
      ),
      logging: LoggingConfigSchema.describe("Logging configuration"),
    },
  };
}
