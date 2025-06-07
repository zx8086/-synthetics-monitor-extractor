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
  nodeEnv: z.enum(["development", "production", "test"]).default("development"),
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
  const config: Partial<Config> = {};

  config.elasticsearch = {
    node:
      (parseEnvVar(
        getEnvValue(envVarMapping.elasticsearch.node),
        "string",
      ) as string) || defaultConfig.elasticsearch.node,
    apiKeyId: parseEnvVar(
      getEnvValue(envVarMapping.elasticsearch.apiKeyId),
      "string",
    ) as string,
    apiKey: parseEnvVar(
      getEnvValue(envVarMapping.elasticsearch.apiKey),
      "string",
    ) as string,
    maxRetries:
      (parseEnvVar(
        getEnvValue(envVarMapping.elasticsearch.maxRetries),
        "number",
      ) as number) || defaultConfig.elasticsearch.maxRetries,
    requestTimeout:
      (parseEnvVar(
        getEnvValue(envVarMapping.elasticsearch.requestTimeout),
        "number",
      ) as number) || defaultConfig.elasticsearch.requestTimeout,
    compression:
      (parseEnvVar(
        getEnvValue(envVarMapping.elasticsearch.compression),
        "boolean",
      ) as boolean) ?? defaultConfig.elasticsearch.compression,
    sniffOnStart:
      (parseEnvVar(
        getEnvValue(envVarMapping.elasticsearch.sniffOnStart),
        "boolean",
      ) as boolean) ?? defaultConfig.elasticsearch.sniffOnStart,
    rejectUnauthorized:
      (parseEnvVar(
        getEnvValue(envVarMapping.elasticsearch.rejectUnauthorized),
        "boolean",
      ) as boolean) ?? defaultConfig.elasticsearch.rejectUnauthorized,
  };

  const kafkaBrokers =
    (parseEnvVar(
      getEnvValue(envVarMapping.kafka.brokers),
      "array",
    ) as string[]) || defaultConfig.kafka.brokers;
  config.kafka = {
    clientId:
      (parseEnvVar(
        getEnvValue(envVarMapping.kafka.clientId),
        "string",
      ) as string) || defaultConfig.kafka.clientId,
    brokers: kafkaBrokers,
    ssl:
      (parseEnvVar(
        getEnvValue(envVarMapping.kafka.ssl),
        "boolean",
      ) as boolean) ?? defaultConfig.kafka.ssl,
    username: parseEnvVar(
      getEnvValue(envVarMapping.kafka.username),
      "string",
    ) as string,
    password: parseEnvVar(
      getEnvValue(envVarMapping.kafka.password),
      "string",
    ) as string,
    connectionTimeout:
      (parseEnvVar(
        getEnvValue(envVarMapping.kafka.connectionTimeout),
        "number",
      ) as number) || defaultConfig.kafka.connectionTimeout,
    authenticationTimeout:
      (parseEnvVar(
        getEnvValue(envVarMapping.kafka.authenticationTimeout),
        "number",
      ) as number) || defaultConfig.kafka.authenticationTimeout,
    requestTimeout:
      (parseEnvVar(
        getEnvValue(envVarMapping.kafka.requestTimeout),
        "number",
      ) as number) || defaultConfig.kafka.requestTimeout,
    initialRetryTime:
      (parseEnvVar(
        getEnvValue(envVarMapping.kafka.initialRetryTime),
        "number",
      ) as number) || defaultConfig.kafka.initialRetryTime,
    retries:
      (parseEnvVar(
        getEnvValue(envVarMapping.kafka.retries),
        "number",
      ) as number) || defaultConfig.kafka.retries,
  };

  config.extraction = {
    intervalMinutes:
      (parseEnvVar(
        getEnvValue(envVarMapping.extraction.intervalMinutes),
        "number",
      ) as number) || defaultConfig.extraction.intervalMinutes,
    timeRange:
      (parseEnvVar(
        getEnvValue(envVarMapping.extraction.timeRange),
        "string",
      ) as string) || defaultConfig.extraction.timeRange,
    maxResults:
      (parseEnvVar(
        getEnvValue(envVarMapping.extraction.maxResults),
        "number",
      ) as number) || defaultConfig.extraction.maxResults,
    timeout:
      (parseEnvVar(
        getEnvValue(envVarMapping.extraction.timeout),
        "string",
      ) as string) || defaultConfig.extraction.timeout,
    indexPattern:
      (parseEnvVar(
        getEnvValue(envVarMapping.extraction.indexPattern),
        "string",
      ) as string) || defaultConfig.extraction.indexPattern,
  };

  config.logging = {
    level:
      (parseEnvVar(getEnvValue(envVarMapping.logging.level), "string") as
        | "debug"
        | "info"
        | "warn"
        | "error") || defaultConfig.logging.level,
    format:
      (parseEnvVar(getEnvValue(envVarMapping.logging.format), "string") as
        | "json"
        | "text") || defaultConfig.logging.format,
    includeTimestamp:
      (parseEnvVar(
        getEnvValue(envVarMapping.logging.includeTimestamp),
        "boolean",
      ) as boolean) ?? defaultConfig.logging.includeTimestamp,
  };

  config.nodeEnv =
    (parseEnvVar(getEnvValue(envVarMapping.nodeEnv), "string") as
      | "development"
      | "production"
      | "test") || defaultConfig.nodeEnv;

  return config;
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
