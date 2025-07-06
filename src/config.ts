/* src/config.ts */

import { z } from "zod";
import { err, log, warn } from "./utils/logger.js";

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
		brokers: z.array(z.string().min(1)).min(1).default(["localhost:9092"]),
		ssl: z.boolean().default(false),
		username: z.string().optional(),
		password: z.string().optional(),
		connectionTimeout: z.number().min(1000).max(30000).default(3000),
		authenticationTimeout: z.number().min(1000).max(30000).default(3000),
		requestTimeout: z.number().min(1000).max(60000).default(30000),
		initialRetryTime: z.number().min(100).max(10000).default(1000),
		retries: z.number().min(0).max(20).default(8),
		topicName: z.string().min(1).default("monitoring.raw.events"),
		maxMessageSize: z.number().min(1024).max(10485760).default(5242880),
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
	intervalMinutes: z.number().min(1).max(60).default(5),
	timeRange: z.string().default("now-5m"),
	maxResults: z.number().min(1).max(10000).default(1000),
	timeout: z.string().default("30s"),
	indexPattern: z.string().default("synthetics-*"),
	monitorNamePattern: z.string().default("*prd*"),
	batchProcessing: z
		.object({
			enabled: z.boolean(),
			batchSize: z.number().min(10).max(1000),
			maxConcurrency: z.number().min(1).max(10),
			streamingThreshold: z.number().min(100).max(10000),
			retryAttempts: z.number().min(0).max(5),
		})
		.default({
			enabled: true,
			batchSize: 100,
			maxConcurrency: 4,
			streamingThreshold: 500,
			retryAttempts: 2,
		}),
});

const LoggingConfigSchema = z.object({
	level: z.enum(["debug", "info", "warn", "error"]).default("info"),
	format: z.enum(["json", "text"]).default("json"),
	includeTimestamp: z.boolean().default(true),
	console: z
		.object({
			enabled: z.boolean(),
			level: z.enum(["debug", "info", "warn", "error"]).optional(),
		})
		.default({
			enabled: true,
		}),
	opentelemetry: z
		.object({
			level: z.enum(["debug", "info", "warn", "error"]).optional(),
		})
		.default({
			level: "info",
		}),
});

const MetricsConfigSchema = z.object({
	enabled: z.boolean().default(true),
	port: z.number().min(1000).max(65535).default(9090),
	prefix: z.string().default("synthetics_extractor_"),
	endpoint: z.string().default("/metrics"),
});

const ApiConfigSchema = z.object({
	enabled: z.boolean().default(true),
	invalidRecordsEndpoint: z.string().default("/api/invalid-records"),
	uiEndpoint: z.string().default("/ui"),
	rateLimit: z.object({
		enabled: z.boolean().default(false),
		windowMs: z.number().min(1000).max(300000).default(60000), // 1 minute
		maxRequests: z.number().min(1).max(10000).default(100), // requests per window
		skipSuccessfulRequests: z.boolean().default(false),
		skipFailedRequests: z.boolean().default(false),
	}),
});

const OpenTelemetryConfigSchema = z.object({
	enabled: z.boolean().default(false),
	tracesEndpoint: z.string().default("http://localhost:4318/v1/traces"),
	metricsEndpoint: z.string().default("http://localhost:4318/v1/metrics"),
	logsEndpoint: z.string().default("http://localhost:4318/v1/logs"),
	serviceName: z.string().default("synthetics-monitor-extractor"),
	serviceVersion: z.string().default("1.0.0"),
	deploymentEnvironment: z.string().default("development"),
	metricIntervalMs: z.number().min(1000).max(60000).default(15000),
	metricReaderInterval: z.number().default(120000), // 2-minute interval to reduce load on endpoints
	summaryLogInterval: z.number().default(300000),
});

const ConfigSchema = z.object({
	elasticsearch: ElasticsearchConfigSchema,
	kafka: KafkaConfigSchema,
	extraction: ExtractionConfigSchema,
	logging: LoggingConfigSchema,
	metrics: MetricsConfigSchema,
	api: ApiConfigSchema,
	openTelemetry: OpenTelemetryConfigSchema,
	nodeEnv: z
		.enum(["development", "production", "staging"])
		.default("development"),
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
		topicName: "monitoring.raw.events",
		maxMessageSize: 5242880,
	},
	extraction: {
		intervalMinutes: 5,
		timeRange: "now-5m",
		maxResults: 1000,
		timeout: "30s",
		indexPattern: "synthetics-*",
		monitorNamePattern: "*prd*",
		batchProcessing: {
			enabled: true,
			batchSize: 100,
			maxConcurrency: 4,
			streamingThreshold: 500,
			retryAttempts: 2,
		},
	},
	logging: {
		level: "info",
		format: "json",
		includeTimestamp: true,
		console: {
			enabled: true,
		},
		opentelemetry: {
			level: "info",
		},
	},
	metrics: {
		enabled: true,
		port: 9090,
		prefix: "synthetics_extractor_",
		endpoint: "/metrics",
	},
	api: {
		enabled: true,
		invalidRecordsEndpoint: "/api/invalid-records",
		uiEndpoint: "/ui",
		rateLimit: {
			enabled: false,
			windowMs: 60000,
			maxRequests: 100,
			skipSuccessfulRequests: false,
			skipFailedRequests: false,
		},
	},
	openTelemetry: {
		enabled: false,
		tracesEndpoint: "http://localhost:4318/v1/traces",
		metricsEndpoint: "http://localhost:4318/v1/metrics",
		logsEndpoint: "http://localhost:4318/v1/logs",
		serviceName: "synthetics-monitor-extractor",
		serviceVersion: "1.0.0",
		deploymentEnvironment: "development",
		metricIntervalMs: 15000,
		metricReaderInterval: 120000,
		summaryLogInterval: 300000,
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
		topicName: "KAFKA_TOPIC_NAME",
		maxMessageSize: "KAFKA_MAX_MESSAGE_SIZE",
	},
	extraction: {
		intervalMinutes: "EXTRACTION_INTERVAL_MINUTES",
		timeRange: "EXTRACTION_TIME_RANGE",
		maxResults: "EXTRACTION_MAX_RESULTS",
		timeout: "EXTRACTION_TIMEOUT",
		indexPattern: "EXTRACTION_INDEX_PATTERN",
		monitorNamePattern: "EXTRACTION_MONITOR_NAME_PATTERN",
		batchProcessing: {
			enabled: "EXTRACTION_BATCH_PROCESSING_ENABLED",
			batchSize: "EXTRACTION_BATCH_SIZE",
			maxConcurrency: "EXTRACTION_MAX_CONCURRENCY",
			streamingThreshold: "EXTRACTION_STREAMING_THRESHOLD",
			retryAttempts: "EXTRACTION_RETRY_ATTEMPTS",
		},
	},
	logging: {
		level: "LOG_LEVEL",
		format: "LOG_FORMAT",
		includeTimestamp: "LOG_INCLUDE_TIMESTAMP",
		console: {
			enabled: "LOG_CONSOLE_ENABLED",
			level: "LOG_CONSOLE_LEVEL",
		},
		opentelemetry: {
			level: "LOG_OPENTELEMETRY_LEVEL",
		},
	},
	metrics: {
		enabled: "METRICS_ENABLED",
		port: "METRICS_PORT",
		prefix: "METRICS_PREFIX",
		endpoint: "METRICS_ENDPOINT",
	},
	api: {
		enabled: "API_ENABLED",
		invalidRecordsEndpoint: "API_INVALID_RECORDS_ENDPOINT",
		uiEndpoint: "API_UI_ENDPOINT",
		rateLimit: {
			enabled: "API_RATE_LIMIT_ENABLED",
			windowMs: "API_RATE_LIMIT_WINDOW_MS",
			maxRequests: "API_RATE_LIMIT_MAX_REQUESTS",
			skipSuccessfulRequests: "API_RATE_LIMIT_SKIP_SUCCESSFUL_REQUESTS",
			skipFailedRequests: "API_RATE_LIMIT_SKIP_FAILED_REQUESTS",
		},
	},
	openTelemetry: {
		enabled: "OPEN_TELEMETRY_ENABLED",
		tracesEndpoint: "OPEN_TELEMETRY_TRACES_ENDPOINT",
		metricsEndpoint: "OPEN_TELEMETRY_METRICS_ENDPOINT",
		logsEndpoint: "OPEN_TELEMETRY_LOGS_ENDPOINT",
		serviceName: "OPEN_TELEMETRY_SERVICE_NAME",
		serviceVersion: "OPEN_TELEMETRY_SERVICE_VERSION",
		deploymentEnvironment: "OPEN_TELEMETRY_DEPLOYMENT_ENVIRONMENT",
		metricIntervalMs: "OPEN_TELEMETRY_METRIC_INTERVAL_MS",
		metricReaderInterval: "OPEN_TELEMETRY_METRIC_READER_INTERVAL",
		summaryLogInterval: "OPEN_TELEMETRY_SUMMARY_LOG_INTERVAL",
	},
	nodeEnv: "NODE_ENV",
} as const;

function parseEnvVar(
	value: string | undefined,
	type: "string" | "number" | "boolean" | "array",
): unknown {
	if (value === undefined || value === "") return undefined;
	if (type === "number") {
		const num = Number(value);
		return isNaN(num) ? undefined : num;
	}
	if (type === "boolean") return value.toLowerCase() === "true";
	if (type === "array") {
		const arr = value
			.split(",")
			.map((s) => s.trim())
			.filter((s) => s.length > 0);
		return arr.length > 0 ? arr : undefined;
	}
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
	const getEnvConfig = (
		key: string,
		type: "string" | "number" | "boolean" | "array",
	) => {
		const value = getEnvValue(key);
		if (value === undefined) return undefined;
		return parseEnvVar(value, type);
	};

	// Helper function to filter out undefined values
	const filterUndefined = (obj: Record<string, any>) => {
		const filtered: Record<string, any> = {};
		for (const [key, value] of Object.entries(obj)) {
			if (value !== undefined) {
				filtered[key] = value;
			}
		}
		return filtered;
	};

	// Load Elasticsearch config
	envConfig.elasticsearch = filterUndefined({
		node: getEnvConfig(envVarMapping.elasticsearch.node, "string"),
		apiKeyId: getEnvConfig(envVarMapping.elasticsearch.apiKeyId, "string"),
		apiKey: getEnvConfig(envVarMapping.elasticsearch.apiKey, "string"),
		maxRetries: getEnvConfig(envVarMapping.elasticsearch.maxRetries, "number"),
		requestTimeout: getEnvConfig(
			envVarMapping.elasticsearch.requestTimeout,
			"number",
		),
		compression: getEnvConfig(
			envVarMapping.elasticsearch.compression,
			"boolean",
		),
		sniffOnStart: getEnvConfig(
			envVarMapping.elasticsearch.sniffOnStart,
			"boolean",
		),
		rejectUnauthorized: getEnvConfig(
			envVarMapping.elasticsearch.rejectUnauthorized,
			"boolean",
		),
		name: getEnvConfig(envVarMapping.elasticsearch.name, "string"),
		opaqueIdPrefix: getEnvConfig(
			envVarMapping.elasticsearch.opaqueIdPrefix,
			"string",
		),
	});

	// Load Kafka config
	envConfig.kafka = filterUndefined({
		clientId: getEnvConfig(envVarMapping.kafka.clientId, "string"),
		brokers: getEnvConfig(envVarMapping.kafka.brokers, "array"),
		ssl: getEnvConfig(envVarMapping.kafka.ssl, "boolean"),
		username: getEnvConfig(envVarMapping.kafka.username, "string"),
		password: getEnvConfig(envVarMapping.kafka.password, "string"),
		connectionTimeout: getEnvConfig(
			envVarMapping.kafka.connectionTimeout,
			"number",
		),
		authenticationTimeout: getEnvConfig(
			envVarMapping.kafka.authenticationTimeout,
			"number",
		),
		requestTimeout: getEnvConfig(envVarMapping.kafka.requestTimeout, "number"),
		initialRetryTime: getEnvConfig(
			envVarMapping.kafka.initialRetryTime,
			"number",
		),
		retries: getEnvConfig(envVarMapping.kafka.retries, "number"),
		topicName: getEnvConfig(envVarMapping.kafka.topicName, "string"),
		maxMessageSize: getEnvConfig(envVarMapping.kafka.maxMessageSize, "number"),
	});

	// Load Extraction config
	envConfig.extraction = filterUndefined({
		intervalMinutes: getEnvConfig(
			envVarMapping.extraction.intervalMinutes,
			"number",
		),
		timeRange: getEnvConfig(envVarMapping.extraction.timeRange, "string"),
		maxResults: getEnvConfig(envVarMapping.extraction.maxResults, "number"),
		timeout: getEnvConfig(envVarMapping.extraction.timeout, "string"),
		indexPattern: getEnvConfig(envVarMapping.extraction.indexPattern, "string"),
		monitorNamePattern: getEnvConfig(
			envVarMapping.extraction.monitorNamePattern,
			"string",
		),
		...(() => {
			const batchConfig = filterUndefined({
				enabled: getEnvConfig(
					envVarMapping.extraction.batchProcessing.enabled,
					"boolean",
				),
				batchSize: getEnvConfig(
					envVarMapping.extraction.batchProcessing.batchSize,
					"number",
				),
				maxConcurrency: getEnvConfig(
					envVarMapping.extraction.batchProcessing.maxConcurrency,
					"number",
				),
				streamingThreshold: getEnvConfig(
					envVarMapping.extraction.batchProcessing.streamingThreshold,
					"number",
				),
				retryAttempts: getEnvConfig(
					envVarMapping.extraction.batchProcessing.retryAttempts,
					"number",
				),
			});
			return Object.keys(batchConfig).length > 0
				? { batchProcessing: batchConfig }
				: {};
		})(),
	});

	// Load Logging config
	envConfig.logging = filterUndefined({
		level: getEnvConfig(envVarMapping.logging.level, "string"),
		format: getEnvConfig(envVarMapping.logging.format, "string"),
		includeTimestamp: getEnvConfig(
			envVarMapping.logging.includeTimestamp,
			"boolean",
		),
		...(() => {
			const consoleConfig = filterUndefined({
				enabled: getEnvConfig(envVarMapping.logging.console.enabled, "boolean"),
				level: getEnvConfig(envVarMapping.logging.console.level, "string"),
			});
			return Object.keys(consoleConfig).length > 0
				? { console: consoleConfig }
				: {};
		})(),
		...(() => {
			const otelConfig = filterUndefined({
				level: getEnvConfig(
					envVarMapping.logging.opentelemetry.level,
					"string",
				),
			});
			return Object.keys(otelConfig).length > 0
				? { opentelemetry: otelConfig }
				: {};
		})(),
	});

	envConfig.metrics = filterUndefined({
		enabled: getEnvConfig(envVarMapping.metrics.enabled, "boolean"),
		port: getEnvConfig(envVarMapping.metrics.port, "number"),
		prefix: getEnvConfig(envVarMapping.metrics.prefix, "string"),
		endpoint: getEnvConfig(envVarMapping.metrics.endpoint, "string"),
	});

	// Load API config
	envConfig.api = filterUndefined({
		enabled: getEnvConfig(envVarMapping.api.enabled, "boolean"),
		invalidRecordsEndpoint: getEnvConfig(
			envVarMapping.api.invalidRecordsEndpoint,
			"string",
		),
		uiEndpoint: getEnvConfig(envVarMapping.api.uiEndpoint, "string"),
		...(() => {
			const rateLimitConfig = filterUndefined({
				enabled: getEnvConfig(envVarMapping.api.rateLimit.enabled, "boolean"),
				windowMs: getEnvConfig(envVarMapping.api.rateLimit.windowMs, "number"),
				maxRequests: getEnvConfig(
					envVarMapping.api.rateLimit.maxRequests,
					"number",
				),
				skipSuccessfulRequests: getEnvConfig(
					envVarMapping.api.rateLimit.skipSuccessfulRequests,
					"boolean",
				),
				skipFailedRequests: getEnvConfig(
					envVarMapping.api.rateLimit.skipFailedRequests,
					"boolean",
				),
			});
			return Object.keys(rateLimitConfig).length > 0
				? { rateLimit: rateLimitConfig }
				: {};
		})(),
	});

	// Load OpenTelemetry config
	envConfig.openTelemetry = filterUndefined({
		enabled: getEnvConfig(envVarMapping.openTelemetry.enabled, "boolean"),
		tracesEndpoint: getEnvConfig(
			envVarMapping.openTelemetry.tracesEndpoint,
			"string",
		),
		metricsEndpoint: getEnvConfig(
			envVarMapping.openTelemetry.metricsEndpoint,
			"string",
		),
		logsEndpoint: getEnvConfig(
			envVarMapping.openTelemetry.logsEndpoint,
			"string",
		),
		serviceName: getEnvConfig(
			envVarMapping.openTelemetry.serviceName,
			"string",
		),
		serviceVersion: getEnvConfig(
			envVarMapping.openTelemetry.serviceVersion,
			"string",
		),
		deploymentEnvironment: getEnvConfig(
			envVarMapping.openTelemetry.deploymentEnvironment,
			"string",
		),
		metricIntervalMs: getEnvConfig(
			envVarMapping.openTelemetry.metricIntervalMs,
			"number",
		),
		metricReaderInterval: getEnvConfig(
			envVarMapping.openTelemetry.metricReaderInterval,
			"number",
		),
		summaryLogInterval: getEnvConfig(
			envVarMapping.openTelemetry.summaryLogInterval,
			"number",
		),
	});

	// Load NodeEnv
	envConfig.nodeEnv = getEnvConfig(envVarMapping.nodeEnv, "string");

	// Use Zod to validate and transform the config - let schema defaults handle missing nested objects
	const result = ConfigSchema.safeParse({
		elasticsearch: {
			...defaultConfig.elasticsearch,
			...envConfig.elasticsearch,
		},
		kafka: { ...defaultConfig.kafka, ...envConfig.kafka },
		extraction: { ...defaultConfig.extraction, ...envConfig.extraction },
		logging: { ...defaultConfig.logging, ...envConfig.logging },
		metrics: { ...defaultConfig.metrics, ...envConfig.metrics },
		api: { ...defaultConfig.api, ...envConfig.api },
		openTelemetry: {
			...defaultConfig.openTelemetry,
			...envConfig.openTelemetry,
		},
		nodeEnv: envConfig.nodeEnv || defaultConfig.nodeEnv,
	});

	if (!result.success) {
		err(`❌ Configuration validation failed: ${result.error.format()}`);
		throw new Error(
			"Invalid configuration: " +
				JSON.stringify(result.error.format(), null, 2),
		);
	}

	return result.data;
}

export function validateEnvironment(): {
	valid: boolean;
	errors: string[];
	warnings?: string[];
} {
	const errors: string[] = [];
	const warnings: string[] = [];

	// These variables have defaults in the schema, so they're not strictly required
	// We'll just warn if they're not set
	if (!getEnvValue("ELASTIC_NODE")) {
		warnings.push(
			"ELASTIC_NODE not set, using default: https://elasticsearch:9200",
		);
	}

	if (!getEnvValue("KAFKA_BROKERS")) {
		warnings.push("KAFKA_BROKERS not set, using default: localhost:9092");
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
		err(`❌ Environment validation failed: ${envValidation.errors.join(", ")}`);
		if (envValidation.warnings && envValidation.warnings.length > 0) {
			warn(`⚠️ Environment warnings: ${envValidation.warnings.join(", ")}`);
		}
		process.exit(1);
	}

	if (envValidation.warnings && envValidation.warnings.length > 0) {
		warn(`⚠️ Environment warnings: ${envValidation.warnings.join(", ")}`);
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
		metrics: { ...defaultConfig.metrics, ...envConfig.metrics },
		api: { ...defaultConfig.api, ...envConfig.api },
		openTelemetry: {
			...defaultConfig.openTelemetry,
			...envConfig.openTelemetry,
		},
		nodeEnv: envConfig.nodeEnv || defaultConfig.nodeEnv,
	};

	config = ConfigSchema.parse(mergedConfig);

	if (
		config.nodeEnv === "development" ||
		getEnvValue("LOG_CONFIG") === "true"
	) {
		log(
			`✅ Configuration loaded successfully: ${JSON.stringify({
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
					topicName: config.kafka.topicName,
				},
				extraction: {
					intervalMinutes: config.extraction.intervalMinutes,
					timeRange: config.extraction.timeRange,
					indexPattern: config.extraction.indexPattern,
				},
				logging: {
					level: config.logging.level,
					format: config.logging.format,
					console: {
						enabled: config.logging.console.enabled,
						level: config.logging.console.level,
					},
					opentelemetry: {
						enabled: config.openTelemetry.enabled, // Uses global OPEN_TELEMETRY_ENABLED
						level: config.logging.opentelemetry.level,
					},
				},
				metrics: {
					enabled: config.metrics.enabled,
					port: config.metrics.port,
					prefix: config.metrics.prefix,
				},
				api: {
					enabled: config.api.enabled,
					invalidRecordsEndpoint: config.api.invalidRecordsEndpoint,
					uiEndpoint: config.api.uiEndpoint,
				},
				openTelemetry: {
					enabled: config.openTelemetry.enabled,
					tracesEndpoint: config.openTelemetry.tracesEndpoint,
					metricsEndpoint: config.openTelemetry.metricsEndpoint,
					logsEndpoint: config.openTelemetry.logsEndpoint,
					serviceName: config.openTelemetry.serviceName,
					metricIntervalMs: config.openTelemetry.metricIntervalMs,
				},
				nodeEnv: config.nodeEnv,
			})}`,
		);
	}
} catch (error) {
	err(
		`💥 Configuration validation failed: ${error instanceof Error ? error.message : String(error)}`,
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
			metrics: MetricsConfigSchema.describe("Metrics configuration"),
			api: ApiConfigSchema.describe("API and UI configuration"),
			openTelemetry: OpenTelemetryConfigSchema.describe(
				"OpenTelemetry configuration",
			),
		},
	};
}
