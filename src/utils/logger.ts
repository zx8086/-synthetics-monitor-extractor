/* src/utils/logger.ts */

import winston from "winston";
import { ecsFormat } from "@elastic/ecs-winston-format";
import { trace, context, type SpanContext } from "@opentelemetry/api";
import TransportStream from "winston-transport";

// Do NOT import config at the top to avoid circular dependency

let loggerInstance: winston.Logger | null = null;

function getLogger(): winston.Logger {
	if (loggerInstance) return loggerInstance;

	// Import config at runtime to break circular dependency
	let config: any = null;
	try {
		// eslint-disable-next-line @typescript-eslint/no-var-requires
		const configModule = require("../config.js");
		config = configModule.config;
	} catch (error) {
		config = null;
	}

	class OpenTelemetryHttpTransport extends TransportStream {
		constructor(options: any) {
			super(options);
		}

		override async log(info: any, callback: () => void) {
			if (config?.openTelemetry?.enabled) {
				try {
					// Debug: Log the actual endpoint being used
					// eslint-disable-next-line no-console
					// console.log(`OpenTelemetry endpoint: ${config.openTelemetry.logsEndpoint}`);

					const logData = {
						resourceLogs: [
							{
								resource: {
									attributes: [
										{
											key: "service.name",
											value: {
												stringValue:
													config?.openTelemetry?.serviceName ||
													"synthetics-monitor-extractor",
											},
										},
									],
								},
								scopeLogs: [
									{
										scope: {
											name: "synthetics-monitor-extractor",
										},
										logRecords: [
											{
												timeUnixNano: Date.now() * 1000000,
												severityNumber: this.getSeverityNumber(info.level),
												severityText: info.level.toUpperCase(),
												body: {
													stringValue: info.message,
												},
												attributes: [
													{
														key: "log.level",
														value: { stringValue: info.level },
													},
													{
														key: "timestamp",
														value: { stringValue: info["@timestamp"] },
													},
												],
											},
										],
									},
								],
							},
						],
					};

					const response = await fetch(
						config?.openTelemetry?.logsEndpoint ||
							"http://localhost:4318/v1/logs",
						{
							method: "POST",
							headers: {
								"Content-Type": "application/json",
							},
							body: JSON.stringify(logData),
						},
					);

					if (!response.ok) {
						// eslint-disable-next-line no-console
						console.error(
							`OpenTelemetry HTTP error: ${response.status} ${response.statusText}`,
						);
					}
				} catch (error) {
					// eslint-disable-next-line no-console
					console.error(
						`Failed to send log to OpenTelemetry (endpoint: ${config?.openTelemetry?.logsEndpoint || "http://localhost:4318/v1/logs"}):`,
						error,
					);
				}
			}
			callback();
		}

		private getSeverityNumber(level: string): number {
			switch (level.toLowerCase()) {
				case "error":
					return 17;
				case "warn":
					return 13;
				case "info":
					return 9;
				case "debug":
					return 5;
				default:
					return 9;
			}
		}
	}

	const customFormat = winston.format.combine(
		ecsFormat({ convertReqRes: true, apmIntegration: true }),
		winston.format((info) => {
			const {
				"@timestamp": timestamp,
				"ecs.version": ecsVersion,
				level,
				message,
				meta,
				...rest
			} = info;
			// Only flatten meta fields to top-level, do NOT stringify into message
			let flattened = {
				"@timestamp": timestamp,
				"ecs.version": ecsVersion,
				level,
				log: { level },
				message,
				...rest,
			};
			if (meta && typeof meta === "object") {
				flattened = { ...flattened, ...meta };
			}
			// Fix for span property
			const traceObj = info["trace"] as
				| { id?: string; span?: { id?: string } }
				| undefined;
			if (traceObj) {
				(flattened as any).trace = {
					id: traceObj.id || "",
					span: { id: traceObj.span?.id || "" },
				};
			}
			return flattened;
		})(),
	);

	const transports: TransportStream[] = [];

	// Always add console transport
	transports.push(
		new winston.transports.Console({
			level: config?.logging?.level || "info",
			format:
				config?.logging?.format === "json"
					? winston.format.json()
					: winston.format.combine(
							winston.format.colorize(),
							winston.format.simple(),
						),
		}),
	);

	// Add OpenTelemetry transport if enabled
	if (config?.openTelemetry?.enabled) {
		try {
			const otelTransport = new OpenTelemetryHttpTransport({
				level: config.logging.level,
			});
			transports.push(otelTransport);
		} catch (error) {
			// eslint-disable-next-line no-console
			console.error("Failed to create OpenTelemetry transport:", error);
		}
	}

	loggerInstance = winston.createLogger({
		level: config?.logging?.level || "info",
		format: customFormat,
		transports,
	});

	return loggerInstance;
}

export function log(message: string, meta?: any): void {
	const ctx = context.active();
	const span = trace.getSpan(ctx);
	const spanContext: SpanContext | undefined = span?.spanContext();

	const logData = {
		message,
		...(meta && { meta }),
		...(spanContext && {
			trace: {
				id: spanContext.traceId,
				span: { id: spanContext.spanId },
			},
		}),
	};

	getLogger().info(logData);
}

export function err(message: string, meta?: any): void {
	const ctx = context.active();
	const span = trace.getSpan(ctx);
	const spanContext: SpanContext | undefined = span?.spanContext();

	const logData = {
		message,
		...(meta && { meta }),
		...(spanContext && {
			trace: {
				id: spanContext.traceId,
				span: { id: spanContext.spanId },
			},
		}),
	};

	getLogger().error(logData);
}

export function warn(message: string, meta?: any): void {
	const ctx = context.active();
	const span = trace.getSpan(ctx);
	const spanContext: SpanContext | undefined = span?.spanContext();

	const logData = {
		message,
		...(meta && { meta }),
		...(spanContext && {
			trace: {
				id: spanContext.traceId,
				span: { id: spanContext.spanId },
			},
		}),
	};

	getLogger().warn(logData);
}

export function debug(message: string, meta?: any): void {
	const ctx = context.active();
	const span = trace.getSpan(ctx);
	const spanContext: SpanContext | undefined = span?.spanContext();

	const logData = {
		message,
		...(meta && { meta }),
		...(spanContext && {
			trace: {
				id: spanContext.traceId,
				span: { id: spanContext.spanId },
			},
		}),
	};

	getLogger().debug(logData);
}
