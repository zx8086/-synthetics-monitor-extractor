/* src/otlp/MonitoredOTLPMetricExporter.ts */

import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-http";
import type { ResourceMetrics } from "@opentelemetry/sdk-metrics";
import type { OTLPExporterNodeConfigBase } from "@opentelemetry/otlp-exporter-base";
import type { ExportResult } from "@opentelemetry/core";
import { MonitoredOTLPExporter } from "./MonitoredOTLPExporter.js";
import { log, err } from "../utils/logger.js";

export class MonitoredOTLPMetricExporter extends MonitoredOTLPExporter<ResourceMetrics> {
	protected readonly exporterType = "Metrics";
	private readonly otlpExporter: OTLPMetricExporter;

	constructor(
		exporterConfig: OTLPExporterNodeConfigBase,
		timeoutMillis: number = 10000,
	) {
		super(exporterConfig, exporterConfig.url || "", timeoutMillis);
		this.otlpExporter = new OTLPMetricExporter({
			...exporterConfig,
			timeoutMillis: timeoutMillis,
		});
	}

	async export(
		metrics: ResourceMetrics,
		resultCallback: (result: ExportResult) => void,
	): Promise<void> {
		const startTime = Date.now();
		this.totalExports++;

		try {
			await this.checkNetworkConnectivity();
			this.logSystemResources();

			const exportPromise = new Promise<ExportResult>((resolve) => {
				this.otlpExporter.export(metrics, (result) => {
					const duration = Date.now() - startTime;
					resolve(result);
				});
			});

			const timeoutPromise = new Promise<ExportResult>((_, reject) => {
				setTimeout(() => {
					reject(
						new Error(`Metrics export timeout after ${this.timeoutMillis}ms`),
					);
				}, this.timeoutMillis);
			});

			const result = await Promise.race([exportPromise, timeoutPromise]);
			const duration = Date.now() - startTime;

			if (result.code === 0) {
				this.successfulExports++;
				const metricCount =
					metrics.scopeMetrics?.reduce(
						(acc, scope) => acc + (scope.metrics?.length || 0),
						0,
					) || 0;
				this.logSuccess(metricCount, duration);
			} else {
				this.logDetailedFailure(result.error, 1, duration);
			}

			this.logExportDuration(startTime);
			resultCallback(result);
		} catch (error) {
			const duration = Date.now() - startTime;
			this.logDetailedFailure(error, 1, duration);
			resultCallback({
				code: 1,
				error: error instanceof Error ? error : new Error(String(error)),
			});
		}
	}

	async forceFlush(): Promise<void> {
		return this.otlpExporter.forceFlush();
	}

	async shutdown(): Promise<void> {
		await this.baseShutdown();
		return this.otlpExporter.shutdown();
	}
}
