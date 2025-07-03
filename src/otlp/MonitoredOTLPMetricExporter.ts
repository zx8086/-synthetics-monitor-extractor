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
    log(`DEBUG: Creating OTLPMetricExporter with timeout ${timeoutMillis}ms for ${exporterConfig.url}`);
    this.otlpExporter = new OTLPMetricExporter({
      ...exporterConfig,
      timeoutMillis: timeoutMillis,
    });
    log(`DEBUG: OTLPMetricExporter created successfully`);
  }

  async export(
    metrics: ResourceMetrics,
    resultCallback: (result: ExportResult) => void,
  ): Promise<void> {
    const startTime = Date.now();
    this.totalExports++;

    try {
      log(`DEBUG: Starting metrics export to ${this.url} with timeout ${this.timeoutMillis}ms`);
      await this.checkNetworkConnectivity();
      this.logSystemResources();

      this.otlpExporter.export(metrics, (result) => {
        const duration = Date.now() - startTime;
        
        if (result.code === 0) {
          this.successfulExports++;
          const metricCount = metrics.scopeMetrics?.reduce((acc, scope) => 
            acc + (scope.metrics?.length || 0), 0) || 0;
          this.logSuccess(metricCount, duration);
          log(`DEBUG: Metrics export SUCCESS in ${duration}ms`);
        } else {
          this.logDetailedFailure(result.error, 1, duration);
          log(`DEBUG: Metrics export FAILED in ${duration}ms - Error:`, result.error?.message || result.error);
        }
        
        this.logExportDuration(startTime);
        resultCallback(result);
      });
    } catch (error) {
      const duration = Date.now() - startTime;
      this.logDetailedFailure(error, 1, duration);
      log(`DEBUG: Metrics export EXCEPTION in ${duration}ms - Error:`, error instanceof Error ? error.message : error);
      resultCallback({ code: 1, error: error instanceof Error ? error : new Error(String(error)) });
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
