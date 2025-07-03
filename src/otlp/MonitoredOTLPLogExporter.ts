/* src/otlp/MonitoredOTLPLogExporter.ts */

import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-http";
import type { ReadableLogRecord } from "@opentelemetry/sdk-logs";
import type { OTLPExporterNodeConfigBase } from "@opentelemetry/otlp-exporter-base";
import type { ExportResult } from "@opentelemetry/core";
import { MonitoredOTLPExporter } from "./MonitoredOTLPExporter.js";
import { log, err } from "../utils/logger.js";

export class MonitoredOTLPLogExporter extends MonitoredOTLPExporter<ReadableLogRecord[]> {
  protected readonly exporterType = "Logs";
  private readonly otlpExporter: OTLPLogExporter;

  constructor(
    exporterConfig: OTLPExporterNodeConfigBase,
    timeoutMillis: number = 10000,
  ) {
    super(exporterConfig, exporterConfig.url || "", timeoutMillis);
    this.otlpExporter = new OTLPLogExporter(exporterConfig);
  }

  async export(
    logs: ReadableLogRecord[],
    resultCallback: (result: ExportResult) => void,
  ): Promise<void> {
    const startTime = Date.now();
    this.totalExports++;

    try {
      await this.checkNetworkConnectivity();
      this.logSystemResources();

      this.otlpExporter.export(logs, (result) => {
        const duration = Date.now() - startTime;
        
        if (result.code === 0) {
          this.successfulExports++;
          this.logSuccess(logs.length, duration);
        } else {
          this.logDetailedFailure(result.error, logs.length, duration);
        }
        
        this.logExportDuration(startTime);
        resultCallback(result);
      });
    } catch (error) {
      const duration = Date.now() - startTime;
      this.logDetailedFailure(error, logs.length, duration);
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
