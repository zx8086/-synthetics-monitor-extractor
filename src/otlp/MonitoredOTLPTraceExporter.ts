/* src/otlp/MonitoredOTLPTraceExporter.ts */

import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import type { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import type { OTLPExporterNodeConfigBase } from "@opentelemetry/otlp-exporter-base";
import type { ExportResult } from "@opentelemetry/core";
import { MonitoredOTLPExporter } from "./MonitoredOTLPExporter.js";
import { log, err } from "../utils/logger.js";

export class MonitoredOTLPTraceExporter extends MonitoredOTLPExporter<ReadableSpan[]> {
  protected readonly exporterType = "Traces";
  private readonly otlpExporter: OTLPTraceExporter;

  constructor(
    exporterConfig: OTLPExporterNodeConfigBase,
    timeoutMillis: number = 60000,
  ) {
    super(exporterConfig, exporterConfig.url || "", timeoutMillis);
    this.otlpExporter = new OTLPTraceExporter(exporterConfig);
  }

  async export(
    spans: ReadableSpan[],
    resultCallback: (result: ExportResult) => void,
  ): Promise<void> {
    const startTime = Date.now();
    this.totalExports++;

    try {
      await this.checkNetworkConnectivity();
      this.logSystemResources();

      this.otlpExporter.export(spans, (result) => {
        const duration = Date.now() - startTime;
        
        if (result.code === 0) {
          this.successfulExports++;
          this.logSuccess(spans.length, duration);
        } else {
          this.logDetailedFailure(result.error, spans.length, duration);
        }
        
        this.logExportDuration(startTime);
        resultCallback(result);
      });
    } catch (error) {
      const duration = Date.now() - startTime;
      this.logDetailedFailure(error, spans.length, duration);
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
