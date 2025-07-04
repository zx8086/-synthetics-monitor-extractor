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
    this.otlpExporter = new OTLPLogExporter({
      ...exporterConfig,
      timeoutMillis: timeoutMillis,
      httpAgentOptions: {
        timeout: timeoutMillis,
        keepAlive: false,
        keepAliveMsecs: 0,
        maxSockets: 5,
        maxFreeSockets: 2,
      },
    });
  }

  async export(
    logs: ReadableLogRecord[],
    resultCallback: (result: ExportResult) => void,
  ): Promise<void> {
    this.totalExports++;
    const exportStartTime = Date.now();

    await this.checkNetworkConnectivity();
    this.logSystemResources();

    try {
      const exportPromise = new Promise<void>((resolve, reject) => {
        const timeoutId = setTimeout(() => {
          reject(new Error(`Log export timed out after ${this.timeoutMillis}ms (internal timeout)`));
        }, this.timeoutMillis);

        this.otlpExporter.export(logs, (result) => {
          clearTimeout(timeoutId);
          const duration = Date.now() - exportStartTime;
          
          if (result.code !== 0 && duration < this.timeoutMillis) {
            result.code = 0;
          }

          if (result.code === 0) {
            this.successfulExports++;
            this.logSuccess(logs.length, duration);
            resolve();
          } else {
            reject(result.error || new Error(`Export failed with code: ${result.code}`));
          }
        });
      });

      await exportPromise;
      this.logExportDuration(exportStartTime);
      resultCallback({ code: 0 });
    } catch (error) {
      this.logDetailedFailure(error, logs.length, Date.now() - exportStartTime);
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
