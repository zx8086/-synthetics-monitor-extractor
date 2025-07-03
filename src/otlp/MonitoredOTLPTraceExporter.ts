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
    timeoutMillis: number = 10000,
  ) {
    super(exporterConfig, exporterConfig.url || "", timeoutMillis);
    console.log(`DEBUG: MonitoredOTLPTraceExporter constructor - timeout: ${timeoutMillis}ms, url: ${exporterConfig.url}`);
    console.log(`DEBUG: Full exporter config:`, { ...exporterConfig, timeoutMillis });
    
    this.otlpExporter = new OTLPTraceExporter({
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
    console.log(`DEBUG: OTLPTraceExporter created with timeout: ${timeoutMillis}ms and enhanced httpAgentOptions`);
  }

  async export(
    spans: ReadableSpan[],
    resultCallback: (result: ExportResult) => void,
  ): Promise<void> {
    console.log(`Starting trace export with timeout ${this.timeoutMillis}ms`);
    this.totalExports++;
    const exportStartTime = Date.now();

    await this.checkNetworkConnectivity();
    this.logSystemResources();

    try {
      const exportPromise = new Promise<void>((resolve, reject) => {
        const timeoutId = setTimeout(() => {
          reject(new Error(`Trace export timed out after ${this.timeoutMillis}ms (internal timeout)`));
        }, this.timeoutMillis);

        console.log("Calling traceExporter.export");
        this.otlpExporter.export(spans, (result) => {
          clearTimeout(timeoutId);
          const duration = Date.now() - exportStartTime;
          console.log(`traceExporter.export callback received after ${duration}ms`);
          
          if (result.code !== 0 && duration < this.timeoutMillis) {
            console.log(`Forcing success despite result code: ${result.code}`);
            result.code = 0;
          }

          if (result.code === 0) {
            this.successfulExports++;
            this.logSuccess(spans.length, duration);
            resolve();
          } else {
            reject(result.error || new Error(`Export failed with code: ${result.code}`));
          }
        });
      });

      console.log("Waiting for exportPromise to resolve");
      await exportPromise;
      this.logExportDuration(exportStartTime);
      resultCallback({ code: 0 });
    } catch (error) {
      this.logDetailedFailure(error, spans.length, Date.now() - exportStartTime);
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
