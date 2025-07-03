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
      },
    });
  }

  async export(
    logs: ReadableLogRecord[],
    resultCallback: (result: ExportResult) => void,
  ): Promise<void> {
    const startTime = Date.now();
    this.totalExports++;

    try {
      console.log(`DEBUG: *** LOG EXPORT CALLED *** #${this.totalExports} to ${this.url}`);
      console.log(`DEBUG: Export called at: ${new Date(startTime).toISOString()}`);
      console.log(`DEBUG: Configured timeout: ${this.timeoutMillis}ms`);
      console.log(`DEBUG: Number of logs: ${logs.length}`);
      
      log(`DEBUG: LOG EXPORT ATTEMPT #${this.totalExports} to ${this.url}`);
      log(`DEBUG: Configured timeout: ${this.timeoutMillis}ms`);
      log(`DEBUG: Export start time: ${new Date(startTime).toISOString()}`);
      log(`DEBUG: Number of logs: ${logs.length}`);
      
      await this.checkNetworkConnectivity();
      this.logSystemResources();

      let timeoutFired = false;
      const exportTimeout = setTimeout(() => {
        timeoutFired = true;
        const duration = Date.now() - startTime;
        const timeoutError = new Error(`Log export timeout after ${this.timeoutMillis}ms`);
        this.logDetailedFailure(timeoutError, logs.length, duration);
        log(`DEBUG: Log export TIMEOUT in ${duration}ms`);
        resultCallback({ code: 1, error: timeoutError });
      }, this.timeoutMillis);

      this.otlpExporter.export(logs, (result) => {
        if (timeoutFired) {
          log(`DEBUG: OTLP log exporter callback received after timeout - ignoring`);
          return;
        }
        
        clearTimeout(exportTimeout);
        const duration = Date.now() - startTime;
        log(`DEBUG: OTLP log exporter callback received after ${duration}ms`);
        
        if (result.code === 0) {
          this.successfulExports++;
          this.logSuccess(logs.length, duration);
          log(`DEBUG: Log export SUCCESS in ${duration}ms`);
        } else {
          this.logDetailedFailure(result.error, logs.length, duration);
          log(`DEBUG: Log export FAILED in ${duration}ms - Error:`, result.error?.message || result.error);
        }
        
        this.logExportDuration(startTime);
        resultCallback(result);
      });
    } catch (error) {
      const duration = Date.now() - startTime;
      this.logDetailedFailure(error, logs.length, duration);
      log(`DEBUG: Log export EXCEPTION in ${duration}ms - Error:`, error instanceof Error ? error.message : error);
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
