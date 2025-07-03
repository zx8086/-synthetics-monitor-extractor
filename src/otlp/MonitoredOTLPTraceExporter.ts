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
    const startTime = Date.now();
    this.totalExports++;

    try {
      console.log(`DEBUG: *** TRACE EXPORT CALLED *** #${this.totalExports} to ${this.url}`);
      console.log(`DEBUG: Export called at: ${new Date(startTime).toISOString()}`);
      console.log(`DEBUG: Configured timeout: ${this.timeoutMillis}ms`);
      console.log(`DEBUG: Number of spans: ${spans.length}`);
      
      log(`DEBUG: TRACE EXPORT ATTEMPT #${this.totalExports} to ${this.url}`);
      log(`DEBUG: Configured timeout: ${this.timeoutMillis}ms`);
      log(`DEBUG: Export start time: ${new Date(startTime).toISOString()}`);
      log(`DEBUG: Number of spans: ${spans.length}`);
      
      this.logSystemResources();

      let timeoutFired = false;
      const exportTimeout = setTimeout(() => {
        timeoutFired = true;
        const duration = Date.now() - startTime;
        const timeoutError = new Error(`Trace export timeout after ${this.timeoutMillis}ms`);
        this.logDetailedFailure(timeoutError, spans.length, duration);
        log(`DEBUG: Trace export TIMEOUT in ${duration}ms`);
        console.log(`DEBUG: *** TRACE EXPORT TIMEOUT *** after ${duration}ms`);
        resultCallback({ code: 1, error: timeoutError });
      }, this.timeoutMillis);

      this.otlpExporter.export(spans, (result) => {
        if (timeoutFired) {
          log(`DEBUG: OTLP trace exporter callback received after timeout - ignoring`);
          console.log(`DEBUG: *** TRACE EXPORT CALLBACK AFTER TIMEOUT *** - ignoring`);
          return;
        }
        
        clearTimeout(exportTimeout);
        const duration = Date.now() - startTime;
        log(`DEBUG: OTLP trace exporter callback received after ${duration}ms`);
        console.log(`DEBUG: *** TRACE EXPORT CALLBACK *** received after ${duration}ms`);
        
        if (result.code === 0) {
          this.successfulExports++;
          this.logSuccess(spans.length, duration);
          log(`DEBUG: Trace export SUCCESS in ${duration}ms`);
          console.log(`DEBUG: *** TRACE EXPORT SUCCESS *** in ${duration}ms`);
        } else {
          this.logDetailedFailure(result.error, spans.length, duration);
          log(`DEBUG: Trace export FAILED in ${duration}ms - Error:`, result.error?.message || result.error);
          console.log(`DEBUG: *** TRACE EXPORT FAILED *** in ${duration}ms - Error:`, result.error?.message || result.error);
        }
        
        this.logExportDuration(startTime);
        resultCallback(result);
      });
    } catch (error) {
      const duration = Date.now() - startTime;
      this.logDetailedFailure(error, spans.length, duration);
      log(`DEBUG: Trace export EXCEPTION in ${duration}ms - Error:`, error instanceof Error ? error.message : error);
      console.log(`DEBUG: *** TRACE EXPORT EXCEPTION *** in ${duration}ms - Error:`, error instanceof Error ? error.message : error);
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
