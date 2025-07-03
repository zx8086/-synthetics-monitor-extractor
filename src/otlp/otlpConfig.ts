/* src/otlp/otlpConfig.ts */

import { config } from "../config.js";

export const otlpConfig = {
  logIntervalMs: config.openTelemetry.metricIntervalMs || 10000,
  timeoutMillis: 10000,
  concurrencyLimit: 100,
  keepAlive: true,
};
