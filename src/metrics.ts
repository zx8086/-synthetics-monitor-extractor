import * as promClient from 'prom-client';
import { config } from './config.js';

export let registry: promClient.Registry;
export let kafkaProducedMessagesCounter: promClient.Counter;
export let kafkaProducerErrorsCounter: promClient.Counter;
export let kafkaMessageSizeHistogram: promClient.Histogram;

export function initializeMetrics() {
  registry = new promClient.Registry();

  promClient.collectDefaultMetrics({
    register: registry,
    prefix: config.metrics.prefix,
  });

  kafkaProducedMessagesCounter = new promClient.Counter({
    name: 'kafka_produced_messages',
    help: 'Number of produced Kafka messages',
    labelNames: ['topic', 'status'],
    registers: [registry],
  });

  kafkaProducerErrorsCounter = new promClient.Counter({
    name: 'kafka_producer_errors',
    help: 'Number of Kafka producer errors',
    labelNames: ['topic', 'error_type'],
    registers: [registry],
  });

  kafkaMessageSizeHistogram = new promClient.Histogram({
    name: 'kafka_message_size_bytes',
    help: 'Size of Kafka messages in bytes',
    labelNames: ['topic'],
    buckets: [100, 1000, 10000, 100000, 1000000],
    registers: [registry],
  });
}

export async function startMetricsServer() {
  if (!config.metrics.enabled) {
    console.log('Metrics disabled, skipping metrics server startup');
    return;
  }

  const server = Bun.serve({
    port: config.metrics.port,
    async fetch(req) {
      const url = new URL(req.url);
      if (url.pathname === config.metrics.endpoint) {
        const metrics = await registry.metrics();
        return new Response(metrics, {
          headers: { 'Content-Type': registry.contentType },
        });
      }
      return new Response('Not Found', { status: 404 });
    },
  });

  console.log(`Metrics server started on port ${config.metrics.port}${config.metrics.endpoint}`);
  return server;
}
