# OpenTelemetry Logging System

This project now uses a centralized logging system that sends all logs to OpenTelemetry instead of writing to stdio.

## Configuration

### Environment Variables

Add these to your `.env` file:

```bash
# OpenTelemetry Configuration
OPEN_TELEMETRY_ENABLED=true
OPEN_TELEMETRY_TRACES_ENDPOINT=http://localhost:4318/v1/traces
OPEN_TELEMETRY_METRICS_ENDPOINT=http://localhost:4318/v1/metrics
OPEN_TELEMETRY_LOGS_ENDPOINT=http://localhost:4318/v1/logs
OPEN_TELEMETRY_SERVICE_NAME=synthetics-monitor-extractor
OPEN_TELEMETRY_METRIC_INTERVAL_MS=15000

# Logging Configuration
LOG_LEVEL=info
LOG_FORMAT=json
LOG_INCLUDE_TIMESTAMP=true
```

### Log Levels

- `debug` - Detailed debug information
- `info` - General information messages
- `warn` - Warning messages
- `error` - Error messages

## Usage

### Import the Logger

```typescript
import { log, err, warn, debug } from "./src/utils/logger.js";
```

### Basic Logging

```typescript
// Info level logging
log("Application started successfully");

// Error level logging
err("Failed to connect to database", { error: "Connection timeout" });

// Warning level logging
warn("High memory usage detected", { memory: "85%" });

// Debug level logging
debug("Processing request", { requestId: "123", method: "GET" });
```

### Structured Logging

All log functions accept an optional metadata object:

```typescript
log("User action completed", {
  userId: "12345",
  action: "login",
  timestamp: new Date().toISOString(),
  ip: "192.168.1.1"
});
```

### Trace Integration

The logger automatically includes trace and span IDs when available:

```typescript
// Trace information is automatically included in the log output
log("Processing request", { requestId: "123" });
// Output will include trace.id and trace.span.id if available
```

## Features

### ECS Format

Logs are formatted using Elastic Common Schema (ECS) for better integration with Elasticsearch and other observability tools.

### OpenTelemetry Integration

- Automatic trace and span correlation
- Structured logging with metadata
- No console output (logs only go to OpenTelemetry)
- Configurable log levels

### Error Handling

The logger includes proper error handling and will gracefully degrade if OpenTelemetry is not available.

## Testing

Run the test script to verify logging functionality:

```bash
bun run test-logger.js
```

## Migration from console.log

Replace all `console.log`, `console.error`, `console.warn`, and `console.debug` calls with the appropriate logger functions:

```typescript
// Before
console.log("Processing data...");
console.error("Error occurred:", error);

// After
log("Processing data...");
err("Error occurred:", error);
```

## OpenTelemetry Setup

Make sure you have an OpenTelemetry collector running that can receive logs on the configured endpoint. The default endpoint is `http://localhost:4318/v1/logs`.

For local development, you can use the OpenTelemetry Collector with a simple configuration:

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:

exporters:
  logging:
    loglevel: debug

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [logging]
``` 