# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Running the Application
```bash
# Development with hot reload
bun run dev
bun run start

# Production build
bun run build
```

### Code Quality
```bash
# Lint and format code
bun run lint
bun run format

# Security scanning
bun run security:full          # Complete Snyk security scan
bun run security:scan          # Dependency vulnerabilities
bun run security:code          # Code analysis
```

### Docker Operations
```bash
# Build and run container
bun run docker:build
bun run docker:run

# Security scan container
bun run docker:security
```

### Health Check
```bash
# Check if application is healthy
bun run health
```

## Architecture Overview

This is a **data pipeline service** that extracts synthetic monitoring data from Elasticsearch and publishes it to Kafka. Built with **Bun runtime** for performance.

### Core Data Flow
1. **Scheduled extraction** from Elasticsearch (configurable intervals)
2. **Data validation** using Zod schemas with business context requirements
3. **Transformation** to standardized MonitorInfo format
4. **Kafka publishing** with routing headers
5. **Error tracking** in SQLite database for invalid records
6. **Observability** via OpenTelemetry and Prometheus

### Key Components
- **src/index.ts**: Main orchestration and extraction logic
- **src/config.ts**: Environment-driven configuration with Zod validation
- **src/types.ts**: Data models and validation schemas
- **src/elasticsearch.ts**: Data extraction client
- **src/kafka.ts**: Message publishing client
- **src/api.ts**: HTTP API and web UI with manual OpenTelemetry instrumentation
- **src/instrumentation.ts**: OpenTelemetry setup and custom metrics
- **src/database.ts**: SQLite database for error tracking

## Critical Configuration

### Environment Variables
Set these required variables:
- `ELASTIC_NODE`: Elasticsearch endpoint
- `KAFKA_BROKERS`: Comma-separated Kafka broker list
- `OPEN_TELEMETRY_ENABLED`: Enable tracing (set to `true` for Elasticsearch visibility)

### Logging Configuration
The application supports independent control of logging channels:

#### Console Logging
- `LOG_CONSOLE_ENABLED=true/false` - Control stdout/stderr output
- `LOG_CONSOLE_LEVEL=debug/info/warn/error` - Console-specific log level

#### OpenTelemetry Logging  
- `LOG_OPENTELEMETRY_ENABLED=true/false` - Send logs to OTEL endpoint
- `LOG_OPENTELEMETRY_LEVEL=debug/info/warn/error` - OTEL-specific log level
- Works independently of `OPEN_TELEMETRY_ENABLED` (which controls traces/metrics)

#### Common Production Patterns
```bash
# Cloud-native: Only OpenTelemetry logs
LOG_CONSOLE_ENABLED=false
LOG_OPENTELEMETRY_ENABLED=true

# Traditional: Only console logs  
LOG_CONSOLE_ENABLED=true
LOG_OPENTELEMETRY_ENABLED=false

# Hybrid: Different log levels per channel
LOG_CONSOLE_ENABLED=true
LOG_CONSOLE_LEVEL=warn
LOG_OPENTELEMETRY_ENABLED=true  
LOG_OPENTELEMETRY_LEVEL=debug
```

### Business Context Requirements
The system enforces strict business context validation through monitor tags:
- `domain:` - Business domain (required)
- `department:` - Organizational department (required)
- `criticality:` - High/medium/low priority (required)
- `environment:` - Deployment environment (required)

Records missing these tags are considered invalid and stored in the error database.

## OpenTelemetry Instrumentation

### Bun Runtime Compatibility
- **Auto-instrumentation doesn't work** with Bun's HTTP server
- **Manual instrumentation** is implemented in `src/api.ts` using `executeWithSpan()` wrapper
- All API endpoints create spans with proper HTTP attributes
- Traces are exported to configured OTLP endpoint

### Enabling Traces in Elasticsearch
Set these environment variables:
```bash
export OPEN_TELEMETRY_ENABLED=true
export OPEN_TELEMETRY_TRACES_ENDPOINT=https://your-elasticsearch-apm:8200/intake/v2/events
```

## Data Validation & Error Handling

### Validation Strategy
- **Zod schemas** for runtime validation
- **Business context validation** for operational requirements
- **Invalid records** are stored in SQLite database with error details
- **Validation errors** are grouped by monitor name and type

### Error Database
- SQLite database in `data/invalid_records.db`
- API endpoints at `/api/invalid-records` for querying errors
- Web UI at `/ui` for viewing and managing invalid records

## Development Patterns

### Configuration Management
- All configuration is environment-driven via `src/config.ts`
- Zod validation ensures type safety and required fields
- Cross-field validation rules (e.g., API keys must be provided together)
- Development vs production defaults

### Observability
- **Prometheus metrics** at `/metrics` endpoint
- **OpenTelemetry traces** to configured OTLP endpoint
- **Structured logging** with Winston and ECS format
- **Custom metrics** for HTTP requests and Kafka operations

### Connection Management
- **Elasticsearch client** with retry logic and health checks
- **Kafka producer** with connection pooling and error handling
- **Graceful shutdown** handling for all connections

## Testing

Currently no formal test suite exists. The codebase has:
- Type definitions for testing frameworks
- Mock data generators in Kafka module
- Bun built-in testing capabilities available

When adding tests, focus on:
- Validation functions with various input scenarios
- Elasticsearch query building and transformation logic
- Kafka message publishing with error conditions
- Business context extraction from tags

## CI/CD Pipeline

GitHub Actions workflow includes:
- Multi-platform Docker builds (AMD64/ARM64)
- Snyk security scanning (dependencies and code)
- Container security scanning with SBOM generation
- Docker Hub publishing
- Self-hosted runner support

## Troubleshooting

### Common Issues
1. **API endpoints not showing in Elasticsearch**: Check `OPEN_TELEMETRY_ENABLED=true` and proper traces endpoint
2. **Business context validation errors**: Ensure monitors have required tags (domain, department, criticality, environment)
3. **Kafka connection failures**: Verify broker list and SSL/SASL configuration
4. **Memory issues**: Check extraction batch size and time range configuration

### Debug Endpoints
- `/api/invalid-records` - View validation errors
- `/api/invalid-records/summary` - Error summary by type
- `/metrics` - Prometheus metrics
- `/ui` - Web interface for error management