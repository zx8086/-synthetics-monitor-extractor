# Configuration System

This project uses a comprehensive configuration system with Zod validation, similar to the MCP server pattern.

## Environment Variables

### Required Variables
- `ELASTIC_NODE` - Elasticsearch cluster URL (e.g., https://elasticsearch.example.com)
- `KAFKA_BROKERS` - Comma-separated list of Kafka brokers (e.g., broker1:9092,broker2:9092)

### Optional Variables

#### Elasticsearch Configuration
- `ELASTIC_API_KEY_ID` - Elasticsearch API key ID
- `ELASTIC_API_KEY` - Elasticsearch API key
- `ELASTIC_MAX_RETRIES` - Maximum retry attempts (default: 5)
- `ELASTIC_REQUEST_TIMEOUT` - Request timeout in milliseconds (default: 30000)
- `ELASTIC_COMPRESSION` - Enable compression (default: true)
- `ELASTIC_SNIFF_ON_START` - Enable sniffing on start (default: false)
- `ELASTIC_REJECT_UNAUTHORIZED` - Reject unauthorized TLS certificates (default: true)

#### Kafka Configuration
- `KAFKA_CLIENT_ID` - Kafka client ID (default: synthetics-extractor)
- `KAFKA_SSL` - Enable SSL (default: false)
- `KAFKA_USERNAME` - SASL username (required if SSL enabled)
- `KAFKA_PASSWORD` - SASL password (required if SSL enabled)
- `KAFKA_CONNECTION_TIMEOUT` - Connection timeout in milliseconds (default: 3000)
- `KAFKA_AUTHENTICATION_TIMEOUT` - Authentication timeout in milliseconds (default: 3000)
- `KAFKA_REQUEST_TIMEOUT` - Request timeout in milliseconds (default: 30000)
- `KAFKA_INITIAL_RETRY_TIME` - Initial retry time in milliseconds (default: 1000)
- `KAFKA_RETRIES` - Number of retries (default: 8)

#### Extraction Configuration
- `EXTRACTION_INTERVAL_MINUTES` - Extraction interval in minutes (default: 1)
- `EXTRACTION_TIME_RANGE` - Elasticsearch time range query (default: now-5m)
- `EXTRACTION_MAX_RESULTS` - Maximum results per query (default: 1000)
- `EXTRACTION_TIMEOUT` - Query timeout (default: 30s)
- `EXTRACTION_INDEX_PATTERN` - Index pattern to search (default: synthetics-*)

#### Logging Configuration
- `LOG_LEVEL` - Log level: debug, info, warn, error (default: info)
- `LOG_FORMAT` - Log format: json, text (default: text)
- `LOG_INCLUDE_TIMESTAMP` - Include timestamp in logs (default: true)

#### System Configuration
- `NODE_ENV` - Node environment: development, production, test (default: development)

## Configuration Features

- **Type Safety**: All configuration is validated using Zod schemas
- **Environment Validation**: Required variables are checked at startup
- **Default Values**: Sensible defaults for all optional configuration
- **Error Handling**: Clear error messages for invalid configuration
- **Connection Testing**: Validates Elasticsearch and Kafka connections at startup

## Usage

The configuration is automatically loaded when importing from `./src/config.js`:

```typescript
import { config } from "./src/config.js";

// Access typed configuration
console.log(config.elasticsearch.node);
console.log(config.kafka.brokers);
```

## Testing Configuration

Run the configuration test to verify your environment variables:

```bash
bun run test-config.ts
```
