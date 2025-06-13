# Message Validation Guide

This guide explains how to validate Elasticsearch messages against the synthetics-monitor-extractor pipeline and ensure they will successfully reach Kafka.

## Overview

The synthetics-monitor-extractor processes Elasticsearch messages through several validation stages:

1. **Monitor Name Pattern Matching** - Filters messages based on `monitorNamePattern`
2. **Schema Validation** - Validates message structure against `ElasticsearchSourceSchema`
3. **Business Context Extraction** - Extracts metadata from message tags
4. **Message Transformation** - Converts to standardized `MonitorInfo` format
5. **Kafka Publishing** - Sends to Kafka with sanitized keys and routing headers

## Configuration

The extraction process uses these key configuration settings:

```typescript
extraction: {
  intervalMinutes: 10,
  timeRange: "now-5m",
  maxResults: 1000,
  timeout: "30s",
  indexPattern: "synthetics-*",
  monitorNamePattern: "*prd*",  // Filters monitors containing "prd"
}
```

## Validation Requirements

### 1. Monitor Name Pattern

Your monitor name must match the configured pattern:
- Pattern: `*prd*` (contains "prd")
- Example: `"MT - PressKit Journey | TommyHilfigerTheAPXGPCollection (core) - prd"` ✅

### 2. Required Schema Fields

Your message `_source` must contain:

**Required:**
- `monitor.id` (string)
- `monitor.name` (string) 
- `monitor.type` (string)
- `monitor.status` (string)
- `@timestamp` (string)

**Optional but recommended:**
- `monitor.duration.us` (number)
- `url.full` (string)
- `tags` (string array)
- `agent` (object with name, id, type, version)
- `observer` (object with name, geo.name)
- `meta.space_id` (string)

### 3. Business Context Tags

For successful processing, include these tags in your message:

```json
"tags": [
  "domain:marketing_technology",    // Required: business domain
  "department:press_kits",          // Required: department
  "criticality:medium",             // Required: high|medium|low
  "environment:production"          // Required: environment
]
```

### 4. Kafka Message Output

Successfully validated messages produce:

**Topic:** `monitoring.raw.events`

**Key Format:** `raw::{domain}::{sanitized_monitor_name}::{timestamp}`
- Example: `raw::marketingtechnology::mtpresskitjourneytommyhilfigertheapxgpcollectioncoreprd::1718305347041`

**Headers:**
- `monitor-type`: Monitor type (e.g., "browser")
- `status`: Monitor status (e.g., "up")
- `domain`: Business domain
- `department`: Department
- `environment`: Environment name

## Testing Your Message

Use the validation test suite to verify your message:

```bash
bun run test-message-validation.ts
```

This will test your message through all validation stages and show exactly how it will be processed.

## Example Valid Message

```json
{
  "_source": {
    "monitor": {
      "id": "PressKit_TommyHilfigerTheAPXGPCollection-eu-shared-services.prd-developer-experience",
      "name": "MT - PressKit Journey | TommyHilfigerTheAPXGPCollection (core) - prd",
      "type": "browser",
      "status": "up",
      "duration": { "us": 54310046 }
    },
    "url": {
      "full": "https://presskit.tommy.com/TommyHilfigerTheAPXGPCollection/",
      "domain": "presskit.tommy.com",
      "path": "/TommyHilfigerTheAPXGPCollection/"
    },
    "@timestamp": "2025-06-13T19:02:27.041Z",
    "tags": [
      "production",
      "marketing_technology",
      "domain:marketing_technology",
      "department:press_kits", 
      "criticality:medium",
      "environment:production"
    ],
    "agent": {
      "name": "ip-10-34-51-29.eu-central-1.compute.internal",
      "id": "6221e0fc-9de6-40d1-aaf1-8a16d68f4869",
      "type": "heartbeat",
      "version": "9.0.1"
    },
    "observer": {
      "name": "4132adb4-7ea4-45ea-93c7-1d486924cb5e",
      "geo": { "name": "eu-shared-services.prd" }
    },
    "meta": {
      "space_id": "developer-experience"
    }
  }
}
```

## Common Issues

### Message Rejected
- **Monitor name doesn't match pattern**: Ensure monitor name contains "prd"
- **Missing required fields**: Check `monitor.id`, `monitor.name`, `monitor.type`, `monitor.status`, `@timestamp`
- **Invalid business context**: Verify all required tags are present with correct format

### Key Sanitization
Keys are sanitized by:
1. Converting to lowercase
2. Removing all spaces
3. Removing special characters (except letters, numbers, colons, dashes)

Example: `"MT - PressKit Journey"` → `"mtpresskitjourney"`

## Troubleshooting

1. **Run validation tests** to identify specific issues
2. **Check logs** in `src/invalid.json` for detailed error messages
3. **Verify configuration** matches your Elasticsearch index patterns
4. **Test business context extraction** with your specific tag format

For additional help, refer to the source code in `src/types.ts` for validation schemas and `src/index.ts` for processing logic.
