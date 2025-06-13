# Add Comprehensive Message Validation Tests and Documentation

## Overview

This PR adds comprehensive validation testing capabilities for Elasticsearch messages processed through the synthetics-monitor-extractor pipeline. The validation suite demonstrates how messages are validated, transformed, and routed to Kafka.

## Changes Added

### 1. Message Validation Test Suite (`test-message-validation.ts`)

A comprehensive test script that validates the user's actual Elasticsearch message through all pipeline stages:

- **Monitor Name Pattern Matching**: Tests `*prd*` pattern filtering
- **Schema Validation**: Validates against `ElasticsearchSourceSchema` and `ElasticsearchHitSchema`
- **Business Context Extraction**: Tests extraction of domain, department, criticality, environment from tags
- **Kafka Key Sanitization**: Validates key sanitization logic producing expected format
- **Full Pipeline Validation**: Uses actual `validateElasticsearchHits` function

### 2. Validation Documentation (`VALIDATION_GUIDE.md`)

Complete guide explaining:
- Validation pipeline overview and configuration
- Schema requirements and business context extraction rules
- Kafka message output format and routing headers
- Testing procedures and troubleshooting steps
- Example valid message structure

### 3. Package.json Script

Added `test:validation` script for easy testing:
```bash
bun run test:validation
```

## Test Results

The validation suite successfully validates the user's message:

```
✅ PASS - monitorNamePattern
✅ PASS - sourceValidation  
✅ PASS - hitValidation
✅ PASS - businessContext
✅ PASS - keySanitization
✅ PASS - fullPipeline
```

**Final Kafka Message Details:**
- **Topic**: `monitoring.raw.events`
- **Key**: `raw::marketingtechnology::mtpresskitjourneytommyhilfigertheapxgpcollectioncoreprd::1718305347041`
- **Headers**: `monitor-type=browser, status=up, domain=marketing_technology, department=press_kits, environment=eu-shared-services.prd`

## Message Validation Confirmed

The user's Elasticsearch message for "MT - PressKit Journey | TommyHilfigerTheAPXGPCollection (core) - prd" will successfully:

1. **Pass monitor name filtering** (`*prd*` pattern match)
2. **Validate against all schemas** (ElasticsearchSourceSchema, ElasticsearchHitSchema)
3. **Extract business context** from tags (domain: marketing_technology, department: press_kits, criticality: medium, environment: production)
4. **Transform to standardized format** (MonitorInfo object)
5. **Generate sanitized Kafka key** following existing patterns
6. **Publish to Kafka** with proper routing headers

## Benefits

- **Validation Confidence**: Developers can validate any Elasticsearch message before deployment
- **Documentation**: Clear guide for understanding the validation pipeline
- **Testing**: Automated validation testing without requiring full environment setup
- **Troubleshooting**: Detailed error reporting and validation steps

## Testing

Run the validation suite:
```bash
bun run test:validation
```

The test uses mock configuration to avoid environment dependencies while maintaining accuracy by testing against actual validation functions from the codebase.

---

**Link to Devin run**: https://app.devin.ai/sessions/e8c4c53760744993ae02a919ea2736ea

**Requested by**: Simon Owusu (zx8086@mac.com)
