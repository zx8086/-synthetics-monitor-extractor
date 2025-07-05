# syntax=docker/dockerfile:1.4

# Base stage - Use the latest stable version instead of canary for better security
FROM oven/bun:1.1.42-alpine AS base

# Update Alpine packages to fix security vulnerabilities
RUN apk update && \
    apk upgrade --no-cache && \
    apk add --no-cache \
    curl \
    ca-certificates && \
    rm -rf /var/cache/apk/*

# Set common environment variables
ENV CN_ROOT=/usr/src/app \
    CN_CXXCBC_CACHE_DIR=/usr/src/app/deps/cache \
    NODE_ENV=production

WORKDIR /usr/src/app

# Create directories
RUN mkdir -p /usr/src/app/logs /usr/src/app/deps/cache /usr/src/app/.sourcemaps && \
    chown -R bun:bun /usr/src/app

# Dependencies stage - Optimized for caching
FROM base AS deps

# Copy only package files needed for installation
COPY --chown=bun:bun package.json bun.lock tsconfig.json ./

# Single RUN command for better caching
RUN --mount=type=cache,target=/root/.bun,sharing=locked \
    bun install --frozen-lockfile --production && \
    mkdir -p node_modules && \
    chown -R bun:bun node_modules

# Development stage
FROM deps AS development
ENV NODE_ENV=development

# Install development dependencies
RUN bun install --frozen-lockfile

# Copy source files after dependencies
COPY --chown=bun:bun . .
CMD ["bun", "run", "dev"]

# Build stage
FROM deps AS builder

# Copy source files all at once
COPY --chown=bun:bun src/ ./src/
COPY --chown=bun:bun tsconfig.json ./

# Combine mkdir and build operations
RUN --mount=type=cache,target=/usr/src/app/.build \
    mkdir -p dist dist/maps && \
    bun build ./src/index.ts \
    --target=node \
    --outdir ./dist \
    --sourcemap \
    --external dns \
    --external bun \
    --external @platformatic/kafka \
    --manifest && \
    find dist -name "*.map" -exec mv {} dist/maps/ \; || true

# Final release stage
FROM base AS release

# Copy package files and install dependencies in one layer
COPY --chown=bun:bun package.json bun.lock ./
RUN --mount=type=cache,target=/root/.bun,sharing=locked \
    bun install --frozen-lockfile

# Copy source files
COPY --chown=bun:bun . .

# Combine build operations into a single layer
RUN set -e && \
    bun build ./src/index.ts \
    --target=node \
    --outdir ./dist \
    --sourcemap \
    --external dns \
    --external bun \
    --external @platformatic/kafka \
    --manifest && \
    mkdir -p /usr/src/app/dist/maps && \
    find /usr/src/app/dist -name "*.map" -exec mv {} /usr/src/app/dist/maps/ \; || true && \
    bun install --frozen-lockfile --production && \
    chown -R bun:bun .

# Set production variables
ENV OPEN_TELEMETRY_ENABLED=true \
    SOURCE_MAP_SUPPORT=true \
    PRESERVE_SOURCE_MAPS=true \
    NODE_ENV=production

# Consolidate all labels in the final stage
LABEL org.opencontainers.image.title="synthetics-monitor-extractor" \
    org.opencontainers.image.description="Synthetics Monitor Extractor is a high-performance data pipeline service built with Bun that extracts synthetic monitoring data from Elasticsearch and publishes it to Kafka. It features comprehensive observability, data validation, and error tracking capabilities." \
    org.opencontainers.image.created="${BUILD_DATE}" \
    org.opencontainers.image.version="${BUILD_VERSION}" \
    org.opencontainers.image.revision="${COMMIT_HASH}" \
    org.opencontainers.image.authors="Simon Owusu <simonowusupvh@gmail.com>" \
    org.opencontainers.image.vendor="Siobytes" \
    org.opencontainers.image.licenses="MIT" \
    org.opencontainers.image.url="https://github.com/zx8086/-synthetics-monitor-extract" \
    org.opencontainers.image.source="https://github.com/zx8086/-synthetics-monitor-extract" \
    org.opencontainers.image.documentation="https://github.com/zx8086/-synthetics-monitor-extract/README.md" \
    org.opencontainers.image.base.name="oven/bun:1.1.42-alpine" \
    org.opencontainers.image.source.repository="github.com/zx8086/-synthetics-monitor-extract" \
    org.opencontainers.image.source.branch="${GITHUB_REF_NAME:-master}" \
    org.opencontainers.image.source.commit="${COMMIT_HASH}" \
    com.synthetics-monitor-extractor.maintainer="Simon Owusu <simonowusupvh@gmail.com>" \
    com.synthetics-monitor-extractor.release-date="${BUILD_DATE}" \
    com.synthetics-monitor-extractor.version.is-production="true" \
    org.opencontainers.image.ref.name="${GITHUB_REF_NAME:-master}" \
    org.opencontainers.image.version.semver="${BUILD_VERSION}" \
    org.opencontainers.image.version.major="1" \
    org.opencontainers.image.version.minor="0" \
    org.opencontainers.image.version.patch="0"

# Set runtime environment variables
ENV ELASTICSEARCH_URL="" \
    ELASTICSEARCH_USERNAME="" \
    ELASTICSEARCH_PASSWORD="" \
    KAFKA_BROKERS="" \
    KAFKA_TOPIC="" \
    KAFKA_CLIENT_ID="" \
    OPEN_TELEMETRY_ENABLED="" \
    OPEN_TELEMETRY_SERVICE_NAME="" \
    OPEN_TELEMETRY_SERVICE_VERSION="" \
    OPEN_TELEMETRY_DEPLOYMENT_ENVIRONMENT="" \
    OPEN_TELEMETRY_TRACES_ENDPOINT="" \
    OPEN_TELEMETRY_METRICS_ENDPOINT="" \
    OPEN_TELEMETRY_LOGS_ENDPOINT="" \
    OPEN_TELEMETRY_METRIC_READER_INTERVAL="" \
    OPEN_TELEMETRY_SUMMARY_LOG_INTERVAL="" \
    LOG_LEVEL="" \
    LOG_CONSOLE_ENABLED="" \
    LOG_CONSOLE_LEVEL="" \
    LOG_OPENTELEMETRY_ENABLED="" \
    LOG_OPENTELEMETRY_LEVEL="" \
    METRICS_PORT=""

USER bun
EXPOSE 9090/tcp

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9090/metrics || exit 1

CMD ["bun", "run", "dist/index.js"]
