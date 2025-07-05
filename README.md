# synthetics-monitor-extractor

To install dependencies:

```bash
bun install
```

To run:

```bash
bun run index.ts
```

## Environment Variables

### UI Customization
- `KAFKA_CLIENT_ID`: Besides its primary purpose for Kafka connection, this value is also used to customize the UI title.
  For example, setting `KAFKA_CLIENT_ID=my-monitoring-app` will result in a UI title of "My Monitoring App Monitor Errors".
  The value will be formatted by replacing hyphens with spaces and capitalizing each word.

This project was created using `bun init` in bun v1.2.15. [Bun](https://bun.sh) is a fast all-in-one JavaScript runtime.

## Security

### Security Scanning

This project includes comprehensive security scanning:

```bash
# Run all security scans
bun run security:full

# Run dependency vulnerability scan
bun run security:scan

# Run code security analysis
bun run security:code

# Run container security scan
bun run docker:security

# Run security best practices check
bun run security:check
```

### Security Features

- **Input Validation**: All inputs are validated using Zod schemas
- **Circuit Breaker Pattern**: Protects against cascading failures
- **Rate Limiting**: Configurable rate limiting for API endpoints
- **Non-root Container**: Runs as unprivileged user in Docker
- **Security Headers**: Proper security headers in HTTP responses
- **No Hardcoded Secrets**: All sensitive data via environment variables

### Known Vulnerabilities

Some vulnerabilities in the base Bun runtime's Go dependencies are tracked in `.snyk` file. These are:
- Not directly exploitable by our application
- In packages we don't use (SSH, OAuth2, JWT, etc.)
- Mitigated by using the latest stable Bun version

### Security Updates

1. Always use the latest stable Bun version in Dockerfile
2. Run `apk update && apk upgrade` to patch Alpine packages
3. Regularly update dependencies with `bun update`
4. Monitor security advisories via GitHub Dependabot
