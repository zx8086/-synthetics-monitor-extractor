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
