/* test-logger.js */

import { log, err, warn, debug } from "./src/utils/logger.js";

console.log("Testing OpenTelemetry logger...");

// Test different log levels
log("This is an info message", { test: "data", level: "info" });
err("This is an error message", { test: "data", level: "error" });
warn("This is a warning message", { test: "data", level: "warn" });
debug("This is a debug message", { test: "data", level: "debug" });

console.log("Logger test completed"); 