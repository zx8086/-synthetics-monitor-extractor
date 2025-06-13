/* test-schema-investigation.ts */

import { 
  ElasticsearchMonitorSchema,
  ElasticsearchSourceSchema, 
  ElasticsearchHitSchema
} from "./src/types.js";

console.log("🔍 Investigating schema validation discrepancy...");

const minimalMonitorWithStatus = {
  "id": "test-id",
  "name": "MT - PressKit Journey | TommyHilfigerTheAPXGPCollection (core) - prd",
  "type": "browser",
  "status": "up"
};

const minimalMonitorWithoutStatus = {
  "id": "test-id", 
  "name": "MT - PressKit Journey | TommyHilfigerTheAPXGPCollection (core) - prd",
  "type": "browser"
};

const productionMessage = {
  "_source": {
    "monitor": {
      "duration": {
        "us": 54310046
      },
      "origin": "project",
      "name": "MT - PressKit Journey | TommyHilfigerTheAPXGPCollection (core) - prd",
      "project": {
        "name": "eu-shared-services.prd",
        "id": "eu-shared-services.prd"
      },
      "id": "PressKit_TommyHilfigerTheAPXGPCollection-eu-shared-services.prd-developer-experience",
      "timespan": {
        "lt": "2025-07-14T05:02:26.997Z",
        "gte": "2025-06-13T19:02:26.997Z"
      },
      "check_group": "cfb0c695-4888-11f0-b0cb-0a58a9feac02-1",
      "fleet_managed": true,
      "type": "browser",
      "status": "up"
    },
    "@timestamp": "2025-06-13T19:02:27.041Z"
  }
};

async function investigateSchemaValidation() {
  console.log("\n=== Schema Validation Investigation ===\n");

  console.log("1. Testing ElasticsearchMonitorSchema with status field:");
  try {
    const result = ElasticsearchMonitorSchema.parse(minimalMonitorWithStatus);
    console.log("   ✅ Monitor with status: PASS");
    console.log("   Parsed result:", result);
  } catch (error: any) {
    console.log("   ❌ Monitor with status: FAIL");
    if (error.issues) {
      error.issues.forEach((issue: any) => {
        console.log(`      - ${issue.path.join('.')}: ${issue.message}`);
      });
    }
  }

  console.log("\n2. Testing ElasticsearchMonitorSchema without status field:");
  try {
    const result = ElasticsearchMonitorSchema.parse(minimalMonitorWithoutStatus);
    console.log("   ✅ Monitor without status: PASS");
    console.log("   Parsed result:", result);
  } catch (error: any) {
    console.log("   ❌ Monitor without status: FAIL");
    if (error.issues) {
      error.issues.forEach((issue: any) => {
        console.log(`      - ${issue.path.join('.')}: ${issue.message}`);
      });
    }
  }

  console.log("\n3. Testing production message monitor object:");
  try {
    const result = ElasticsearchMonitorSchema.parse(productionMessage._source.monitor);
    console.log("   ✅ Production monitor object: PASS");
    console.log("   Parsed result:", result);
  } catch (error: any) {
    console.log("   ❌ Production monitor object: FAIL");
    if (error.issues) {
      error.issues.forEach((issue: any) => {
        console.log(`      - ${issue.path.join('.')}: ${issue.message}`);
      });
    }
  }

  console.log("\n4. Testing full production message:");
  try {
    const result = ElasticsearchHitSchema.parse(productionMessage);
    console.log("   ✅ Full production message: PASS");
  } catch (error: any) {
    console.log("   ❌ Full production message: FAIL");
    if (error.issues) {
      error.issues.forEach((issue: any) => {
        console.log(`      - ${issue.path.join('.')}: ${issue.message}`);
      });
    }
  }

  console.log("\n5. Checking schema definition:");
  console.log("   ElasticsearchMonitorSchema shape:");
  console.log("   - Requires: id, name, type, status");
  console.log("   - Optional: duration");
  
  console.log("\n6. Verifying field presence in production message:");
  const monitor = productionMessage._source.monitor;
  console.log("   Field presence check:");
  console.log(`   - id: ${!!monitor.id} (${monitor.id})`);
  console.log(`   - name: ${!!monitor.name} (${monitor.name})`);
  console.log(`   - type: ${!!monitor.type} (${monitor.type})`);
  console.log(`   - status: ${!!monitor.status} (${monitor.status})`);
  console.log(`   - duration: ${!!monitor.duration} (${monitor.duration?.us})`);
}

investigateSchemaValidation().catch(console.error);
