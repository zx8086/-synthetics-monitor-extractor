/* test-production-failures.ts */

import { 
  ElasticsearchSourceSchema, 
  ElasticsearchHitSchema,
  validateElasticsearchHits 
} from "./src/types.js";
import { extractBusinessContext } from "./src/index.js";

console.log("🔍 Testing production validation failures...");

const productionFailingMessage = {
  "_source": {
    "summary": {
      "retry_group": "cfb0c695-4888-11f0-b0cb-0a58a9feac02",
      "max_attempts": 2,
      "up": 1,
      "final_attempt": true,
      "down": 0,
      "attempt": 1,
      "status": "up"
    },
    "test_run_id": "ad9b5b5c-3785-4592-9dc7-efc1874d4da8",
    "agent": {
      "name": "ip-10-34-51-29.eu-central-1.compute.internal",
      "id": "6221e0fc-9de6-40d1-aaf1-8a16d68f4869",
      "type": "heartbeat",
      "ephemeral_id": "7e4e8397-1369-4977-a802-ecdad22544e4",
      "version": "9.0.1"
    },
    "synthetics": {
      "type": "heartbeat/summary"
    },
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
    "url": {
      "path": "/TommyHilfigerTheAPXGPCollection/",
      "scheme": "https",
      "port": 443,
      "domain": "presskit.tommy.com",
      "full": "https://presskit.tommy.com/TommyHilfigerTheAPXGPCollection/"
    },
    "tags": [
      "production",
      "marketing_technology",
      "eu-shared-services",
      "presskit",
      "TommyHilfigerTheAPXGPCollection",
      "domain:marketing_technology",
      "department:press_kits",
      "criticality:medium",
      "environment:production"
    ],
    "observer": {
      "geo": {
        "name": "eu-shared-services.prd"
      },
      "name": "4132adb4-7ea4-45ea-93c7-1d486924cb5e"
    },
    "@timestamp": "2025-06-13T19:02:27.041Z",
    "ecs": {
      "version": "8.0.0"
    },
    "config_id": "bca17b2f-40a4-47b0-b6f5-a4a64d844b97",
    "data_stream": {
      "namespace": "developer_experience",
      "type": "synthetics",
      "dataset": "browser"
    },
    "meta": {
      "space_id": "developer-experience"
    },
    "state": {
      "duration_ms": "0",
      "checks": 1,
      "ends": null,
      "started_at": "2025-06-13T19:02:27.041175204Z",
      "id": "4132adb4-7ea4-45ea-93c7-1d486924cb5e-1976aac01e1-0",
      "up": 1,
      "flap_history": [],
      "down": 0,
      "status": "up"
    },
    "event": {
      "agent_id_status": "mismatch",
      "ingested": "2025-06-13T19:02:33Z",
      "type": "heartbeat/summary",
      "dataset": "browser"
    }
  }
};

async function testProductionFailures() {
  console.log("\n=== Testing Production Validation Failures ===\n");

  console.log("1. Testing monitor.status field presence:");
  const monitor = productionFailingMessage._source.monitor;
  console.log("   Monitor object keys:", Object.keys(monitor));
  console.log("   monitor.id:", monitor.id);
  console.log("   monitor.name:", monitor.name);
  console.log("   monitor.type:", monitor.type);
  console.log("   monitor.status:", monitor.status);
  console.log(`   ✅ monitor.status field is present: ${!!monitor.status}`);

  console.log("\n2. Testing ElasticsearchSourceSchema validation:");
  try {
    const result = ElasticsearchSourceSchema.parse(productionFailingMessage._source);
    console.log("   ✅ ElasticsearchSourceSchema: PASS");
  } catch (error: any) {
    console.log("   ❌ ElasticsearchSourceSchema: FAIL");
    if (error.issues) {
      error.issues.forEach((issue: any) => {
        console.log(`      - ${issue.path.join('.')}: ${issue.message}`);
      });
    } else {
      console.log("   Error:", error);
    }
  }

  console.log("\n3. Testing ElasticsearchHitSchema validation:");
  try {
    const result = ElasticsearchHitSchema.parse(productionFailingMessage);
    console.log("   ✅ ElasticsearchHitSchema: PASS");
  } catch (error: any) {
    console.log("   ❌ ElasticsearchHitSchema: FAIL");
    if (error.issues) {
      error.issues.forEach((issue: any) => {
        console.log(`      - ${issue.path.join('.')}: ${issue.message}`);
      });
    } else {
      console.log("   Error:", error);
    }
  }

  console.log("\n4. Testing validateElasticsearchHits function:");
  try {
    const result = await validateElasticsearchHits([productionFailingMessage]);
    console.log(`   ✅ validateElasticsearchHits: PASS - ${result.length} hits validated`);
  } catch (error) {
    console.log("   ❌ validateElasticsearchHits: FAIL");
    console.log("   Error:", error);
  }

  console.log("\n5. Testing business context extraction (AFTER fix):");
  try {
    const businessContext = extractBusinessContext(productionFailingMessage._source);
    console.log("   ✅ Business context extraction: PASS");
    console.log("   Extracted context:", businessContext);
  } catch (error: any) {
    console.log("   ❌ Business context extraction: FAIL");
    console.log("   Error:", error.message);
  }

  console.log("\n6. Testing business context extraction (manual verification):");
  const tags = productionFailingMessage._source.tags || [];
  let domain = "unknown";
  let department = "unknown";
  let criticality = "medium";
  let environment = "unknown";
  let hasCriticalityTag = false;

  for (const tag of tags) {
    if (tag.startsWith("domain:")) {
      domain = tag.replace("domain:", "").trim();
    } else if (tag.startsWith("department:")) {
      department = tag.replace("department:", "").trim();
    } else if (tag.startsWith("criticality:")) {
      const crit = tag.replace("criticality:", "").trim().toLowerCase();
      if (crit === "high" || crit === "medium" || crit === "low") {
        criticality = crit;
        hasCriticalityTag = true;
      }
    } else if (tag.startsWith("environment:")) {
      environment = tag.replace("environment:", "").trim();
    }
  }

  console.log("   Extracted values:");
  console.log(`     - domain: ${domain}`);
  console.log(`     - department: ${department}`);
  console.log(`     - criticality: ${criticality}`);
  console.log(`     - environment: ${environment}`);
  console.log(`     - hasCriticalityTag: ${hasCriticalityTag}`);

  const missingFields: string[] = [];
  if (domain === "unknown") missingFields.push("domain");
  if (department === "unknown") missingFields.push("department");
  if (!hasCriticalityTag && criticality === "medium") missingFields.push("criticality");
  if (environment === "unknown") missingFields.push("environment");

  console.log(`   Missing fields (with fix): [${missingFields.join(", ")}]`);
  console.log(`   ✅ Business context validation (with fix): ${missingFields.length === 0 ? 'PASS' : 'FAIL'}`);

  console.log("\n7. Testing tags array structure:");
  console.log("   Tags found:", tags);
  console.log("   Tags with 'criticality:':", tags.filter(tag => tag.startsWith("criticality:")));
  console.log("   Tags with 'domain:':", tags.filter(tag => tag.startsWith("domain:")));
  console.log("   Tags with 'department:':", tags.filter(tag => tag.startsWith("department:")));
  console.log("   Tags with 'environment:':", tags.filter(tag => tag.startsWith("environment:")));
}

testProductionFailures().catch(console.error);
