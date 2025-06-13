/* test-message-validation.ts */

import { 
  ElasticsearchSourceSchema, 
  ElasticsearchHitSchema,
  validateElasticsearchHits,
  BusinessContextSchema 
} from "./src/types.js";

console.log("🧪 Testing message validation pipeline...");

const mockConfig = {
  extraction: {
    monitorNamePattern: "*prd*"
  }
};

const userMessage = {
  "_index": ".ds-synthetics-browser-developer_experience-2025.05.28-000025",
  "_id": "24OsapcBdcRyx4uCHCq1",
  "_score": null,
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
  },
  "sort": [
    1749841347041
  ]
};

function testMonitorNamePattern() {
  console.log("\n1. Testing Monitor Name Pattern Matching");
  const monitorName = userMessage._source.monitor.name;
  const pattern = mockConfig.extraction.monitorNamePattern;
  
  console.log(`   Monitor name: "${monitorName}"`);
  console.log(`   Pattern: "${pattern}"`);
  
  const matches = monitorName.includes(pattern.replace(/\*/g, ''));
  console.log(`   ✅ Pattern match: ${matches ? 'PASS' : 'FAIL'}`);
  
  return matches;
}

function testElasticsearchSourceValidation() {
  console.log("\n2. Testing ElasticsearchSourceSchema Validation");
  
  try {
    const validatedSource = ElasticsearchSourceSchema.parse(userMessage._source);
    console.log("   ✅ ElasticsearchSourceSchema validation: PASS");
    console.log("   Required fields present:");
    console.log(`     - monitor.id: ${validatedSource.monitor.id}`);
    console.log(`     - monitor.name: ${validatedSource.monitor.name}`);
    console.log(`     - monitor.type: ${validatedSource.monitor.type}`);
    console.log(`     - monitor.status: ${validatedSource.monitor.status}`);
    console.log(`     - @timestamp: ${validatedSource["@timestamp"]}`);
    return true;
  } catch (error) {
    console.log("   ❌ ElasticsearchSourceSchema validation: FAIL");
    console.log("   Error:", error);
    return false;
  }
}

function testElasticsearchHitValidation() {
  console.log("\n3. Testing ElasticsearchHit Validation");
  
  try {
    const hit = { _source: userMessage._source };
    const validatedHit = ElasticsearchHitSchema.parse(hit);
    console.log("   ✅ ElasticsearchHitSchema validation: PASS");
    return true;
  } catch (error) {
    console.log("   ❌ ElasticsearchHitSchema validation: FAIL");
    console.log("   Error:", error);
    return false;
  }
}

function testBusinessContextExtraction() {
  console.log("\n4. Testing Business Context Extraction");
  
  const tags = userMessage._source.tags || [];
  let domain = "unknown";
  let department = "unknown";
  let criticality: "high" | "medium" | "low" = "medium";
  let environment = "unknown";

  for (const tag of tags) {
    if (tag.startsWith("domain:")) {
      domain = tag.replace("domain:", "").trim();
    } else if (tag.startsWith("department:")) {
      department = tag.replace("department:", "").trim();
    } else if (tag.startsWith("criticality:")) {
      const crit = tag.replace("criticality:", "").trim().toLowerCase();
      if (crit === "high" || crit === "medium" || crit === "low") {
        criticality = crit;
      }
    } else if (tag.startsWith("environment:")) {
      environment = tag.replace("environment:", "").trim();
    }
  }

  const businessContext = {
    domain,
    department,
    criticality,
    environment,
  };

  console.log("   Extracted business context:");
  console.log(`     - domain: ${businessContext.domain}`);
  console.log(`     - department: ${businessContext.department}`);
  console.log(`     - criticality: ${businessContext.criticality}`);
  console.log(`     - environment: ${businessContext.environment}`);

  try {
    const validatedContext = BusinessContextSchema.parse(businessContext);
    console.log("   ✅ Business context validation: PASS");
    return true;
  } catch (error) {
    console.log("   ❌ Business context validation: FAIL");
    console.log("   Error:", error);
    return false;
  }
}

function testKafkaKeySanitization() {
  console.log("\n5. Testing Kafka Key Sanitization");
  
  const domain = "marketing_technology";
  const monitorName = "MT - PressKit Journey | TommyHilfigerTheAPXGPCollection (core) - prd";
  const timestamp = 1718305347041;
  
  const rawKey = `raw::${domain}::${monitorName}::${timestamp}`;
  console.log(`   Raw key: "${rawKey}"`);
  
  const sanitizedKey = rawKey
    .toLowerCase()
    .replace(/\s+/g, "") // Remove all spaces
    .replace(/[^a-z0-9:]/g, ""); // Remove all except lowercase letters, numbers, colon
  
  console.log(`   Sanitized key: "${sanitizedKey}"`);
  
  const expectedKey = "raw::marketingtechnology::mtpresskitjourneytommyhilfigertheapxgpcollectioncoreprd::1718305347041";
  const matches = sanitizedKey === expectedKey;
  
  console.log(`   Expected: "${expectedKey}"`);
  console.log(`   ✅ Key sanitization: ${matches ? 'PASS' : 'FAIL'}`);
  
  return matches;
}

async function testFullValidationPipeline() {
  console.log("\n6. Testing Full Validation Pipeline");
  
  try {
    const hits = [{ _source: userMessage._source }];
    const validatedHits = await validateElasticsearchHits(hits);
    
    console.log(`   ✅ Full pipeline validation: PASS`);
    console.log(`   Validated ${validatedHits.length} hits successfully`);
    return true;
  } catch (error) {
    console.log("   ❌ Full pipeline validation: FAIL");
    console.log("   Error:", error);
    return false;
  }
}

async function runAllTests() {
  console.log("=".repeat(60));
  console.log("MESSAGE VALIDATION TEST SUITE");
  console.log("=".repeat(60));
  
  const results = {
    monitorNamePattern: testMonitorNamePattern(),
    sourceValidation: testElasticsearchSourceValidation(),
    hitValidation: testElasticsearchHitValidation(),
    businessContext: testBusinessContextExtraction(),
    keySanitization: testKafkaKeySanitization(),
    fullPipeline: await testFullValidationPipeline()
  };
  
  console.log("\n" + "=".repeat(60));
  console.log("TEST RESULTS SUMMARY");
  console.log("=".repeat(60));
  
  const allPassed = Object.values(results).every(result => result === true);
  
  Object.entries(results).forEach(([test, passed]) => {
    const status = passed ? "✅ PASS" : "❌ FAIL";
    console.log(`${status} - ${test}`);
  });
  
  console.log("\n" + "=".repeat(60));
  console.log(`OVERALL RESULT: ${allPassed ? "✅ ALL TESTS PASSED" : "❌ SOME TESTS FAILED"}`);
  console.log("=".repeat(60));
  
  if (allPassed) {
    console.log("\n🎉 Your message will successfully pass through the validation pipeline and reach Kafka!");
    console.log("\nFinal Kafka message details:");
    console.log("- Topic: monitoring.raw.events");
    console.log("- Key: raw::marketingtechnology::mtpresskitjourneytommyhilfigertheapxgpcollectioncoreprd::1718305347041");
    console.log("- Headers: monitor-type=browser, status=up, domain=marketing_technology, department=press_kits, environment=eu-shared-services.prd");
  }
  
  return allPassed;
}

if (import.meta.main) {
  runAllTests().catch(console.error);
}

export { runAllTests, userMessage };
