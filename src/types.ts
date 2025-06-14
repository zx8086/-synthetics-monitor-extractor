/* src/types.ts */

import { z } from "zod";
import { writeFile } from "fs/promises";
import { join } from "path";
import fs from "fs";

export interface BusinessContext {
  domain: string;
  department: string;
  criticality: "high" | "medium" | "low";
  environment: string;
}

export interface MonitorInfo {
  id: string;
  name: string;
  type: string;
  url: {
    scheme?: string;
    domain?: string;
    port?: string | number;
    path?: string;
    full?: string;
  };
  timestamp: string;
  status: string;
  duration: number;
  dataset: string;
  businessContext: BusinessContext;
  tags: string[];
  checkGroup: string;
  fleetManaged: boolean;
  origin: string;
  project: {
    id: string;
    name: string;
  };
  timespan: {
    gte: string;
    lt: string;
  };
  agent: {
    id: string;
    ephemeral_id?: string;
    name?: string;
    type?: string;
    version?: string;
  };
  observer: {
    geo: Record<string, any>;
    hostname?: string;
    ip?: string;
    name?: string;
    type?: string;
  };
  state: Record<string, any>;
  summary: Record<string, any>;
  meta: Record<string, any>;
  event: Record<string, any>;
  ecs: Record<string, any>;
  dataStream: Record<string, any>;
  configId?: string;
  tls?: {
    established?: boolean;
    version?: string;
    cipher?: string;
    certificate_not_valid_before?: string;
    certificate_not_valid_after?: string;
    server?: {
      x509?: {
        not_after?: string;
        not_before?: string;
        public_key_exponent?: number;
        subject?: {
          distinguished_name?: string;
          common_name?: string;
        };
        issuer?: {
          distinguished_name?: string;
          common_name?: string;
        };
        public_key_algorithm?: string;
        signature_algorithm?: string;
        public_key_size?: number;
        serial_number?: string;
      };
      hash?: {
        sha1?: string;
        sha256?: string;
      };
    };
    version_protocol?: string;
  };
  http?: {
    rtt?: {
      total?: {
        us: number;
      };
    };
    response?: {
      status_code?: number;
      time?: { us: number };
      body?: {
        bytes?: number;
        content?: string;
        hash?: string;
      };
      headers?: Record<string, string>;
      mime_type?: string;
    };
    tls?: {
      established?: boolean;
      version?: string;
      cipher?: string;
      certificate_not_valid_before?: string;
      certificate_not_valid_after?: string;
      server?: {
        x509?: {
          not_after?: string;
          not_before?: string;
          public_key_exponent?: number;
          subject?: {
            distinguished_name?: string;
            common_name?: string;
          };
          issuer?: {
            distinguished_name?: string;
            common_name?: string;
          };
          public_key_algorithm?: string;
          signature_algorithm?: string;
          public_key_size?: number;
          serial_number?: string;
        };
        hash?: {
          sha1?: string;
          sha256?: string;
        };
      };
      version_protocol?: string;
    };
    state?: Record<string, any>;
    event?: Record<string, any>;
  };
  tcp?: {
    rtt?: {
      connect?: {
        us: number;
      };
    };
    ip?: string;
    port?: number;
    url?: {
      scheme?: string;
      domain?: string;
      port?: string | number;
      full?: string;
    };
  };
  icmp?: {
    rtt?: {
      us: number;
    };
    requests?: number;
    ip?: string;
    url?: {
      scheme?: string;
      domain?: string;
      port?: string | number;
      full?: string;
    };
  };
  browser?: Record<string, any>;
}

export interface ElasticsearchHit {
  _source: SourceDocument;
}

export interface SearchResponse<T> {
  took: number;
  timed_out: boolean;
  _shards: {
    total: number;
    successful: number;
    skipped: number;
    failed: number;
  };
  hits: {
    total: {
      value: number;
      relation: string;
    };
    max_score: number | null;
    hits: Array<{
      _index: string;
      _id: string;
      _score: number | null;
      _source: T;
      sort?: number[];
    }>;
  };
}

// Zod schemas for validation
export const BusinessContextSchema = z.object({
  domain: z.string().min(1).refine(val => val !== "unknown", {
    message: "Domain cannot be 'unknown'"
  }),
  department: z.string().min(1).refine(val => val !== "unknown", {
    message: "Department cannot be 'unknown'"
  }),
  criticality: z.enum(["high", "medium", "low"]),
  environment: z.string().min(1).refine(val => val !== "unknown", {
    message: "Environment cannot be 'unknown'"
  }),
});

export const MonitorInfoSchema = z.object({
  id: z.string(),
  name: z.string(),
  type: z.string(),
  url: z.string().optional(),
  timestamp: z.string(),
  status: z.string(),
  duration: z.number(),
  dataset: z.string(),
  businessContext: BusinessContextSchema,
  tags: z.array(z.string()),
  checkGroup: z.string().optional(),
  fleetManaged: z.boolean().optional(),
  origin: z.string().optional(),
  project: z.object({
    name: z.string(),
    id: z.string()
  }).optional(),
  timespan: z.object({
    lt: z.string(),
    gte: z.string()
  }).optional(),
  http: z.object({
    statusCode: z.number().optional(),
    responseTime: z.number().optional(),
    body: z.object({
      bytes: z.number().optional(),
      content: z.string().optional(),
      hash: z.string().optional()
    }).optional(),
    headers: z.record(z.string()).optional(),
    mimeType: z.string().optional(),
    url: z.object({
      scheme: z.string().optional(),
      domain: z.string().optional(),
      port: z.number().optional(),
      path: z.string().optional(),
      full: z.string().optional(),
    }).optional(),
    rtt: z.object({
      total: z.object({
        us: z.number().optional(),
      }).optional(),
    }).optional(),
    response: z.object({
      status_code: z.number().optional(),
      time: z.object({
        us: z.number().optional(),
      }).optional(),
      body: z.object({
        bytes: z.number().optional(),
        content: z.string().optional(),
        hash: z.string().optional(),
      }).optional(),
      headers: z.record(z.string()).optional(),
      mime_type: z.string().optional(),
    }).optional(),
    tls: z.object({
      established: z.boolean().optional(),
      version: z.string().optional(),
      cipher: z.string().optional(),
      certificate_not_valid_before: z.string().optional(),
      certificate_not_valid_after: z.string().optional(),
      server: z.object({
        x509: z.object({
          not_after: z.string().optional(),
          not_before: z.string().optional(),
          public_key_exponent: z.number().optional(),
          subject: z.object({
            distinguished_name: z.string().optional(),
            common_name: z.string().optional(),
          }).optional(),
          issuer: z.object({
            distinguished_name: z.string().optional(),
            common_name: z.string().optional(),
          }).optional(),
          public_key_algorithm: z.string().optional(),
          signature_algorithm: z.string().optional(),
          public_key_size: z.number().optional(),
          serial_number: z.string().optional(),
        }).optional(),
        hash: z.object({
          sha1: z.string().optional(),
          sha256: z.string().optional(),
        }).optional(),
      }).optional(),
      version_protocol: z.string().optional(),
    }).optional(),
    state: z.object({
      duration_ms: z.string().optional(),
      checks: z.number().optional(),
      up: z.number().optional(),
      down: z.number().optional(),
      started_at: z.string().optional(),
      id: z.string().optional(),
      ends: z.string().nullable().optional(),
      flap_history: z.array(z.any()).optional(),
      status: z.string().optional(),
    }).optional(),
    event: z.object({
      agent_id_status: z.string().optional(),
      ingested: z.string().optional(),
      type: z.string().optional(),
      dataset: z.string().optional(),
    }).optional(),
  }).optional(),
  tcp: z.object({
    connectTime: z.number().optional(),
    ip: z.string().optional(),
    port: z.number().optional(),
    url: z.object({
      scheme: z.string().optional(),
      domain: z.string().optional(),
      port: z.number().optional(),
      full: z.string().optional(),
    }).optional(),
    rtt: z.object({
      connect: z.object({
        us: z.number().optional(),
      }).optional(),
    }).optional(),
  }).optional(),
  icmp: z.object({
    rtt: z.number().optional(),
    requests: z.number().optional(),
    ip: z.string().optional(),
    url: z.object({
      scheme: z.string().optional(),
      domain: z.string().optional(),
      port: z.number().optional(),
      full: z.string().optional(),
    }).optional(),
    rtt: z.object({
      us: z.number().optional(),
    }).optional(),
  }).optional(),
  browser: z.object({
    synthetics: z.object({
      type: z.string().optional(),
      journey: z.object({
        name: z.string().optional(),
        id: z.string().optional(),
      }).optional(),
      step: z.object({
        name: z.string().optional(),
        index: z.number().optional(),
        status: z.string().optional(),
      }).optional(),
      error: z.object({
        name: z.string().optional(),
        message: z.string().optional(),
        stack: z.string().optional(),
      }).optional(),
    }).optional(),
    journey: z.object({
      name: z.string().optional(),
      id: z.string().optional(),
    }).optional(),
    step: z.object({
      name: z.string().optional(),
      index: z.number().optional(),
      status: z.string().optional(),
    }).optional(),
    error: z.object({
      name: z.string().optional(),
      message: z.string().optional(),
      stack: z.string().optional(),
    }).optional(),
  }).optional(),
  agent: z.object({
    id: z.string(),
    ephemeral_id: z.string().optional(),
    name: z.string().optional(),
    type: z.string().optional(),
    version: z.string().optional(),
  }),
  observer: z.object({
    geo: z.record(z.any()),
    hostname: z.string().optional(),
    ip: z.string().optional(),
    name: z.string().optional(),
    type: z.string().optional(),
  }),
  state: z.record(z.any()),
  summary: z.record(z.any()),
  meta: z.record(z.any()),
  event: z.record(z.any()),
  ecs: z.record(z.any()),
  dataStream: z.record(z.any()),
  configId: z.string().optional(),
  configId: z.string().optional(),
  dataStream: z.object({
    namespace: z.string().optional(),
    type: z.string().optional(),
    dataset: z.string().optional()
  }).optional()
});

export const ElasticsearchMonitorSchema = z.object({
  id: z.string(),
  name: z.string(),
  type: z.string(),
  status: z.string(),
  duration: z
    .object({
      us: z.number(),
    })
    .optional(),
});

export const ElasticsearchUrlSchema = z
  .object({
    full: z.string().optional(),
    domain: z.string().optional(),
    path: z.string().optional(),
  })
  .optional();

export const ElasticsearchHttpResponseSchema = z
  .object({
    status_code: z.number().optional(),
    body: z
      .object({
        bytes: z.number().optional(),
        content: z.any().optional(),
      })
      .optional(),
  })
  .optional();

export const ElasticsearchHttpSchema = z
  .object({
    response: ElasticsearchHttpResponseSchema,
    rtt: z
      .object({
        total: z
          .object({
            us: z.number(),
          })
          .optional(),
      })
      .optional(),
  })
  .optional();

export const ElasticsearchTlsSchema = z
  .object({
    established: z.boolean().optional(),
    version: z.string().optional(),
  })
  .optional();

export const ElasticsearchAgentSchema = z
  .object({
    name: z.string(),
    id: z.string(),
    type: z.string(),
    ephemeral_id: z.string(),
    version: z.string(),
  })
  .optional();

export const ElasticsearchObserverSchema = z
  .object({
    geo: z
      .object({
        name: z.string(),
      })
      .optional(),
    name: z.string(),
  })
  .optional();

export const ElasticsearchMetaSchema = z
  .object({
    space_id: z.string(),
  })
  .optional();

export const ElasticsearchSourceSchema = z.object({
  monitor: ElasticsearchMonitorSchema,
  url: ElasticsearchUrlSchema,
  "@timestamp": z.string(),
  tags: z.array(z.string()).optional(),
  http: ElasticsearchHttpSchema.optional(),
  tls: ElasticsearchTlsSchema.optional(),
  agent: ElasticsearchAgentSchema.optional(),
  observer: ElasticsearchObserverSchema.optional(),
  meta: ElasticsearchMetaSchema.optional(),
  data_stream: z.object({
    dataset: z.string(),
    type: z.string().optional(),
    namespace: z.string().optional()
  }).optional(),
  event: z.object({
    agent_id_status: z.string().optional(),
    ingested: z.string().optional(),
    type: z.string().optional(),
    dataset: z.string().optional()
  }).optional(),
  ecs: z.object({
    version: z.string().optional()
  }).optional(),
  config_id: z.string().optional()
});

export const ElasticsearchHitSchema = z.object({
  _source: ElasticsearchSourceSchema,
});

// Buffer to store invalid records during an extraction cycle
let invalidRecordsBuffer: Array<{
  timestamp: string;
  type: string;
  monitorName?: string;
  errors: Array<{ message: string }>;
}> = [];

// Helper function to write invalid records to file
export function writeInvalidRecords(type: string, errors: Array<{ message: string }>, monitorName?: string): void {
  const timestamp = new Date().toISOString();
  const invalidData = {
    timestamp,
    type,
    monitorName,
    errors
  };

  try {
    // Add to buffer
    invalidRecordsBuffer.push(invalidData);
    
    // Ensure src directory exists
    fs.mkdirSync('src', { recursive: true });
    
    // Write current buffer to file, overwriting the previous content
    const filePath = 'src/invalid.json';
    console.log(`Writing ${invalidRecordsBuffer.length} invalid records to ${filePath}`);
    
    fs.writeFileSync(filePath, JSON.stringify(invalidRecordsBuffer, null, 2), 'utf8');
    console.log(`Successfully wrote invalid records to ${filePath}`);
  } catch (error) {
    console.error('Error writing invalid records:', error);
    if (error instanceof Error) {
      console.error('Error details:', {
        message: error.message,
        stack: error.stack,
        name: error.name
      });
    }
  }
}

// Function to clear the invalid records buffer at the start of a new extraction
export function clearInvalidRecordsBuffer(): void {
  console.log('Clearing invalid records buffer');
  invalidRecordsBuffer = [];
}

// Validation functions
export async function validateElasticsearchHits(data: unknown[]): Promise<ElasticsearchHit[]> {
  const validHits: ElasticsearchHit[] = [];
  const errorsByMonitor: Record<string, Set<string>> = {};

  for (const hit of data) {
    try {
      const validatedHit = ElasticsearchHitSchema.parse(hit) as ElasticsearchHit;
      validHits.push(validatedHit);
    } catch (error) {
      // Try to get monitor name even from invalid hit
      const monitorName = (hit as any)?._source?.monitor?.name;
      if (!monitorName) continue; // Skip if we can't get a monitor name
      
      if (!errorsByMonitor[monitorName]) {
        errorsByMonitor[monitorName] = new Set();
      }

      if (error instanceof z.ZodError) {
        error.issues.forEach(issue => {
          errorsByMonitor[monitorName]?.add(`${issue.path.join('.')}: ${issue.message}`);
        });
      } else {
        errorsByMonitor[monitorName]?.add(String(error));
      }
    }
  }

  // Write errors for each monitor
  for (const [monitorName, errorSet] of Object.entries(errorsByMonitor)) {
    if (errorSet.size > 0) {
      const errors = Array.from(errorSet).map(message => ({ message }));
      writeInvalidRecords('elasticsearch_hits', errors, monitorName);
    }
  }

  return validHits;
}

export async function validateMonitorInfo(data: unknown[]): Promise<MonitorInfo[]> {
  const validMonitors: MonitorInfo[] = [];
  const errors: unknown[] = [];
  
  for (const info of data) {
    try {
      const validatedInfo = MonitorInfoSchema.parse(info);
      validMonitors.push(validatedInfo);
    } catch (error) {
      console.warn("Skipping invalid monitor info:", error);
      errors.push(error);
      continue;
    }
  }

  if (errors.length > 0) {
    writeInvalidRecords('monitor_info', errors.map(error => ({ message: error instanceof Error ? error.message : String(error) })), 'unknown');
  }

  return validMonitors;
}

export async function validateBusinessContext(data: unknown): Promise<BusinessContext> {
  try {
    return BusinessContextSchema.parse(data);
  } catch (error) {
    console.warn("Invalid business context:", error);
    writeInvalidRecords('business_context', [{ message: error instanceof Error ? error.message : String(error) }], 'unknown');
    throw error; // Re-throw to invalidate the record
  }
}

// Define Zod schemas for type-specific fields
export const HttpSchema = z.object({
  statusCode: z.number().optional(),
  responseTime: z.number().optional(),
  body: z.object({
    bytes: z.number().optional(),
    content: z.string().optional(),
    hash: z.string().optional(),
  }).optional(),
  headers: z.record(z.string()).optional(),
  mimeType: z.string().optional(),
}).optional();

export const TlsSchema = z.object({
  established: z.boolean(),
  version: z.string().optional(),
  cipher: z.string().optional(),
  certificateNotValidBefore: z.string().optional(),
  certificateNotValidAfter: z.string().optional(),
  server: z.object({
    x509: z.object({
      notAfter: z.string().optional(),
      notBefore: z.string().optional(),
      publicKeyExponent: z.number().optional(),
      subject: z.object({
        distinguishedName: z.string().optional(),
        commonName: z.string().optional(),
      }).optional(),
      issuer: z.object({
        distinguishedName: z.string().optional(),
        commonName: z.string().optional(),
      }).optional(),
      publicKeyAlgorithm: z.string().optional(),
      signatureAlgorithm: z.string().optional(),
      publicKeySize: z.number().optional(),
      serialNumber: z.string().optional(),
    }).optional(),
    hash: z.object({
      sha1: z.string().optional(),
      sha256: z.string().optional(),
    }).optional(),
  }).optional(),
}).optional();

export const TcpSchema = z.object({
  connectTime: z.number().optional(),
  ip: z.string().optional(),
  port: z.number().optional(),
  url: z.object({
    scheme: z.string().optional(),
    domain: z.string().optional(),
    port: z.number().optional(),
    full: z.string().optional(),
  }).optional(),
}).optional();

export const IcmpSchema = z.object({
  rtt: z.number().optional(),
  requests: z.number().optional(),
  ip: z.string().optional(),
  url: z.object({
    scheme: z.string().optional(),
    domain: z.string().optional(),
    port: z.number().optional(),
    full: z.string().optional(),
  }).optional(),
}).optional();

export const BrowserSchema = z.object({
  synthetics: z.object({
    type: z.string().optional(),
    journey: z.object({
      name: z.string().optional(),
      id: z.string().optional(),
    }).optional(),
    step: z.object({
      name: z.string().optional(),
      index: z.number().optional(),
      status: z.string().optional(),
    }).optional(),
    error: z.object({
      name: z.string().optional(),
      message: z.string().optional(),
      stack: z.string().optional(),
    }).optional(),
  }).optional(),
  journey: z.object({
    name: z.string().optional(),
    id: z.string().optional(),
  }).optional(),
  step: z.object({
    name: z.string().optional(),
    index: z.number().optional(),
    status: z.string().optional(),
  }).optional(),
  error: z.object({
    name: z.string().optional(),
    message: z.string().optional(),
    stack: z.string().optional(),
  }).optional(),
}).optional();

export const AgentSchema = z.object({
  name: z.string(),
  id: z.string(),
  type: z.string(),
  version: z.string(),
  ephemeralId: z.string().optional(),
}).optional();

export const ObserverSchema = z.object({
  name: z.string(),
  geo: z.string().optional(),
}).optional();

export const StateSchema = z.object({
  status: z.string(),
  durationMs: z.string().optional(),
  checks: z.number().optional(),
  up: z.number().optional(),
  down: z.number().optional(),
  startedAt: z.string().optional(),
  id: z.string().optional(),
}).optional();

export const SummarySchema = z.object({
  status: z.string(),
  up: z.number(),
  down: z.number(),
  attempt: z.number(),
  maxAttempts: z.number(),
  finalAttempt: z.boolean(),
  retryGroup: z.string().optional(),
}).optional();

export const MetaSchema = z.object({
  spaceId: z.string(),
}).optional();

export interface InvalidRecord {
  monitorName: string;
  error: string;
}

export interface SourceDocument {
  "@timestamp": string;
  monitor: {
    id: string;
    name: string;
    type: string;
    status: string;
    duration?: { us: number };
    check_group?: string;
    fleet_managed?: boolean;
    origin?: string;
    ip?: string;
    port?: number;
    timespan?: {
      gte: string;
      lt: string;
    };
    project?: {
      id: string;
      name: string;
    };
    state?: Record<string, any>;
    summary?: Record<string, any>;
  };
  agent?: {
    id: string;
    ephemeral_id?: string;
    name?: string;
    type?: string;
    version?: string;
  };
  observer?: {
    geo: Record<string, any>;
    hostname?: string;
    ip?: string;
    name?: string;
    type?: string;
  };
  http?: {
    response?: {
      status_code?: number;
      time?: { us: number };
      body?: {
        bytes?: number;
        content?: string;
        hash?: string;
      };
      headers?: Record<string, string>;
      mime_type?: string;
    };
    rtt?: {
      total?: {
        us: number;
      };
    };
    tls?: {
      established?: boolean;
      version?: string;
      cipher?: string;
      certificate_not_valid_before?: string;
      certificate_not_valid_after?: string;
      server?: {
        x509?: {
          not_after?: string;
          not_before?: string;
          public_key_exponent?: number;
          subject?: {
            distinguished_name?: string;
            common_name?: string;
          };
          issuer?: {
            distinguished_name?: string;
            common_name?: string;
          };
          public_key_algorithm?: string;
          signature_algorithm?: string;
          public_key_size?: number;
          serial_number?: string;
        };
        hash?: {
          sha1?: string;
          sha256?: string;
        };
      };
      version_protocol?: string;
    };
    state?: Record<string, any>;
    event?: Record<string, any>;
  };
  tcp?: {
    rtt?: {
      connect?: {
        us: number;
      };
    };
  };
  icmp?: {
    rtt?: {
      us: number;
    };
    requests?: number;
  };
  url?: {
    scheme?: string;
    domain?: string;
    port?: string | number;
    path?: string;
    full?: string;
  };
  tls?: {
    established?: boolean;
    version?: string;
    cipher?: string;
    certificate_not_valid_before?: string;
    certificate_not_valid_after?: string;
    server?: {
      x509?: {
        not_after?: string;
        not_before?: string;
        public_key_exponent?: number;
        subject?: {
          distinguished_name?: string;
          common_name?: string;
        };
        issuer?: {
          distinguished_name?: string;
          common_name?: string;
        };
        public_key_algorithm?: string;
        signature_algorithm?: string;
        public_key_size?: number;
        serial_number?: string;
      };
      hash?: {
        sha1?: string;
        sha256?: string;
      };
    };
    version_protocol?: string;
  };
  browser?: Record<string, any>;
  meta?: Record<string, any>;
  event?: Record<string, any>;
  ecs?: Record<string, any>;
  data_stream?: Record<string, any>;
  tags?: string[];
  config_id?: string;
}


