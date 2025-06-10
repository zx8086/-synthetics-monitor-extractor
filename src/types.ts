/* src/types.ts */

import { z } from "zod";
import { writeFile } from "fs/promises";
import { join } from "path";
import fs from "fs/promises";

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
  url?: string;
  timestamp: string;
  status: string;
  duration: number;
  businessContext: BusinessContext;
  tags: string[];
  environment: string;
  http?: {
    statusCode?: number;
    responseTime?: number;
    body?: {
      bytes?: number;
      content?: any;
    };
  };
  tls?: {
    established: boolean;
    version?: string;
  };
  agent?: {
    name: string;
    id: string;
    type: string;
    version: string;
  };
  observer?: {
    name: string;
    geo?: string;
  };
  meta?: {
    space_id: string;
  };
}

export interface ElasticsearchHit {
  _source: {
    monitor: {
      id: string;
      name: string;
      type: string;
      status: string;
      duration?: { us: number };
    };
    url?: {
      full?: string;
      domain?: string;
      path?: string;
    };
    "@timestamp": string;
    tags?: string[];
    http?: {
      response?: {
        status_code: number;
        body?: {
          bytes?: number;
          content?: any;
        };
      };
      rtt?: { total?: { us: number } };
    };
    tls?: {
      established: boolean;
      version?: string;
    };
    agent?: {
      name: string;
      id: string;
      type: string;
      ephemeral_id: string;
      version: string;
    };
    observer?: {
      geo?: {
        name: string;
      };
      name: string;
    };
    meta?: {
      space_id: string;
    };
  };
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
  businessContext: BusinessContextSchema,
  tags: z.array(z.string()),
  environment: z.string(),
  http: z
    .object({
      statusCode: z.number().optional(),
      responseTime: z.number().optional(),
      body: z
        .object({
          bytes: z.number().optional(),
          content: z.any().optional(),
        })
        .optional(),
    })
    .optional(),
  tls: z
    .object({
      established: z.boolean(),
      version: z.string().optional(),
    })
    .optional(),
  agent: z
    .object({
      name: z.string(),
      id: z.string(),
      type: z.string(),
      version: z.string(),
    })
    .optional(),
  observer: z
    .object({
      name: z.string(),
      geo: z.string().optional(),
    })
    .optional(),
  meta: z
    .object({
      space_id: z.string(),
    })
    .optional(),
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
    status_code: z.number(),
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
    established: z.boolean(),
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
  http: ElasticsearchHttpSchema,
  tls: ElasticsearchTlsSchema,
  agent: ElasticsearchAgentSchema,
  observer: ElasticsearchObserverSchema,
  meta: ElasticsearchMetaSchema,
});

export const ElasticsearchHitSchema = z.object({
  _source: ElasticsearchSourceSchema,
});

// Helper function to write invalid records to file
export async function writeInvalidRecords(type: string, errors: Array<{ message: string }>, monitorName?: string): Promise<void> {
  const timestamp = new Date().toISOString();
  const invalidData = {
    timestamp,
    monitorName,
    errors
  };

  try {
    // Read existing content if file exists
    let existingContent: any[] = [];
    try {
      const fileContent = await fs.readFile('src/invalid.json', 'utf-8');
      existingContent = JSON.parse(fileContent);
      if (!Array.isArray(existingContent)) {
        existingContent = [existingContent];
      }
    } catch (error) {
      // File doesn't exist or is empty, start with empty array
      existingContent = [];
    }

    // Append new invalid record
    existingContent.push(invalidData);

    // Write back to file
    await fs.writeFile('src/invalid.json', JSON.stringify(existingContent, null, 2));
  } catch (error) {
    console.error('Error writing invalid records:', error);
  }
}

// Validation functions
export async function validateElasticsearchHits(data: unknown[]): Promise<ElasticsearchHit[]> {
  const validHits: ElasticsearchHit[] = [];
  const errorsByMonitor: Record<string, Set<string>> = {};

  for (const hit of data) {
    try {
      const validatedHit = ElasticsearchHitSchema.parse(hit);
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
          errorsByMonitor[monitorName].add(`${issue.path.join('.')}: ${issue.message}`);
        });
      } else {
        errorsByMonitor[monitorName].add(String(error));
      }
    }
  }

  // Write errors for each monitor
  for (const [monitorName, errorSet] of Object.entries(errorsByMonitor)) {
    if (errorSet.size > 0) {
      const errors = Array.from(errorSet).map(message => ({ message }));
      await writeInvalidRecords('elasticsearch_hits', errors, monitorName);
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
    await writeInvalidRecords('monitor_info', errors.map(error => ({ message: error instanceof Error ? error.message : String(error) })), 'unknown');
  }

  return validMonitors;
}

export async function validateBusinessContext(data: unknown): Promise<BusinessContext> {
  try {
    return BusinessContextSchema.parse(data);
  } catch (error) {
    console.warn("Invalid business context:", error);
    await writeInvalidRecords('business_context', [{ message: error instanceof Error ? error.message : String(error) }], 'unknown');
    throw error; // Re-throw to invalidate the record
  }
}

