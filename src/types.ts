/* src/types.ts */

import { z } from "zod";

export interface BusinessContext {
  domain: string;
  department: string;
  criticality: "high" | "medium" | "low";
  environment: string;
}

export interface ServiceInfo {
  name: string;
  endpoint: string;
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
  service: ServiceInfo;
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
      status?: string;
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
  domain: z.string(),
  department: z.string(),
  criticality: z.enum(["high", "medium", "low"]),
  environment: z.string(),
});

export const ServiceInfoSchema = z.object({
  name: z.string(),
  endpoint: z.string(),
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
  service: ServiceInfoSchema,
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
  status: z.string().optional(),
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

// Validation functions
export function validateElasticsearchHits(data: unknown[]): ElasticsearchHit[] {
  return data.map((hit) => ElasticsearchHitSchema.parse(hit));
}

export function validateMonitorInfo(data: unknown[]): MonitorInfo[] {
  return data.map((info) => MonitorInfoSchema.parse(info));
}

export function validateBusinessContext(data: unknown): BusinessContext {
  return BusinessContextSchema.parse(data);
}

export function validateServiceInfo(data: unknown): ServiceInfo {
  return ServiceInfoSchema.parse(data);
}
