import { z } from "zod";


export const BusinessContextSchema = z.object({
  domain: z.string(),
  department: z.string(),
  criticality: z.enum(["high", "medium", "low"]),
  environment: z.string(),
});

export type BusinessContext = z.infer<typeof BusinessContextSchema>;


export const ServiceInfoSchema = z.object({
  name: z.string(),
  endpoint: z.string(),
});

export type ServiceInfo = z.infer<typeof ServiceInfoSchema>;


export const HttpBodySchema = z.object({
  bytes: z.number().optional(),
  content: z.any().optional(),
}).optional();

export const HttpInfoSchema = z.object({
  statusCode: z.number().optional(),
  responseTime: z.number().optional(),
  body: HttpBodySchema,
}).optional();

export const TlsInfoSchema = z.object({
  established: z.boolean(),
  version: z.string().optional(),
}).optional();

export const AgentInfoSchema = z.object({
  name: z.string(),
  id: z.string(),
  type: z.string(),
  version: z.string(),
}).optional();

export const ObserverInfoSchema = z.object({
  name: z.string(),
  geo: z.string().optional(),
}).optional();

export const MetaInfoSchema = z.object({
  space_id: z.string(),
}).optional();

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
  http: HttpInfoSchema,
  tls: TlsInfoSchema,
  agent: AgentInfoSchema,
  observer: ObserverInfoSchema,
  meta: MetaInfoSchema,
});

export type MonitorInfo = z.infer<typeof MonitorInfoSchema>;


export const ElasticsearchMonitorSchema = z.object({
  id: z.string(),
  name: z.string(),
  type: z.string(),
  status: z.string(),
  duration: z.object({
    us: z.number(),
  }).optional(),
});

export const ElasticsearchUrlSchema = z.object({
  full: z.string().optional(),
  domain: z.string().optional(),
  path: z.string().optional(),
}).optional();

export const ElasticsearchHttpResponseSchema = z.object({
  status_code: z.number(),
  body: z.object({
    bytes: z.number().optional(),
    content: z.any().optional(),
  }).optional(),
}).optional();

export const ElasticsearchHttpSchema = z.object({
  response: ElasticsearchHttpResponseSchema,
  rtt: z.object({
    total: z.object({
      us: z.number(),
    }).optional(),
  }).optional(),
}).optional();

export const ElasticsearchTlsSchema = z.object({
  established: z.boolean(),
  version: z.string().optional(),
}).optional();

export const ElasticsearchAgentSchema = z.object({
  name: z.string(),
  id: z.string(),
  type: z.string(),
  ephemeral_id: z.string(),
  version: z.string(),
}).optional();

export const ElasticsearchObserverSchema = z.object({
  geo: z.object({
    name: z.string(),
  }).optional(),
  name: z.string(),
}).optional();

export const ElasticsearchMetaSchema = z.object({
  space_id: z.string(),
}).optional();

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

export type ElasticsearchHit = z.infer<typeof ElasticsearchHitSchema>;


export const SearchResponseSchema = z.object({
  took: z.number(),
  timed_out: z.boolean(),
  _shards: z.object({
    total: z.number(),
    successful: z.number(),
    skipped: z.number(),
    failed: z.number(),
  }),
  hits: z.object({
    total: z.object({
      value: z.number(),
      relation: z.string(),
    }),
    max_score: z.number().nullable(),
    hits: z.array(z.object({
      _index: z.string(),
      _id: z.string(),
      _score: z.number().nullable(),
      _source: z.any(),
      sort: z.array(z.number()).optional(),
    })),
  }),
});

export type SearchResponse<T> = z.infer<typeof SearchResponseSchema>;


export function validateElasticsearchHits(data: unknown[]): ElasticsearchHit[] {
  return data.map(hit => ElasticsearchHitSchema.parse(hit));
}

export function validateMonitorInfo(data: unknown[]): MonitorInfo[] {
  return data.map(monitor => MonitorInfoSchema.parse(monitor));
}

export function validateBusinessContext(data: unknown): BusinessContext {
  return BusinessContextSchema.parse(data);
}

export function validateServiceInfo(data: unknown): ServiceInfo {
  return ServiceInfoSchema.parse(data);
}
