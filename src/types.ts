/* src/types.ts */

import { z } from "zod";
import { writeFile } from "fs/promises";
import { join } from "path";
import fs from "fs";
import { storeInvalidRecord } from "./database.js";
import { log, warn } from "./utils/logger.js";

export interface BusinessContext {
	domain: string;
	department: string;
	criticality: "high" | "medium" | "low";
	environment: string;
}

export interface MonitorInfo {
	_source?: SourceDocument;
	id: string;
	name: string;
	type: string;
	url: {
		scheme?: string;
		domain?: string;
		port?: number;
		path?: string;
		full?: string;
	};
	timestamp: string;
	businessContext: BusinessContext;
	tags: string[];
	monitor: {
		id: string;
		name: string;
		type: string;
		status: string;
		duration?: {
			us: number;
		};
		ip?: string;
		origin?: string;
		timespan?: {
			lt: string;
			gte: string;
		};
		fleet_managed?: boolean;
		check_group?: string;
		project?: {
			name: string;
			id: string;
		};
	};
	http?: {
		response?: {
			status_code: number;
			mime_type?: string;
			headers?: Record<string, string>;
			body?: {
				bytes: number;
				content: string;
				hash: string;
			};
		};
		rtt?: {
			total?: {
				us: number;
			};
		};
		state?: string;
	};
	tls?: {
		established: boolean;
		version: string;
		cipher: string;
		certificate_not_valid_before: string;
		certificate_not_valid_after: string;
		version_protocol: string;
		server?: {
			x509?: {
				not_after: string;
				not_before: string;
				public_key_exponent: number;
				public_key_algorithm: string;
				public_key_size: number;
				signature_algorithm: string;
				serial_number: string;
				subject: {
					distinguished_name: string;
					common_name: string;
				};
				issuer: {
					distinguished_name: string;
					common_name: string;
				};
			};
			hash?: {
				sha1: string;
				sha256: string;
			};
		};
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
	browser?: {
		name?: string;
		version?: string;
		os?: {
			name?: string;
			version?: string;
			full?: string;
		};
		device?: {
			name?: string;
			type?: string;
			mobile?: boolean;
		};
		viewport?: {
			width?: number;
			height?: number;
		};
		user_agent?: string;
	};
	synthetics?: {
		type?: string;
	};
	summary?: {
		retry_group: string;
		max_attempts: number;
		up: number;
		down: number;
		attempt: number;
		final_attempt: boolean;
		status: string;
	};
	state?: {
		duration_ms: string;
		checks: number;
		ends: null;
		started_at: string;
		up: number;
		id: string;
		down: number;
		flap_history: any[];
		status: string;
	};
	event?: {
		agent_id_status?: string;
		ingested?: string;
		type?: string;
		dataset?: string;
	};
	data_stream?: {
		namespace: string;
		type: string;
		dataset: string;
	};
	ecs?: {
		version: string;
	};
	config_id?: string;
	agent?: {
		name: string;
		id: string;
		type: string;
		version: string;
		ephemeral_id: string;
	};
	observer?: {
		name: string;
		geo?: {
			name: string;
		};
	};
	meta?: {
		space_id: string;
	};
	ip?: string;
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

// Simple business context validation - preserve original format
export const BusinessContextSchema = z.object({
	domain: z.string(),
	department: z.string(),
	criticality: z.enum(["high", "medium", "low"]),
	environment: z.string(),
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
	environment: z.string(),
	monitor: z
		.object({
			id: z.string(),
			name: z.string(),
			type: z.string(),
			status: z.string(),
			duration: z
				.object({
					us: z.number().optional(),
				})
				.optional(),
			ip: z.string().optional(),
			origin: z.string().optional(),
			timespan: z
				.object({
					lt: z.string().optional(),
					gte: z.string().optional(),
				})
				.optional(),
			fleet_managed: z.boolean().optional(),
			check_group: z.string().optional(),
			project: z
				.object({
					id: z.string().optional(),
					name: z.string().optional(),
				})
				.optional(),
		})
		.optional(),
	http: z
		.object({
			statusCode: z.number().optional(),
			responseTime: z.number().optional(),
			response: z
				.object({
					status_code: z.number().optional(),
					mime_type: z.string().optional(),
					headers: z.record(z.string()).optional(),
					body: z
						.object({
							bytes: z.number().optional(),
							content: z.any().optional(),
							hash: z.string().optional(),
						})
						.optional(),
				})
				.optional(),
			rtt: z
				.object({
					total: z
						.object({
							us: z.number().optional(),
						})
						.optional(),
				})
				.optional(),
			state: z
				.object({
					duration_ms: z.string().optional(),
					checks: z.number().optional(),
					ends: z.any().optional(),
					started_at: z.string().optional(),
					up: z.number().optional(),
					id: z.string().optional(),
					down: z.number().optional(),
					flap_history: z.array(z.any()).optional(),
					status: z.string().optional(),
				})
				.optional(),
		})
		.optional(),
	tls: z
		.object({
			established: z.boolean().optional(),
			version: z.string().optional(),
			cipher: z.string().optional(),
			certificate_not_valid_before: z.string().optional(),
			certificate_not_valid_after: z.string().optional(),
			version_protocol: z.string().optional(),
			server: z
				.object({
					x509: z
						.object({
							not_after: z.string().optional(),
							not_before: z.string().optional(),
							public_key_exponent: z.number().optional(),
							public_key_algorithm: z.string().optional(),
							public_key_size: z.number().optional(),
							signature_algorithm: z.string().optional(),
							serial_number: z.string().optional(),
							subject: z
								.object({
									distinguished_name: z.string().optional(),
									common_name: z.string().optional(),
								})
								.optional(),
							issuer: z
								.object({
									distinguished_name: z.string().optional(),
									common_name: z.string().optional(),
								})
								.optional(),
						})
						.optional(),
					hash: z
						.object({
							sha1: z.string().optional(),
							sha256: z.string().optional(),
						})
						.optional(),
				})
				.optional(),
		})
		.optional(),
	tcp: z
		.object({
			rtt: z
				.object({
					connect: z
						.object({
							us: z.number().optional(),
						})
						.optional(),
				})
				.optional(),
		})
		.optional(),
	icmp: z
		.object({
			rtt: z
				.object({
					us: z.number().optional(),
				})
				.optional(),
			requests: z.number().optional(),
		})
		.optional(),
	synthetics: z
		.object({
			type: z.string().optional(),
		})
		.optional(),
	summary: z
		.object({
			retry_group: z.string().optional(),
			max_attempts: z.number().optional(),
			up: z.number().optional(),
			down: z.number().optional(),
			attempt: z.number().optional(),
			final_attempt: z.boolean().optional(),
			status: z.string().optional(),
		})
		.optional(),
	state: z
		.object({
			duration_ms: z.string().optional(),
			checks: z.number().optional(),
			ends: z.any().optional(),
			started_at: z.string().optional(),
			up: z.number().optional(),
			id: z.string().optional(),
			down: z.number().optional(),
			flap_history: z.array(z.any()).optional(),
			status: z.string().optional(),
		})
		.optional(),
	event: z
		.object({
			agent_id_status: z.string().optional(),
			ingested: z.string().optional(),
			type: z.string().optional(),
			dataset: z.string().optional(),
		})
		.optional(),
	data_stream: z
		.object({
			namespace: z.string().optional(),
			type: z.string().optional(),
			dataset: z.string().optional(),
		})
		.optional(),
	ecs: z
		.object({
			version: z.string().optional(),
		})
		.optional(),
	config_id: z.string().optional(),
	agent: z
		.object({
			name: z.string(),
			id: z.string(),
			type: z.string(),
			version: z.string(),
			ephemeral_id: z.string().optional(),
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
	project: z
		.object({
			name: z.string().optional(),
			id: z.string().optional(),
		})
		.optional(),
	timespan: z
		.object({
			lt: z.string().optional(),
			gte: z.string().optional(),
		})
		.optional(),
	check_group: z.string().optional(),
	fleet_managed: z.boolean().optional(),
	origin: z.string().optional(),
	ip: z.string().optional(),
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
	monitor: z.object({
		id: z.string(),
		name: z.string(),
		type: z.string(),
		status: z.string().optional(),
		duration: z.object({ us: z.number() }).optional(),
		origin: z.string().optional(),
		project: z
			.object({
				name: z.string().optional(),
				id: z.string().optional(),
			})
			.optional(),
		timespan: z
			.object({
				lt: z.string().optional(),
				gte: z.string().optional(),
			})
			.optional(),
		check_group: z.string().optional(),
		fleet_managed: z.boolean().optional(),
		ip: z.string().optional(),
	}),
	url: z
		.object({
			full: z.string().optional(),
			domain: z.string().optional(),
			path: z.string().optional(),
			scheme: z.string().optional(),
			port: z.number().optional(),
		})
		.optional(),
	"@timestamp": z.string(),
	tags: z.array(z.string()).optional(),
	http: z
		.object({
			response: z
				.object({
					status_code: z.number().optional(),
					mime_type: z.string().optional(),
					headers: z.record(z.string()).optional(),
					body: z
						.object({
							bytes: z.number().optional(),
							content: z.any().optional(),
							hash: z.string().optional(),
						})
						.optional(),
				})
				.optional(),
			rtt: z
				.object({
					total: z.object({ us: z.number().optional() }).optional(),
				})
				.optional(),
			state: z
				.object({
					duration_ms: z.string().optional(),
					checks: z.number().optional(),
					ends: z.any().optional(),
					started_at: z.string().optional(),
					up: z.number().optional(),
					id: z.string().optional(),
					down: z.number().optional(),
					flap_history: z.array(z.any()).optional(),
					status: z.string().optional(),
				})
				.optional(),
		})
		.optional(),
	tls: z
		.object({
			established: z.boolean().optional(),
			version: z.string().optional(),
			cipher: z.string().optional(),
			certificate_not_valid_before: z.string().optional(),
			certificate_not_valid_after: z.string().optional(),
			version_protocol: z.string().optional(),
			server: z
				.object({
					x509: z
						.object({
							not_after: z.string().optional(),
							not_before: z.string().optional(),
							public_key_exponent: z.number().optional(),
							public_key_algorithm: z.string().optional(),
							public_key_size: z.number().optional(),
							signature_algorithm: z.string().optional(),
							serial_number: z.string().optional(),
							subject: z
								.object({
									distinguished_name: z.string().optional(),
									common_name: z.string().optional(),
								})
								.optional(),
							issuer: z
								.object({
									distinguished_name: z.string().optional(),
									common_name: z.string().optional(),
								})
								.optional(),
						})
						.optional(),
					hash: z
						.object({
							sha1: z.string().optional(),
							sha256: z.string().optional(),
						})
						.optional(),
				})
				.optional(),
		})
		.optional(),
	tcp: z
		.object({
			rtt: z
				.object({
					connect: z
						.object({
							us: z.number().optional(),
						})
						.optional(),
				})
				.optional(),
		})
		.optional(),
	icmp: z
		.object({
			rtt: z
				.object({
					us: z.number().optional(),
				})
				.optional(),
			requests: z.number().optional(),
		})
		.optional(),
	synthetics: z
		.object({
			type: z.string().optional(),
		})
		.optional(),
	summary: z
		.object({
			retry_group: z.string().optional(),
			max_attempts: z.number().optional(),
			up: z.number().optional(),
			down: z.number().optional(),
			attempt: z.number().optional(),
			final_attempt: z.boolean().optional(),
			status: z.string().optional(),
		})
		.optional(),
	state: z
		.object({
			duration_ms: z.string().optional(),
			checks: z.number().optional(),
			ends: z.any().optional(),
			started_at: z.string().optional(),
			up: z.number().optional(),
			id: z.string().optional(),
			down: z.number().optional(),
			flap_history: z.array(z.any()).optional(),
			status: z.string().optional(),
		})
		.optional(),
	event: z
		.object({
			agent_id_status: z.string().optional(),
			ingested: z.string().optional(),
			type: z.string().optional(),
			dataset: z.string().optional(),
		})
		.optional(),
	data_stream: z
		.object({
			namespace: z.string().optional(),
			type: z.string().optional(),
			dataset: z.string().optional(),
		})
		.optional(),
	ecs: z
		.object({
			version: z.string().optional(),
		})
		.optional(),
	config_id: z.string().optional(),
	agent: z
		.object({
			name: z.string(),
			id: z.string(),
			type: z.string(),
			version: z.string(),
			ephemeral_id: z.string().optional(),
		})
		.optional(),
	observer: z
		.object({
			geo: z
				.object({
					name: z.string().optional(),
				})
				.optional(),
			name: z.string().optional(),
		})
		.optional(),
	meta: z
		.object({
			space_id: z.string().optional(),
		})
		.optional(),
});

export const ElasticsearchHitSchema = z.object({
	_source: ElasticsearchSourceSchema,
});

// Helper function to write invalid records to database
export async function writeInvalidRecords(
	type: string,
	errors: Array<{ message: string }>,
	monitorName?: string,
): Promise<void> {
	log(
		`Database will handle invalid records tracking (type: ${type}, errors: ${JSON.stringify(errors)}, monitorName: ${monitorName})`,
	);

	for (const error of errors) {
		storeInvalidRecord(type, error.message, monitorName);
	}
}

// Function is maintained for backward compatibility but no longer needs to clear a buffer
export function clearInvalidRecordsBuffer(): void {
	// No need to clear anything with the database approach
}

// Validation functions
export async function validateElasticsearchHits(
	data: unknown[],
): Promise<ElasticsearchHit[]> {
	const validHits: ElasticsearchHit[] = [];
	const errorsByMonitor: Record<string, Set<string>> = {};

	for (const hit of data) {
		try {
			const validatedHit = ElasticsearchHitSchema.parse(
				hit,
			) as ElasticsearchHit;
			validHits.push(validatedHit);
		} catch (error) {
			// Try to get monitor name even from invalid hit
			const monitorName = (hit as any)?._source?.monitor?.name;
			if (!monitorName) continue; // Skip if we can't get a monitor name

			if (!errorsByMonitor[monitorName]) {
				errorsByMonitor[monitorName] = new Set();
			}

			if (error instanceof z.ZodError) {
				error.issues.forEach((issue) => {
					errorsByMonitor[monitorName]?.add(
						`${issue.path.join(".")}: ${issue.message}`,
					);
				});
			} else {
				errorsByMonitor[monitorName]?.add(String(error));
			}
		}
	}

	// Write errors for each monitor
	for (const [monitorName, errorSet] of Object.entries(errorsByMonitor)) {
		if (errorSet.size > 0) {
			const errors = Array.from(errorSet).map((message) => ({ message }));
			await writeInvalidRecords("elasticsearch_hits", errors, monitorName);
		}
	}

	return validHits;
}

export async function validateMonitorInfo(
	data: unknown[],
): Promise<MonitorInfo[]> {
	const validMonitors: MonitorInfo[] = [];
	const errors: unknown[] = [];

	for (const info of data) {
		try {
			const validatedInfo = MonitorInfoSchema.parse(info) as MonitorInfo;
			validMonitors.push(validatedInfo);
		} catch (error) {
			warn(`Skipping invalid monitor info (validation_error: ${error})`);
			errors.push(error);
			continue;
		}
	}

	if (errors.length > 0) {
		await writeInvalidRecords(
			"monitor_info",
			errors.map((error) => ({
				message: error instanceof Error ? error.message : String(error),
			})),
			"unknown",
		);
	}

	return validMonitors;
}

export async function validateBusinessContext(
	data: unknown,
): Promise<BusinessContext> {
	try {
		return BusinessContextSchema.parse(data);
	} catch (error) {
		warn(`Invalid business context (validation_error: ${error})`);
		await writeInvalidRecords(
			"business_context",
			[{ message: error instanceof Error ? error.message : String(error) }],
			"unknown",
		);
		throw error;
	}
}

// Define Zod schemas for type-specific fields
export const HttpSchema = z
	.object({
		statusCode: z.number().optional(),
		responseTime: z.number().optional(),
		body: z
			.object({
				bytes: z.number().optional(),
				content: z.string().optional(),
				hash: z.string().optional(),
			})
			.optional(),
		headers: z.record(z.string()).optional(),
		mimeType: z.string().optional(),
	})
	.optional();

export const TlsSchema = z
	.object({
		established: z.boolean(),
		version: z.string().optional(),
		cipher: z.string().optional(),
		certificateNotValidBefore: z.string().optional(),
		certificateNotValidAfter: z.string().optional(),
		server: z
			.object({
				x509: z
					.object({
						notAfter: z.string().optional(),
						notBefore: z.string().optional(),
						publicKeyExponent: z.number().optional(),
						subject: z
							.object({
								distinguishedName: z.string().optional(),
								commonName: z.string().optional(),
							})
							.optional(),
						issuer: z
							.object({
								distinguishedName: z.string().optional(),
								commonName: z.string().optional(),
							})
							.optional(),
						publicKeyAlgorithm: z.string().optional(),
						signatureAlgorithm: z.string().optional(),
						publicKeySize: z.number().optional(),
						serialNumber: z.string().optional(),
					})
					.optional(),
				hash: z
					.object({
						sha1: z.string().optional(),
						sha256: z.string().optional(),
					})
					.optional(),
			})
			.optional(),
	})
	.optional();

export const TcpSchema = z
	.object({
		connectTime: z.number().optional(),
		ip: z.string().optional(),
		port: z.number().optional(),
		url: z
			.object({
				scheme: z.string().optional(),
				domain: z.string().optional(),
				port: z.number().optional(),
				full: z.string().optional(),
			})
			.optional(),
	})
	.optional();

export const IcmpSchema = z
	.object({
		rtt: z.number().optional(),
		requests: z.number().optional(),
		ip: z.string().optional(),
		url: z
			.object({
				scheme: z.string().optional(),
				domain: z.string().optional(),
				port: z.number().optional(),
				full: z.string().optional(),
			})
			.optional(),
	})
	.optional();

export const BrowserSchema = z
	.object({
		synthetics: z
			.object({
				type: z.string().optional(),
				journey: z
					.object({
						name: z.string().optional(),
						id: z.string().optional(),
					})
					.optional(),
				step: z
					.object({
						name: z.string().optional(),
						index: z.number().optional(),
						status: z.string().optional(),
					})
					.optional(),
				error: z
					.object({
						name: z.string().optional(),
						message: z.string().optional(),
						stack: z.string().optional(),
					})
					.optional(),
			})
			.optional(),
		journey: z
			.object({
				name: z.string().optional(),
				id: z.string().optional(),
			})
			.optional(),
		step: z
			.object({
				name: z.string().optional(),
				index: z.number().optional(),
				status: z.string().optional(),
			})
			.optional(),
		error: z
			.object({
				name: z.string().optional(),
				message: z.string().optional(),
				stack: z.string().optional(),
			})
			.optional(),
	})
	.optional();

export const AgentSchema = z
	.object({
		name: z.string(),
		id: z.string(),
		type: z.string(),
		version: z.string(),
		ephemeralId: z.string().optional(),
	})
	.optional();

export const ObserverSchema = z
	.object({
		name: z.string(),
		geo: z.string().optional(),
	})
	.optional();

export const StateSchema = z
	.object({
		status: z.string(),
		durationMs: z.string().optional(),
		checks: z.number().optional(),
		up: z.number().optional(),
		down: z.number().optional(),
		startedAt: z.string().optional(),
		id: z.string().optional(),
	})
	.optional();

export const SummarySchema = z
	.object({
		status: z.string(),
		up: z.number(),
		down: z.number(),
		attempt: z.number(),
		maxAttempts: z.number(),
		finalAttempt: z.boolean(),
		retryGroup: z.string().optional(),
	})
	.optional();

export const MetaSchema = z
	.object({
		spaceId: z.string(),
	})
	.optional();

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
	};
	agent?: {
		id: string;
		ephemeral_id?: string;
		name?: string;
		type?: string;
		version?: string;
	};
	observer?: {
		geo?: {
			name?: string;
		};
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
	meta?: {
		space_id: string;
	};
	event?: {
		agent_id_status?: string;
		ingested?: string;
		type?: string;
		dataset?: string;
	};
	ecs?: {
		version?: string;
	};
	data_stream?: {
		namespace?: string;
		type?: string;
		dataset?: string;
	};
	tags?: string[];
	config_id?: string;
	summary?: {
		retry_group?: string;
		max_attempts?: number;
		up?: number;
		down?: number;
		attempt?: number;
		final_attempt?: boolean;
		status?: string;
	};
	state?: {
		duration_ms?: string;
		checks?: number;
		ends?: any;
		started_at?: string;
		up?: number;
		id?: string;
		down?: number;
		flap_history?: any[];
		status?: string;
	};
	synthetics?: {
		type?: string;
	};
}
