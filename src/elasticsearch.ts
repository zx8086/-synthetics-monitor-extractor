/* src/elasticsearch.ts */

import { Client, estypes } from "@elastic/elasticsearch";
import { HttpConnection } from "@elastic/transport";
import { config } from "./config.js";
import { Agent as HttpAgent } from "http";
import { Agent as HttpsAgent } from "https";
import { log, err, warn } from "./utils/logger.js";

// Configure global agent defaults for keep-alive
const httpAgent = new HttpAgent({
	keepAlive: true,
	keepAliveMsecs: 30000, // 30 seconds
	maxSockets: 50,
	timeout: 60000, // 1 minute
});

const httpsAgent = new HttpsAgent({
	keepAlive: true,
	keepAliveMsecs: 30000, // 30 seconds
	maxSockets: 50,
	timeout: 60000, // 1 minute
	rejectUnauthorized: config.elasticsearch.rejectUnauthorized,
});

// Singleton client instance
let clientInstance: Client | null = null;

/**
 * Creates an optimized Elasticsearch client with proper connection pooling
 */
export function getElasticsearchClient(): Client {
	if (!clientInstance) {
		log("Creating new Elasticsearch client with connection pooling");

		clientInstance = new Client({
			node: config.elasticsearch.node,
			auth:
				config.elasticsearch.apiKeyId && config.elasticsearch.apiKey
					? {
							apiKey: {
								id: config.elasticsearch.apiKeyId,
								api_key: config.elasticsearch.apiKey,
							},
						}
					: undefined,
			maxRetries: config.elasticsearch.maxRetries,
			requestTimeout: config.elasticsearch.requestTimeout,
			sniffOnStart: config.elasticsearch.sniffOnStart,
			name: config.elasticsearch.name,
			opaqueIdPrefix: config.elasticsearch.opaqueIdPrefix,
			compression: config.elasticsearch.compression,
			headers: {
				"Keep-Alive": "timeout=60, max=1000",
				Connection: "keep-alive",
				Accept: "application/json",
				"Content-Type": "application/json",
				"Accept-Encoding": "gzip, deflate",
			},
			Connection: HttpConnection,
			tls: {
				rejectUnauthorized: config.elasticsearch.rejectUnauthorized,
			},
			agent: config.elasticsearch.node.startsWith("https")
				? httpsAgent
				: httpAgent,
		});

		// Set up a maintenance interval to keep connections alive
		const interval = setInterval(() => {
			if (clientInstance) {
				clientInstance
					.ping()
					.catch((err) => warn(`Elasticsearch ping failed: ${err.message}`));
			}
		}, 300000); // 5 minutes

		// Clean up on process exit
		process.on("beforeExit", () => {
			clearInterval(interval);
			if (clientInstance) {
				log("Closing Elasticsearch client connections");
				clientInstance.close();
				clientInstance = null;
			}
		});
	}

	return clientInstance;
}

/**
 * Executes an Elasticsearch search with proper error handling and retry logic
 */
export async function executeSearch<T>(
	searchParams: estypes.SearchRequest,
): Promise<T[]> {
	const client = getElasticsearchClient();

	try {
		const response = await client.search<T>(searchParams);

		return response.hits.hits.map((hit) => hit._source as T);
	} catch (error: any) {
		if (error.name === "TimeoutError") {
			warn(
				`Elasticsearch query timed out, retrying with longer timeout (query: ${JSON.stringify(searchParams)})`,
			);

			// Retry with longer timeout
			const retryResponse = await client.search<T>({
				...searchParams,
				timeout: "60s",
			});

			return retryResponse.hits.hits.map((hit) => hit._source as T);
		}

		// Log the detailed error
		err(`Elasticsearch search failed (elasticsearch_error: ${error.message})`);
		if (error.meta?.body) {
			err(
				`Error details (elasticsearch_error_body: ${JSON.stringify(error.meta.body, null, 2)})`,
			);
		}

		throw error;
	}
}

/**
 * Gracefully close the Elasticsearch client connection
 */
export async function closeElasticsearchClient(): Promise<void> {
	if (clientInstance) {
		log("Closing Elasticsearch client connections");
		await clientInstance.close();
		clientInstance = null;
	}
}

/**
 * Check the health of the Elasticsearch connection
 */
export async function checkElasticsearchHealth(): Promise<boolean> {
	try {
		const client = getElasticsearchClient();
		await client.ping();
		return true;
	} catch (error) {
		err(`Elasticsearch health check failed (elasticsearch_error: ${error})`);
		return false;
	}
}