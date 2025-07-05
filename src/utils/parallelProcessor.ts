/* src/utils/parallelProcessor.ts */

import { log, warn } from "./logger.js";

interface BatchProcessingConfig {
	batchSize: number;
	maxConcurrency: number;
	retryAttempts: number;
	retryDelay: number;
}

export class ParallelProcessor<T, R> {
	private config: BatchProcessingConfig;

	constructor(config: Partial<BatchProcessingConfig> = {}) {
		this.config = {
			batchSize: config.batchSize ?? 100,
			maxConcurrency: config.maxConcurrency ?? 4,
			retryAttempts: config.retryAttempts ?? 2,
			retryDelay: config.retryDelay ?? 1000,
		};

		log("ParallelProcessor initialized", {
			parallel_processor: {
				config: this.config,
			},
		});
	}

	/**
	 * Process items in parallel batches with controlled concurrency
	 */
	async processBatches<E>(
		items: T[],
		processor: (item: T) => Promise<R>,
		errorHandler?: (item: T, error: Error) => E | null,
	): Promise<{ results: R[]; errors: E[] }> {
		if (items.length === 0) {
			return { results: [], errors: [] };
		}

		const startTime = performance.now();
		const batches = this.createBatches(items);
		const results: R[] = [];
		const errors: E[] = [];

		log(`Processing ${items.length} items in ${batches.length} batches`, {
			parallel_processing: {
				totalItems: items.length,
				batchCount: batches.length,
				batchSize: this.config.batchSize,
				maxConcurrency: this.config.maxConcurrency,
			},
		});

		// Process batches with controlled concurrency
		for (let i = 0; i < batches.length; i += this.config.maxConcurrency) {
			const currentBatches = batches.slice(i, i + this.config.maxConcurrency);

			const batchPromises = currentBatches.map(async (batch, batchIndex) => {
				const actualBatchIndex = i + batchIndex;
				return this.processBatch(
					batch,
					processor,
					errorHandler,
					actualBatchIndex,
				);
			});

			const batchResults = await Promise.allSettled(batchPromises);

			for (const result of batchResults) {
				if (result.status === "fulfilled") {
					results.push(...result.value.results);
					errors.push(...result.value.errors);
				} else {
					warn(`Batch processing failed`, {
						parallel_processing_error: {
							reason: result.reason,
							message:
								result.reason instanceof Error
									? result.reason.message
									: String(result.reason),
						},
					});
				}
			}
		}

		const duration = performance.now() - startTime;
		log(`Parallel processing completed`, {
			parallel_processing_summary: {
				totalItems: items.length,
				successfulResults: results.length,
				errors: errors.length,
				durationMs: Math.round(duration),
				itemsPerSecond: Math.round(items.length / (duration / 1000)),
			},
		});

		return { results, errors };
	}

	/**
	 * Process a single batch with retry logic
	 */
	private async processBatch<E>(
		batch: T[],
		processor: (item: T) => Promise<R>,
		errorHandler?: (item: T, error: Error) => E | null,
		batchIndex?: number,
	): Promise<{ results: R[]; errors: E[] }> {
		const results: R[] = [];
		const errors: E[] = [];

		const itemPromises = batch.map(async (item, itemIndex) => {
			try {
				const result = await this.processItemWithRetry(item, processor);
				return { success: true, result, item, itemIndex };
			} catch (error) {
				const handledError = errorHandler
					? errorHandler(item, error as Error)
					: null;
				return {
					success: false,
					error: handledError,
					item,
					itemIndex,
					originalError: error,
				};
			}
		});

		const itemResults = await Promise.allSettled(itemPromises);

		for (const itemResult of itemResults) {
			if (itemResult.status === "fulfilled") {
				const { success, result, error } = itemResult.value;
				if (success && result !== undefined) {
					results.push(result);
				} else if (!success && error !== null) {
					errors.push(error);
				}
			} else {
				warn(`Item processing failed in batch ${batchIndex}`, {
					batch_processing_error: {
						reason: itemResult.reason,
						message:
							itemResult.reason instanceof Error
								? itemResult.reason.message
								: String(itemResult.reason),
					},
				});
			}
		}

		return { results, errors };
	}

	/**
	 * Process a single item with retry logic
	 */
	private async processItemWithRetry(
		item: T,
		processor: (item: T) => Promise<R>,
	): Promise<R> {
		let lastError: Error;

		for (let attempt = 0; attempt <= this.config.retryAttempts; attempt++) {
			try {
				return await processor(item);
			} catch (error) {
				lastError = error as Error;

				if (attempt < this.config.retryAttempts) {
					// Add exponential backoff with jitter
					const delay =
						this.config.retryDelay * Math.pow(2, attempt) + Math.random() * 100;
					await this.sleep(delay);
				}
			}
		}

		throw lastError!;
	}

	/**
	 * Create batches from the input array
	 */
	private createBatches(items: T[]): T[][] {
		const batches: T[][] = [];
		for (let i = 0; i < items.length; i += this.config.batchSize) {
			batches.push(items.slice(i, i + this.config.batchSize));
		}
		return batches;
	}

	/**
	 * Sleep utility for retry delays
	 */
	private sleep(ms: number): Promise<void> {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	/**
	 * Update configuration dynamically
	 */
	updateConfig(newConfig: Partial<BatchProcessingConfig>): void {
		this.config = { ...this.config, ...newConfig };
		log("ParallelProcessor configuration updated", {
			parallel_processor_config: this.config,
		});
	}

	/**
	 * Get current configuration
	 */
	getConfig(): BatchProcessingConfig {
		return { ...this.config };
	}
}

/**
 * Streaming processor for very large datasets
 */
export class StreamingProcessor<T, R> {
	private processedCount = 0;
	private errorCount = 0;
	private startTime: number | null = null;

	constructor(
		private processor: (item: T) => Promise<R>,
		private onResult?: (result: R, index: number) => void,
		private onError?: (error: Error, item: T, index: number) => void,
		private progressInterval: number = 1000,
	) {}

	/**
	 * Process items as a stream with memory-efficient iteration
	 */
	async *processStream(items: Iterable<T>): AsyncGenerator<R, void, unknown> {
		this.startTime = performance.now();
		let index = 0;

		for (const item of items) {
			try {
				const result = await this.processor(item);
				this.processedCount++;

				if (this.onResult) {
					this.onResult(result, index);
				}

				// Log progress periodically
				if (this.processedCount % this.progressInterval === 0) {
					this.logProgress();
				}

				yield result;
			} catch (error) {
				this.errorCount++;
				if (this.onError) {
					this.onError(error as Error, item, index);
				}
			}
			index++;
		}

		this.logFinalStats();
	}

	/**
	 * Process all items and collect results (use for smaller datasets)
	 */
	async processAll(items: T[]): Promise<R[]> {
		const results: R[] = [];

		for await (const result of this.processStream(items)) {
			results.push(result);
		}

		return results;
	}

	private logProgress(): void {
		if (this.startTime) {
			const duration = performance.now() - this.startTime;
			const rate = this.processedCount / (duration / 1000);

			log(`Streaming progress update`, {
				streaming_progress: {
					processedCount: this.processedCount,
					errorCount: this.errorCount,
					durationMs: Math.round(duration),
					itemsPerSecond: Math.round(rate),
				},
			});
		}
	}

	private logFinalStats(): void {
		if (this.startTime) {
			const duration = performance.now() - this.startTime;
			const rate = this.processedCount / (duration / 1000);

			log(`Streaming processing completed`, {
				streaming_summary: {
					totalProcessed: this.processedCount,
					totalErrors: this.errorCount,
					durationMs: Math.round(duration),
					itemsPerSecond: Math.round(rate),
				},
			});
		}
	}

	getStats() {
		return {
			processedCount: this.processedCount,
			errorCount: this.errorCount,
			startTime: this.startTime,
		};
	}
}
