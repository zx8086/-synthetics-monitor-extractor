/* src/utils/circuitBreaker.ts */

import { log, warn, err } from "./logger.js";

export enum CircuitState {
	CLOSED = "CLOSED",
	OPEN = "OPEN",
	HALF_OPEN = "HALF_OPEN",
}

interface CircuitBreakerConfig {
	failureThreshold: number;
	resetTimeout: number;
	halfOpenMaxCalls: number;
	successThreshold: number;
	name: string;
}

interface CircuitBreakerMetrics {
	totalCalls: number;
	successfulCalls: number;
	failedCalls: number;
	consecutiveFailures: number;
	lastFailureTime?: Date;
	lastSuccessTime?: Date;
	stateChangeTime: Date;
}

export class CircuitBreaker {
	private state: CircuitState = CircuitState.CLOSED;
	private metrics: CircuitBreakerMetrics;
	private config: CircuitBreakerConfig;
	private halfOpenCalls = 0;

	constructor(config: Partial<CircuitBreakerConfig> = {}) {
		this.config = {
			failureThreshold: config.failureThreshold ?? 5,
			resetTimeout: config.resetTimeout ?? 60000, // 1 minute
			halfOpenMaxCalls: config.halfOpenMaxCalls ?? 3,
			successThreshold: config.successThreshold ?? 2,
			name: config.name ?? "CircuitBreaker",
		};

		this.metrics = {
			totalCalls: 0,
			successfulCalls: 0,
			failedCalls: 0,
			consecutiveFailures: 0,
			stateChangeTime: new Date(),
		};

		log(`Circuit breaker "${this.config.name}" initialized`, {
			circuit_breaker: {
				name: this.config.name,
				config: this.config,
			},
		});
	}

	async execute<T>(operation: () => Promise<T>): Promise<T> {
		if (this.shouldReject()) {
			const error = new Error(
				`Circuit breaker "${this.config.name}" is OPEN - calls rejected`,
			);
			err(`Circuit breaker rejected call`, {
				circuit_breaker: {
					name: this.config.name,
					state: this.state,
					metrics: this.getMetrics(),
				},
			});
			throw error;
		}

		this.metrics.totalCalls++;

		try {
			const result = await operation();
			this.onSuccess();
			return result;
		} catch (error) {
			this.onFailure();
			throw error;
		}
	}

	private shouldReject(): boolean {
		if (this.state === CircuitState.CLOSED) {
			return false;
		}

		if (this.state === CircuitState.OPEN) {
			// Check if we should transition to HALF_OPEN
			const now = Date.now();
			const stateChangeTime = this.metrics.stateChangeTime.getTime();

			if (now - stateChangeTime >= this.config.resetTimeout) {
				this.transitionTo(CircuitState.HALF_OPEN);
				this.halfOpenCalls = 0;
				return false;
			}
			return true;
		}

		if (this.state === CircuitState.HALF_OPEN) {
			return this.halfOpenCalls >= this.config.halfOpenMaxCalls;
		}

		return false;
	}

	private onSuccess(): void {
		this.metrics.successfulCalls++;
		this.metrics.consecutiveFailures = 0;
		this.metrics.lastSuccessTime = new Date();

		if (this.state === CircuitState.HALF_OPEN) {
			this.halfOpenCalls++;

			// Check if we've had enough successful calls to close the circuit
			const recentSuccesses = this.halfOpenCalls;
			if (recentSuccesses >= this.config.successThreshold) {
				this.transitionTo(CircuitState.CLOSED);
				this.halfOpenCalls = 0;
			}
		}
	}

	private onFailure(): void {
		this.metrics.failedCalls++;
		this.metrics.consecutiveFailures++;
		this.metrics.lastFailureTime = new Date();

		if (this.state === CircuitState.HALF_OPEN) {
			this.transitionTo(CircuitState.OPEN);
			this.halfOpenCalls = 0;
		} else if (this.state === CircuitState.CLOSED) {
			if (this.metrics.consecutiveFailures >= this.config.failureThreshold) {
				this.transitionTo(CircuitState.OPEN);
			}
		}
	}

	private transitionTo(newState: CircuitState): void {
		const oldState = this.state;
		this.state = newState;
		this.metrics.stateChangeTime = new Date();

		log(`Circuit breaker state transition`, {
			circuit_breaker: {
				name: this.config.name,
				transition: `${oldState} -> ${newState}`,
				metrics: this.getMetrics(),
			},
		});

		// Log warnings for problematic states
		if (newState === CircuitState.OPEN) {
			warn(`Circuit breaker "${this.config.name}" opened due to failures`, {
				circuit_breaker: {
					name: this.config.name,
					consecutiveFailures: this.metrics.consecutiveFailures,
					threshold: this.config.failureThreshold,
				},
			});
		}
	}

	getState(): CircuitState {
		return this.state;
	}

	getMetrics(): CircuitBreakerMetrics {
		return { ...this.metrics };
	}

	isOpen(): boolean {
		return this.state === CircuitState.OPEN;
	}

	isClosed(): boolean {
		return this.state === CircuitState.CLOSED;
	}

	isHalfOpen(): boolean {
		return this.state === CircuitState.HALF_OPEN;
	}

	// Force state for testing purposes
	forceState(state: CircuitState): void {
		this.transitionTo(state);
	}

	// Reset metrics
	reset(): void {
		this.state = CircuitState.CLOSED;
		this.metrics = {
			totalCalls: 0,
			successfulCalls: 0,
			failedCalls: 0,
			consecutiveFailures: 0,
			stateChangeTime: new Date(),
		};
		this.halfOpenCalls = 0;

		log(`Circuit breaker "${this.config.name}" reset`, {
			circuit_breaker: {
				name: this.config.name,
				state: this.state,
			},
		});
	}
}

// Exponential backoff utility
export class ExponentialBackoff {
	private attempts = 0;
	private readonly baseDelay: number;
	private readonly maxDelay: number;
	private readonly jitterFactor: number;
	private readonly maxAttempts: number;

	constructor(
		options: {
			baseDelay?: number;
			maxDelay?: number;
			jitterFactor?: number;
			maxAttempts?: number;
		} = {},
	) {
		this.baseDelay = options.baseDelay ?? 1000;
		this.maxDelay = options.maxDelay ?? 30000;
		this.jitterFactor = options.jitterFactor ?? 0.1;
		this.maxAttempts = options.maxAttempts ?? 5;
	}

	async execute<T>(operation: () => Promise<T>): Promise<T> {
		let lastError: Error;

		for (this.attempts = 0; this.attempts < this.maxAttempts; this.attempts++) {
			try {
				const result = await operation();
				this.reset();
				return result;
			} catch (error) {
				lastError = error as Error;

				if (this.attempts === this.maxAttempts - 1) {
					break;
				}

				const delay = this.calculateDelay();
				log(
					`Retrying operation in ${delay}ms (attempt ${this.attempts + 1}/${this.maxAttempts})`,
					{
						retry: {
							attempt: this.attempts + 1,
							maxAttempts: this.maxAttempts,
							delay,
							error: lastError.message,
						},
					},
				);

				await this.sleep(delay);
			}
		}

		throw lastError!;
	}

	private calculateDelay(): number {
		const exponentialDelay = Math.min(
			this.baseDelay * Math.pow(2, this.attempts),
			this.maxDelay,
		);

		// Add jitter to prevent thundering herd
		const jitter = exponentialDelay * this.jitterFactor * Math.random();
		return Math.floor(exponentialDelay + jitter);
	}

	private sleep(ms: number): Promise<void> {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	private reset(): void {
		this.attempts = 0;
	}

	getAttempts(): number {
		return this.attempts;
	}
}
