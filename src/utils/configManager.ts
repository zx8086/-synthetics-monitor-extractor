/* src/utils/configManager.ts */

import { watch } from "node:fs";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { z } from "zod";
import { log, warn, err } from "./logger.js";

// Define which configuration sections can be hot-reloaded
interface HotReloadableConfig {
	extraction: {
		batchProcessing: {
			enabled: boolean;
			batchSize: number;
			maxConcurrency: number;
			streamingThreshold: number;
			retryAttempts: number;
		};
	};
	logging: {
		level: string;
	};
}

const HotReloadableConfigSchema = z.object({
	extraction: z.object({
		batchProcessing: z.object({
			enabled: z.boolean(),
			batchSize: z.number().min(10).max(1000),
			maxConcurrency: z.number().min(1).max(10),
			streamingThreshold: z.number().min(100).max(10000),
			retryAttempts: z.number().min(0).max(5),
		}),
	}),
	logging: z.object({
		level: z.enum(["debug", "info", "warn", "error"]),
	}),
});

export class ConfigManager {
	private currentConfig: HotReloadableConfig;
	private configFilePath: string;
	private watchers: Map<string, any> = new Map();
	private listeners: Map<
		string,
		Array<(newValue: any, oldValue: any) => void>
	> = new Map();
	private isWatching = false;

	constructor(baseConfig: Partial<HotReloadableConfig>, configDir?: string) {
		// Set default configuration
		this.currentConfig = {
			extraction: {
				batchProcessing: {
					enabled: true,
					batchSize: 100,
					maxConcurrency: 4,
					streamingThreshold: 500,
					retryAttempts: 2,
				},
			},
			logging: {
				level: "info",
			},
			...baseConfig,
		};

		this.configFilePath = configDir
			? join(configDir, "hotreload.json")
			: join(process.cwd(), "config", "hotreload.json");

		log("ConfigManager initialized", {
			config_manager: {
				configFilePath: this.configFilePath,
				currentConfig: this.currentConfig,
			},
		});
	}

	/**
	 * Start watching for configuration file changes
	 */
	async startWatching(): Promise<void> {
		if (this.isWatching) {
			warn("ConfigManager is already watching for changes");
			return;
		}

		try {
			// Try to load initial config from file
			await this.loadConfigFromFile();
		} catch (error) {
			warn("No existing config file found, using defaults", {
				config_file_error:
					error instanceof Error ? error.message : String(error),
			});
		}

		try {
			const watcher = watch(
				this.configFilePath,
				{ persistent: false },
				async (eventType) => {
					if (eventType === "change") {
						log("Configuration file changed, reloading...", {
							config_event: "file_changed",
							file: this.configFilePath,
						});
						await this.loadConfigFromFile();
					}
				},
			);

			this.watchers.set("config", watcher);
			this.isWatching = true;

			log("Configuration file watching started", {
				config_manager: {
					watching: this.configFilePath,
				},
			});
		} catch (error) {
			warn("Failed to start watching config file", {
				config_watch_error:
					error instanceof Error ? error.message : String(error),
			});
		}
	}

	/**
	 * Stop watching for configuration changes
	 */
	stopWatching(): void {
		for (const [key, watcher] of this.watchers) {
			try {
				watcher.close();
				log(`Stopped watching ${key}`);
			} catch (error) {
				warn(`Error stopping watcher for ${key}`, {
					watcher_error: error instanceof Error ? error.message : String(error),
				});
			}
		}
		this.watchers.clear();
		this.isWatching = false;
	}

	/**
	 * Load configuration from file
	 */
	private async loadConfigFromFile(): Promise<void> {
		try {
			const fileContent = await readFile(this.configFilePath, "utf8");
			const rawConfig = JSON.parse(fileContent);

			// Validate the configuration
			const validatedConfig = HotReloadableConfigSchema.parse(rawConfig);
			const oldConfig = { ...this.currentConfig };

			// Update current configuration
			this.currentConfig = validatedConfig;

			// Notify listeners of changes
			this.notifyConfigChange(oldConfig, validatedConfig);

			log("Configuration reloaded successfully", {
				config_reload: {
					file: this.configFilePath,
					changes: this.getConfigDifferences(oldConfig, validatedConfig),
				},
			});
		} catch (error) {
			err("Failed to load configuration from file", {
				config_load_error: {
					file: this.configFilePath,
					error: error instanceof Error ? error.message : String(error),
				},
			});
			throw error;
		}
	}

	/**
	 * Get current configuration
	 */
	getConfig(): HotReloadableConfig {
		return { ...this.currentConfig };
	}

	/**
	 * Get a specific configuration section
	 */
	getSection<K extends keyof HotReloadableConfig>(
		section: K,
	): HotReloadableConfig[K] {
		return { ...this.currentConfig[section] };
	}

	/**
	 * Register a listener for configuration changes
	 */
	onChange<K extends keyof HotReloadableConfig>(
		section: K,
		listener: (
			newValue: HotReloadableConfig[K],
			oldValue: HotReloadableConfig[K],
		) => void,
	): void {
		if (!this.listeners.has(section)) {
			this.listeners.set(section, []);
		}
		this.listeners.get(section)?.push(listener as any);
	}

	/**
	 * Notify listeners of configuration changes
	 */
	private notifyConfigChange(
		oldConfig: HotReloadableConfig,
		newConfig: HotReloadableConfig,
	): void {
		for (const [section, listeners] of this.listeners) {
			const oldValue = oldConfig[section as keyof HotReloadableConfig];
			const newValue = newConfig[section as keyof HotReloadableConfig];

			if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
				for (const listener of listeners) {
					try {
						listener(newValue, oldValue);
					} catch (error) {
						err(`Error in config change listener for section ${section}`, {
							listener_error:
								error instanceof Error ? error.message : String(error),
						});
					}
				}
			}
		}
	}

	/**
	 * Get differences between two configurations
	 */
	private getConfigDifferences(
		oldConfig: HotReloadableConfig,
		newConfig: HotReloadableConfig,
	): string[] {
		const differences: string[] = [];

		// Simple shallow comparison for now
		const checkDifferences = (obj1: any, obj2: any, path: string = "") => {
			for (const key in obj2) {
				const currentPath = path ? `${path}.${key}` : key;

				if (
					typeof obj2[key] === "object" &&
					obj2[key] !== null &&
					!Array.isArray(obj2[key])
				) {
					if (typeof obj1[key] === "object" && obj1[key] !== null) {
						checkDifferences(obj1[key], obj2[key], currentPath);
					} else {
						differences.push(`${currentPath}: added section`);
					}
				} else if (JSON.stringify(obj1[key]) !== JSON.stringify(obj2[key])) {
					differences.push(
						`${currentPath}: ${JSON.stringify(obj1[key])} -> ${JSON.stringify(obj2[key])}`,
					);
				}
			}
		};

		checkDifferences(oldConfig, newConfig);
		return differences;
	}

	/**
	 * Save current configuration to file
	 */
	async saveConfigToFile(): Promise<void> {
		try {
			const configJson = JSON.stringify(this.currentConfig, null, 2);
			await readFile(this.configFilePath, "utf8"); // This will create the file if it doesn't exist
		} catch {
			// File doesn't exist, which is fine
		}

		try {
			await require("fs/promises").writeFile(
				this.configFilePath,
				JSON.stringify(this.currentConfig, null, 2),
			);
			log("Configuration saved to file", {
				config_save: {
					file: this.configFilePath,
				},
			});
		} catch (error) {
			err("Failed to save configuration to file", {
				config_save_error: {
					file: this.configFilePath,
					error: error instanceof Error ? error.message : String(error),
				},
			});
			throw error;
		}
	}
}

// Export types for use in other modules
export type { HotReloadableConfig };
