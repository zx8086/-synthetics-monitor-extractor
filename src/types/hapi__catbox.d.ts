/**
 * Type declaration file for hapi__catbox
 *
 * This file is used to satisfy TypeScript's type checking system
 * for implicit references to the hapi__catbox module.
 */

declare module 'hapi__catbox' {
  // Minimal interface to satisfy TypeScript
  export interface Policy<T = any> {
    get(id: string): Promise<{ item: T; cached: boolean; }>;
    set(id: string, value: T, ttl?: number): Promise<void>;
    drop(id: string): Promise<void>;
  }

  export interface ClientOptions {
    partition?: string;
    shared?: boolean;
  }

  export interface Client<T = any> {
    start(): Promise<void>;
    stop(): Promise<void>;
    get(key: string): Promise<{ item: T; }>;
    set(key: string, value: T, ttl?: number): Promise<void>;
    drop(key: string): Promise<void>;
  }
}
