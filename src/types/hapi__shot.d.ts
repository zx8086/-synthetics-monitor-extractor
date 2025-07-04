/**
 * Type declaration file for hapi__shot
 *
 * This file is used to satisfy TypeScript's type checking system
 * for implicit references to the hapi__shot module.
 */

declare module 'hapi__shot' {
  // Minimal interface to satisfy TypeScript
  export interface RequestOptions {
    method?: string;
    url?: string;
    payload?: any;
    headers?: Record<string, string>;
    validate?: boolean;
    auth?: any;
  }

  export interface Response {
    statusCode: number;
    headers: Record<string, string>;
    payload: string;
    rawPayload: Buffer;
    raw: {
      req: any;
      res: any;
    };
    result: any;
  }

  export function inject(dispatchFunc: any, options: RequestOptions): Promise<Response>;
  export function inject(server: any, options: RequestOptions): Promise<Response>;
}
