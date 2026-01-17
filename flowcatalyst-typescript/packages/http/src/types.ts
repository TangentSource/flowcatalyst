/**
 * HTTP Layer Types
 *
 * Type definitions for the HTTP layer including Hono environment,
 * context variables, and common interfaces.
 */

import type { Context } from 'hono';
import type { PrincipalInfo, ExecutionContext } from '@flowcatalyst/domain-core';
import type { Logger } from 'pino';

/**
 * Tracing data stored in request context.
 */
export interface TracingData {
	/** Correlation ID for distributed tracing (from header or generated) */
	readonly correlationId: string;
	/** Causation ID linking to parent event (from header, may be null) */
	readonly causationId: string | null;
	/** Unique execution ID for this request */
	readonly executionId: string;
	/** Request start time */
	readonly startTime: number;
}

/**
 * Audit data stored in request context.
 */
export interface AuditData {
	/** The authenticated principal ID (null if not authenticated) */
	readonly principalId: string | null;
	/** Full principal information (loaded lazily) */
	readonly principal: PrincipalInfo | null;
}

/**
 * Hono environment type for FlowCatalyst applications.
 *
 * @example
 * ```typescript
 * const app = new Hono<FlowCatalystEnv>();
 *
 * app.get('/api/users', (c) => {
 *     const tracing = c.get('tracing');
 *     const audit = c.get('audit');
 *     const log = c.get('log');
 *
 *     log.info({ userId: audit.principalId }, 'Fetching users');
 *     return c.json({ users: [] });
 * });
 * ```
 */
export interface FlowCatalystEnv {
	Variables: {
		/** Tracing context for distributed tracing */
		tracing: TracingData;
		/** Audit context for authentication/authorization */
		audit: AuditData;
		/** Request-scoped logger with tracing context */
		log: Logger;
		/** Execution context for use case calls */
		executionContext: ExecutionContext;
	};
}

/**
 * Type alias for Hono Context with FlowCatalyst environment.
 */
export type FlowCatalystContext = Context<FlowCatalystEnv>;

/**
 * Configuration for the tracing middleware.
 */
export interface TracingMiddlewareConfig {
	/** Header name for correlation ID (default: X-Correlation-ID) */
	readonly correlationIdHeader?: string;
	/** Alternative header name for correlation ID (default: X-Request-ID) */
	readonly requestIdHeader?: string;
	/** Header name for causation ID (default: X-Causation-ID) */
	readonly causationIdHeader?: string;
	/** Whether to add correlation ID to response headers (default: true) */
	readonly propagateToResponse?: boolean;
}

/**
 * Configuration for the audit middleware.
 */
export interface AuditMiddlewareConfig {
	/** Cookie name for session token (default: session) */
	readonly sessionCookieName?: string;
	/** Paths to skip authentication (e.g., /health, /metrics) */
	readonly skipPaths?: string[];
	/** Function to validate JWT and extract principal ID */
	readonly validateToken: (token: string) => Promise<string | null>;
	/** Function to load principal by ID (optional, for full principal loading) */
	readonly loadPrincipal?: (principalId: string) => Promise<PrincipalInfo | null>;
}

/**
 * Standard error response format.
 */
export interface ErrorResponse {
	/** Human-readable error message */
	readonly message: string;
	/** Machine-readable error code */
	readonly code: string;
	/** Additional error details */
	readonly details?: Record<string, unknown>;
}
