/**
 * Tracing Middleware
 *
 * Hono middleware for distributed tracing. Extracts correlation and causation IDs
 * from request headers and propagates them to response headers.
 *
 * This middleware integrates with TracingContext from @flowcatalyst/domain-core
 * for consistent tracing across HTTP and background processing.
 */

import type { MiddlewareHandler } from 'hono';
import { generate as generateTsid } from '@flowcatalyst/tsid';
import type { FlowCatalystEnv, TracingMiddlewareConfig, TracingData } from '../types.js';

/** Default header names */
const DEFAULT_CORRELATION_ID_HEADER = 'X-Correlation-ID';
const DEFAULT_REQUEST_ID_HEADER = 'X-Request-ID';
const DEFAULT_CAUSATION_ID_HEADER = 'X-Causation-ID';

/**
 * Create tracing middleware for Hono.
 *
 * Extracts correlation ID from request headers (X-Correlation-ID or X-Request-ID),
 * generates one if not present, and adds it to response headers.
 *
 * @param config - Optional configuration
 * @returns Hono middleware handler
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono';
 * import { tracingMiddleware } from '@flowcatalyst/http';
 *
 * const app = new Hono<FlowCatalystEnv>();
 * app.use('*', tracingMiddleware());
 *
 * app.get('/api/users', (c) => {
 *     const { correlationId, executionId } = c.get('tracing');
 *     console.log(`Request ${executionId} in trace ${correlationId}`);
 *     return c.json({ users: [] });
 * });
 * ```
 */
export function tracingMiddleware(config: TracingMiddlewareConfig = {}): MiddlewareHandler<FlowCatalystEnv> {
	const {
		correlationIdHeader = DEFAULT_CORRELATION_ID_HEADER,
		requestIdHeader = DEFAULT_REQUEST_ID_HEADER,
		causationIdHeader = DEFAULT_CAUSATION_ID_HEADER,
		propagateToResponse = true,
	} = config;

	return async (c, next) => {
		// Extract correlation ID from headers, or generate one
		let correlationId = c.req.header(correlationIdHeader);
		if (!correlationId) {
			correlationId = c.req.header(requestIdHeader);
		}
		if (!correlationId) {
			correlationId = `trace-${generateTsid()}`;
		}

		// Extract causation ID from headers (may be null)
		const causationId = c.req.header(causationIdHeader) ?? null;

		// Generate unique execution ID for this request
		const executionId = `exec-${generateTsid()}`;

		// Store tracing data in context
		const tracingData: TracingData = {
			correlationId,
			causationId,
			executionId,
			startTime: Date.now(),
		};
		c.set('tracing', tracingData);

		// Execute next handlers
		await next();

		// Add correlation ID to response headers
		if (propagateToResponse) {
			c.header(correlationIdHeader, correlationId);
		}
	};
}

/**
 * Get tracing data from context, throwing if not available.
 *
 * @param c - Hono context
 * @returns Tracing data
 * @throws Error if tracing middleware has not been applied
 */
export function requireTracing(c: { get: (key: 'tracing') => TracingData | undefined }): TracingData {
	const tracing = c.get('tracing');
	if (!tracing) {
		throw new Error('Tracing context not available. Ensure tracingMiddleware is applied.');
	}
	return tracing;
}

/**
 * Get tracing headers for outgoing requests.
 * Use this when making HTTP calls to other services to propagate tracing.
 *
 * @param tracing - Tracing data from current request
 * @returns Headers object for outgoing request
 */
export function getTracingHeaders(tracing: TracingData): Record<string, string> {
	const headers: Record<string, string> = {
		[DEFAULT_CORRELATION_ID_HEADER]: tracing.correlationId,
	};

	// For outgoing requests, the current execution becomes the causation
	headers[DEFAULT_CAUSATION_ID_HEADER] = tracing.executionId;

	return headers;
}
