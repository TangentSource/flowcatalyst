/**
 * Execution Context Middleware
 *
 * Creates an ExecutionContext for use case execution by combining
 * tracing and audit context. This middleware should be applied after
 * tracing and audit middleware.
 */

import type { MiddlewareHandler } from 'hono';
import { ExecutionContext } from '@flowcatalyst/domain-core';
import type { FlowCatalystEnv } from '../types.js';

/**
 * Create execution context middleware for Hono.
 *
 * Combines tracing context (correlation ID, causation ID) with
 * audit context (principal ID) to create an ExecutionContext for
 * use case execution.
 *
 * @returns Hono middleware handler
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono';
 * import { tracingMiddleware, auditMiddleware, executionContextMiddleware } from '@flowcatalyst/http';
 *
 * const app = new Hono<FlowCatalystEnv>();
 *
 * // Apply in order: tracing → audit → executionContext
 * app.use('*', tracingMiddleware());
 * app.use('*', auditMiddleware({ validateToken: ... }));
 * app.use('*', executionContextMiddleware());
 *
 * app.post('/api/users', (c) => {
 *     const ctx = c.get('executionContext');
 *     const result = await createUserUseCase.execute(command, ctx);
 *     // ...
 * });
 * ```
 */
export function executionContextMiddleware(): MiddlewareHandler<FlowCatalystEnv> {
	return async (c, next) => {
		const tracing = c.get('tracing');
		const audit = c.get('audit');

		if (!tracing) {
			throw new Error('Tracing context not available. Apply tracingMiddleware before executionContextMiddleware.');
		}

		// Create execution context from tracing and audit data
		const executionContext = ExecutionContext.fromTracingContext(
			{
				correlationId: tracing.correlationId,
				causationId: tracing.causationId,
			},
			audit?.principalId ?? 'anonymous',
		);

		c.set('executionContext', executionContext);

		return next();
	};
}

/**
 * Get execution context from Hono context, throwing if not available.
 *
 * @param c - Hono context
 * @returns Execution context
 * @throws Error if execution context middleware has not been applied
 */
export function requireExecutionContext(c: {
	get: (key: 'executionContext') => ExecutionContext | undefined;
}): ExecutionContext {
	const ctx = c.get('executionContext');
	if (!ctx) {
		throw new Error('ExecutionContext not available. Ensure executionContextMiddleware is applied.');
	}
	return ctx;
}
