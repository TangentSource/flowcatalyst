/**
 * Error Handler
 *
 * Global error handler for Hono applications.
 * Catches exceptions and maps them to appropriate HTTP responses.
 */

import type { ErrorHandler } from 'hono';
import type { ContentfulStatusCode } from 'hono/utils/http-status';
import { HTTPException } from 'hono/http-exception';
import type { Logger } from 'pino';
import type { FlowCatalystEnv, ErrorResponse } from './types.js';

/**
 * Configuration for the error handler.
 */
export interface ErrorHandlerConfig {
	/** Logger for error logging */
	readonly logger?: Logger;
	/** Whether to include stack traces in responses (default: false) */
	readonly includeStack?: boolean;
	/** Custom error mappers */
	readonly mappers?: ErrorMapper[];
}

/**
 * Custom error mapper function.
 */
export interface ErrorMapper {
	/** Check if this mapper handles the error */
	canHandle: (error: Error) => boolean;
	/** Map the error to an HTTP response */
	toResponse: (error: Error) => { status: number; body: ErrorResponse };
}

/**
 * Create a global error handler for Hono.
 *
 * Handles:
 * - HTTPException: Uses status and message from exception
 * - Custom errors: Uses registered mappers
 * - Unknown errors: Returns 500 Internal Server Error
 *
 * @param config - Error handler configuration
 * @returns Hono error handler
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono';
 * import { createErrorHandler, createLogger } from '@flowcatalyst/http';
 *
 * const logger = createLogger({ serviceName: 'api' });
 * const app = new Hono<FlowCatalystEnv>();
 *
 * app.onError(createErrorHandler({
 *     logger,
 *     mappers: [
 *         {
 *             canHandle: (e) => e.name === 'ValidationError',
 *             toResponse: (e) => ({
 *                 status: 400,
 *                 body: { code: 'VALIDATION_ERROR', message: e.message },
 *             }),
 *         },
 *     ],
 * }));
 * ```
 */
export function createErrorHandler(config: ErrorHandlerConfig = {}): ErrorHandler<FlowCatalystEnv> {
	const { logger, includeStack = false, mappers = [] } = config;

	return (error, c) => {
		// Get request logger if available, fall back to base logger
		const log = c.get('log') ?? logger;
		const tracing = c.get('tracing');

		// Handle HTTPException (from Hono)
		if (error instanceof HTTPException) {
			const status = error.status;
			const message = error.message || 'An error occurred';

			if (status >= 500 && log) {
				log.error(
					{
						error: error.name,
						message: error.message,
						status,
						...(tracing ? { correlationId: tracing.correlationId } : {}),
					},
					'HTTP exception',
				);
			}

			const body: ErrorResponse = {
				code: `HTTP_${status}`,
				message,
			};

			return c.json(body, status as ContentfulStatusCode);
		}

		// Try custom mappers
		for (const mapper of mappers) {
			if (mapper.canHandle(error)) {
				const { status, body } = mapper.toResponse(error);

				if (status >= 500 && log) {
					log.error(
						{
							error: error.name,
							message: error.message,
							status,
							...(tracing ? { correlationId: tracing.correlationId } : {}),
						},
						'Mapped error',
					);
				}

				return c.json(body, status as ContentfulStatusCode);
			}
		}

		// Log unexpected errors
		if (log) {
			log.error(
				{
					error: error.name,
					message: error.message,
					stack: error.stack,
					...(tracing ? { correlationId: tracing.correlationId } : {}),
				},
				'Unhandled error',
			);
		}

		// Return generic error response
		const body: ErrorResponse = {
			code: 'INTERNAL_ERROR',
			message: 'An unexpected error occurred',
			...(includeStack && error.stack ? { details: { stack: error.stack } } : {}),
		};

		return c.json(body, 500 as ContentfulStatusCode);
	};
}

/**
 * Create common error mappers.
 *
 * @returns Array of common error mappers
 */
export function createCommonErrorMappers(): ErrorMapper[] {
	return [
		// Zod validation errors
		{
			canHandle: (e) => e.name === 'ZodError',
			toResponse: (e) => {
				const zodError = e as { issues?: Array<{ path: string[]; message: string }> };
				const details = zodError.issues?.reduce(
					(acc, issue) => {
						const path = issue.path.join('.');
						acc[path] = issue.message;
						return acc;
					},
					{} as Record<string, unknown>,
				);

				return {
					status: 400,
					body: {
						code: 'VALIDATION_ERROR',
						message: 'Request validation failed',
						...(details && Object.keys(details).length > 0 ? { details } : {}),
					},
				};
			},
		},
		// JSON parse errors
		{
			canHandle: (e) => e instanceof SyntaxError && e.message.includes('JSON'),
			toResponse: () => ({
				status: 400,
				body: {
					code: 'INVALID_JSON',
					message: 'Invalid JSON in request body',
				},
			}),
		},
	];
}

/**
 * Create the standard error handler with common mappers.
 *
 * @param logger - Logger instance
 * @returns Hono error handler
 */
export function createStandardErrorHandler(logger?: Logger): ErrorHandler<FlowCatalystEnv> {
	return createErrorHandler({
		...(logger ? { logger } : {}),
		mappers: createCommonErrorMappers(),
	});
}
