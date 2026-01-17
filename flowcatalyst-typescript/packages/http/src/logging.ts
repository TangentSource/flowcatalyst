/**
 * Structured Logging
 *
 * Pino-based structured logging with automatic tracing context injection.
 * Creates request-scoped loggers that include correlation and execution IDs.
 */

import pino, { type Logger, type LoggerOptions } from 'pino';
import type { MiddlewareHandler } from 'hono';
import type { FlowCatalystEnv, TracingData } from './types.js';

/**
 * Configuration for the logging middleware.
 */
export interface LoggingConfig {
	/** Log level (default: 'info') */
	readonly level?: string;
	/** Service name for log context */
	readonly serviceName?: string;
	/** Whether to log request/response details (default: true) */
	readonly logRequests?: boolean;
	/** Paths to skip logging (e.g., /health) */
	readonly skipPaths?: string[];
	/** Additional base context to include in all logs */
	readonly baseContext?: Record<string, unknown>;
	/** Custom Pino options */
	readonly pinoOptions?: LoggerOptions;
}

/**
 * Create the base logger instance.
 *
 * @param config - Logging configuration
 * @returns Pino logger instance
 *
 * @example
 * ```typescript
 * const logger = createLogger({
 *     level: 'info',
 *     serviceName: 'control-plane',
 * });
 *
 * logger.info({ userId: '123' }, 'User created');
 * ```
 */
export function createLogger(config: LoggingConfig = {}): Logger {
	const { level = 'info', serviceName, baseContext = {}, pinoOptions = {} } = config;

	const options: LoggerOptions = {
		level,
		...pinoOptions,
		base: {
			...(serviceName ? { service: serviceName } : {}),
			...baseContext,
			...pinoOptions.base,
		},
	};

	return pino(options);
}

/**
 * Create a child logger with tracing context.
 *
 * @param baseLogger - The base Pino logger
 * @param tracing - Tracing data from request context
 * @returns Child logger with tracing fields
 */
export function createRequestLogger(baseLogger: Logger, tracing: TracingData): Logger {
	return baseLogger.child({
		correlationId: tracing.correlationId,
		executionId: tracing.executionId,
		...(tracing.causationId ? { causationId: tracing.causationId } : {}),
	});
}

/**
 * Create logging middleware for Hono.
 *
 * Attaches a request-scoped logger to the context with tracing fields.
 * Optionally logs request and response details.
 *
 * @param baseLogger - The base Pino logger
 * @param config - Logging configuration
 * @returns Hono middleware handler
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono';
 * import { tracingMiddleware, loggingMiddleware, createLogger } from '@flowcatalyst/http';
 *
 * const logger = createLogger({ serviceName: 'api' });
 * const app = new Hono<FlowCatalystEnv>();
 *
 * app.use('*', tracingMiddleware());
 * app.use('*', loggingMiddleware(logger));
 *
 * app.get('/api/users', (c) => {
 *     const log = c.get('log');
 *     log.info('Fetching users');  // Automatically includes correlationId, executionId
 *     return c.json({ users: [] });
 * });
 * ```
 */
export function loggingMiddleware(
	baseLogger: Logger,
	config: LoggingConfig = {},
): MiddlewareHandler<FlowCatalystEnv> {
	const { logRequests = true, skipPaths = [] } = config;

	return async (c, next) => {
		const path = new URL(c.req.url).pathname;

		// Check if path should be skipped
		const shouldSkip = skipPaths.some((skip) => path.startsWith(skip));

		// Get tracing context (should be set by tracingMiddleware)
		const tracing = c.get('tracing');
		if (!tracing) {
			// If no tracing, use base logger
			c.set('log', baseLogger);
			return next();
		}

		// Create request-scoped logger with tracing context
		const log = createRequestLogger(baseLogger, tracing);
		c.set('log', log);

		// Log request start
		if (logRequests && !shouldSkip) {
			log.info(
				{
					method: c.req.method,
					path,
					userAgent: c.req.header('user-agent'),
				},
				'Request started',
			);
		}

		// Execute handlers
		await next();

		// Log request completion
		if (logRequests && !shouldSkip) {
			const duration = Date.now() - tracing.startTime;
			log.info(
				{
					method: c.req.method,
					path,
					status: c.res.status,
					durationMs: duration,
				},
				'Request completed',
			);
		}
	};
}

/**
 * Get logger from Hono context.
 *
 * @param c - Hono context
 * @returns Logger (or base logger if not available)
 */
export function getLogger(c: { get: (key: 'log') => Logger | undefined }): Logger {
	return c.get('log') ?? pino();
}
