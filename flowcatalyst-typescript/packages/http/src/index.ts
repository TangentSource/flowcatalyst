/**
 * @flowcatalyst/http
 *
 * HTTP layer utilities for FlowCatalyst platform using Hono:
 * - Middleware for tracing, authentication, and logging
 * - Result to HTTP response mapping
 * - OpenAPI/Zod schema utilities
 * - Structured logging with Pino
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono';
 * import {
 *     FlowCatalystEnv,
 *     tracingMiddleware,
 *     auditMiddleware,
 *     loggingMiddleware,
 *     executionContextMiddleware,
 *     createLogger,
 *     createStandardErrorHandler,
 *     sendResult,
 * } from '@flowcatalyst/http';
 *
 * // Create app with FlowCatalyst environment
 * const app = new Hono<FlowCatalystEnv>();
 *
 * // Create logger
 * const logger = createLogger({ serviceName: 'control-plane' });
 *
 * // Apply middleware
 * app.use('*', tracingMiddleware());
 * app.use('*', auditMiddleware({
 *     validateToken: async (token) => validateJwt(token),
 * }));
 * app.use('*', loggingMiddleware(logger));
 * app.use('*', executionContextMiddleware());
 *
 * // Error handler
 * app.onError(createStandardErrorHandler(logger));
 *
 * // Route with use case
 * app.post('/api/users', async (c) => {
 *     const ctx = c.get('executionContext');
 *     const body = await c.req.json();
 *     const result = await createUserUseCase.execute(body, ctx);
 *     return sendResult(c, result, { successStatus: 201 });
 * });
 * ```
 */

// Types
export {
	type FlowCatalystEnv,
	type FlowCatalystContext,
	type TracingData,
	type AuditData,
	type TracingMiddlewareConfig,
	type AuditMiddlewareConfig,
	type ErrorResponse,
} from './types.js';

// Middleware - Tracing
export { tracingMiddleware, requireTracing, getTracingHeaders } from './middleware/tracing.js';

// Middleware - Audit
export {
	auditMiddleware,
	requireAuth,
	getPrincipalId,
	isAuthenticated,
	requireRole,
} from './middleware/audit.js';

// Middleware - Execution Context
export { executionContextMiddleware, requireExecutionContext } from './middleware/execution-context.js';

// Logging
export {
	createLogger,
	createRequestLogger,
	loggingMiddleware,
	getLogger,
	type LoggingConfig,
} from './logging.js';

// Response utilities
export {
	getErrorStatus,
	toErrorResponse,
	sendResult,
	matchResult,
	jsonSuccess,
	jsonCreated,
	noContent,
	jsonError,
	notFound,
	unauthorized,
	forbidden,
	badRequest,
	type SendResultOptions,
} from './response.js';

// Error handler
export {
	createErrorHandler,
	createCommonErrorMappers,
	createStandardErrorHandler,
	type ErrorHandlerConfig,
	type ErrorMapper,
} from './error-handler.js';

// OpenAPI utilities
export {
	CommonSchemas,
	ErrorResponseSchema,
	paginatedResponse,
	entitySchema,
	OpenAPIResponses,
	combineResponses,
	validateBody,
	validateQuery,
	safeValidate,
} from './openapi.js';

// Re-export commonly used types
export { z } from 'zod';
export { HTTPException } from 'hono/http-exception';
