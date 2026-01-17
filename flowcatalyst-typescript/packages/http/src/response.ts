/**
 * Response Utilities
 *
 * Utilities for mapping Result types to HTTP responses and
 * handling errors consistently.
 */

import type { Context } from 'hono';
import type { ContentfulStatusCode } from 'hono/utils/http-status';
import { Result, type UseCaseError } from '@flowcatalyst/domain-core';
import type { ErrorResponse, FlowCatalystEnv } from './types.js';

/**
 * HTTP status codes for use case errors.
 */
const ERROR_STATUS_MAP: Record<string, number> = {
	validation: 400,
	not_found: 404,
	business_rule: 409,
	concurrency: 409,
};

/**
 * Get HTTP status code for a use case error.
 *
 * @param error - The use case error
 * @returns HTTP status code
 */
export function getErrorStatus(error: UseCaseError): number {
	return ERROR_STATUS_MAP[error.type] ?? 500;
}

/**
 * Convert a use case error to an error response.
 *
 * @param error - The use case error
 * @returns Error response object
 */
export function toErrorResponse(error: UseCaseError): ErrorResponse {
	const hasDetails = Object.keys(error.details).length > 0;
	return {
		message: error.message,
		code: error.code,
		...(hasDetails ? { details: error.details } : {}),
	};
}

/**
 * Options for sending a result as HTTP response.
 */
export interface SendResultOptions<T, R> {
	/** Status code for success (default: 200) */
	successStatus?: number;
	/** Transform success value before sending */
	transform?: (value: T) => R;
}

/**
 * Send a Result as an HTTP response.
 *
 * On success, sends the value (optionally transformed) with the success status.
 * On failure, maps the error to an appropriate HTTP status and error response.
 *
 * @param c - Hono context
 * @param result - The Result to send
 * @param options - Response options
 * @returns HTTP response
 *
 * @example
 * ```typescript
 * app.post('/api/users', async (c) => {
 *     const ctx = c.get('executionContext');
 *     const result = await createUserUseCase.execute(command, ctx);
 *     return sendResult(c, result, {
 *         successStatus: 201,
 *         transform: (event) => ({ userId: event.userId }),
 *     });
 * });
 * ```
 */
export function sendResult<T, R = T>(
	c: Context<FlowCatalystEnv>,
	result: Result<T>,
	options: SendResultOptions<T, R> = {},
): Response {
	const { successStatus = 200, transform } = options;

	if (Result.isSuccess(result)) {
		const value = transform ? transform(result.value) : result.value;
		return c.json(value as object, successStatus as ContentfulStatusCode);
	}

	const status = getErrorStatus(result.error);
	const response = toErrorResponse(result.error);
	return c.json(response, status as ContentfulStatusCode);
}

/**
 * Match on a Result and return appropriate HTTP responses.
 *
 * More flexible than sendResult - allows custom handling for success and failure.
 *
 * @param c - Hono context
 * @param result - The Result to match
 * @param onSuccess - Handler for success case
 * @param onFailure - Optional handler for failure case (default: toErrorResponse)
 * @returns HTTP response
 *
 * @example
 * ```typescript
 * app.get('/api/users/:id', async (c) => {
 *     const result = await userOperations.findById(c.req.param('id'));
 *     return matchResult(c, result,
 *         (user) => c.json(toUserDto(user)),
 *         (error) => {
 *             // Custom error handling
 *             if (error.code === 'USER_NOT_FOUND') {
 *                 return c.json({ error: 'User not found' }, 404);
 *             }
 *             return c.json(toErrorResponse(error), getErrorStatus(error));
 *         }
 *     );
 * });
 * ```
 */
export function matchResult<T>(
	c: Context<FlowCatalystEnv>,
	result: Result<T>,
	onSuccess: (value: T) => Response,
	onFailure?: (error: UseCaseError) => Response,
): Response {
	if (Result.isSuccess(result)) {
		return onSuccess(result.value);
	}

	if (onFailure) {
		return onFailure(result.error);
	}

	const status = getErrorStatus(result.error);
	const response = toErrorResponse(result.error);
	return c.json(response, status as ContentfulStatusCode);
}

/**
 * Create a success JSON response.
 *
 * @param c - Hono context
 * @param data - Response data
 * @param status - HTTP status (default: 200)
 * @returns HTTP response
 */
export function jsonSuccess<T extends object>(c: Context, data: T, status: number = 200): Response {
	return c.json(data, status as ContentfulStatusCode);
}

/**
 * Create a created (201) JSON response.
 *
 * @param c - Hono context
 * @param data - Response data
 * @returns HTTP response
 */
export function jsonCreated<T extends object>(c: Context, data: T): Response {
	return c.json(data, 201);
}

/**
 * Create a no content (204) response.
 *
 * @param c - Hono context
 * @returns HTTP response
 */
export function noContent(c: Context): Response {
	return c.body(null, 204);
}

/**
 * Create an error JSON response.
 *
 * @param c - Hono context
 * @param status - HTTP status code
 * @param code - Error code
 * @param message - Error message
 * @param details - Optional error details
 * @returns HTTP response
 */
export function jsonError(
	c: Context,
	status: number,
	code: string,
	message: string,
	details?: Record<string, unknown>,
): Response {
	const response: ErrorResponse = {
		code,
		message,
		...(details ? { details } : {}),
	};
	return c.json(response, status as ContentfulStatusCode);
}

/**
 * Create a not found (404) error response.
 *
 * @param c - Hono context
 * @param message - Error message (default: 'Not found')
 * @returns HTTP response
 */
export function notFound(c: Context, message: string = 'Not found'): Response {
	return jsonError(c, 404, 'NOT_FOUND', message);
}

/**
 * Create an unauthorized (401) error response.
 *
 * @param c - Hono context
 * @param message - Error message (default: 'Authentication required')
 * @returns HTTP response
 */
export function unauthorized(c: Context, message: string = 'Authentication required'): Response {
	return jsonError(c, 401, 'UNAUTHORIZED', message);
}

/**
 * Create a forbidden (403) error response.
 *
 * @param c - Hono context
 * @param message - Error message (default: 'Access denied')
 * @returns HTTP response
 */
export function forbidden(c: Context, message: string = 'Access denied'): Response {
	return jsonError(c, 403, 'FORBIDDEN', message);
}

/**
 * Create a bad request (400) error response.
 *
 * @param c - Hono context
 * @param message - Error message
 * @param details - Optional validation details
 * @returns HTTP response
 */
export function badRequest(c: Context, message: string, details?: Record<string, unknown>): Response {
	return jsonError(c, 400, 'BAD_REQUEST', message, details);
}
