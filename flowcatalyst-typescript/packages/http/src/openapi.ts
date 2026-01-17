/**
 * OpenAPI Integration
 *
 * Utilities for OpenAPI documentation and Zod schema integration
 * with Hono applications.
 */

import { z } from 'zod';

/**
 * Common Zod schemas for FlowCatalyst APIs.
 */
export const CommonSchemas = {
	/**
	 * TSID - 13-character Crockford Base32 string.
	 */
	tsid: z.string().length(13).regex(/^[0-9A-HJKMNP-TV-Z]{13}$/i, 'Invalid TSID format'),

	/**
	 * Typed ID - entity prefix + underscore + TSID (e.g., "user_0HZXEQ5Y8JY5Z").
	 */
	typedId: z.string().regex(/^[a-z]+_[0-9A-HJKMNP-TV-Z]{13}$/i, 'Invalid typed ID format'),

	/**
	 * ISO 8601 datetime string.
	 */
	datetime: z.string().datetime(),

	/**
	 * Email address.
	 */
	email: z.string().email(),

	/**
	 * Non-empty string.
	 */
	nonEmptyString: z.string().min(1),

	/**
	 * Pagination parameters.
	 */
	pagination: z.object({
		page: z.coerce.number().int().min(0).default(0),
		pageSize: z.coerce.number().int().min(1).max(100).default(20),
	}),
};

/**
 * Standard error response schema.
 */
export const ErrorResponseSchema = z.object({
	message: z.string().describe('Human-readable error message'),
	code: z.string().describe('Machine-readable error code'),
	details: z.record(z.unknown()).optional().describe('Additional error details'),
});

/**
 * Paginated response wrapper schema.
 *
 * @param itemSchema - Schema for individual items
 * @returns Schema for paginated response
 */
export function paginatedResponse<T extends z.ZodTypeAny>(itemSchema: T) {
	return z.object({
		items: z.array(itemSchema),
		page: z.number().int().min(0),
		pageSize: z.number().int().min(1),
		totalItems: z.number().int().min(0),
		totalPages: z.number().int().min(0),
		hasNext: z.boolean(),
		hasPrevious: z.boolean(),
	});
}

/**
 * Create a schema with common entity fields.
 *
 * @param fields - Entity-specific fields
 * @returns Schema with id, createdAt, updatedAt
 */
export function entitySchema<T extends z.ZodRawShape>(fields: T) {
	return z.object({
		id: CommonSchemas.tsid.describe('Unique entity ID'),
		createdAt: CommonSchemas.datetime.describe('When the entity was created'),
		updatedAt: CommonSchemas.datetime.describe('When the entity was last updated'),
		...fields,
	});
}

/**
 * OpenAPI response definitions for common status codes.
 */
export const OpenAPIResponses = {
	/** 200 OK */
	ok: <T extends z.ZodTypeAny>(schema: T, description: string = 'Successful response') => ({
		200: {
			description,
			content: { 'application/json': { schema } },
		},
	}),

	/** 201 Created */
	created: <T extends z.ZodTypeAny>(schema: T, description: string = 'Resource created') => ({
		201: {
			description,
			content: { 'application/json': { schema } },
		},
	}),

	/** 204 No Content */
	noContent: (description: string = 'No content') => ({
		204: { description },
	}),

	/** 400 Bad Request */
	badRequest: (description: string = 'Invalid request') => ({
		400: {
			description,
			content: { 'application/json': { schema: ErrorResponseSchema } },
		},
	}),

	/** 401 Unauthorized */
	unauthorized: (description: string = 'Authentication required') => ({
		401: {
			description,
			content: { 'application/json': { schema: ErrorResponseSchema } },
		},
	}),

	/** 403 Forbidden */
	forbidden: (description: string = 'Access denied') => ({
		403: {
			description,
			content: { 'application/json': { schema: ErrorResponseSchema } },
		},
	}),

	/** 404 Not Found */
	notFound: (description: string = 'Resource not found') => ({
		404: {
			description,
			content: { 'application/json': { schema: ErrorResponseSchema } },
		},
	}),

	/** 409 Conflict */
	conflict: (description: string = 'Conflict') => ({
		409: {
			description,
			content: { 'application/json': { schema: ErrorResponseSchema } },
		},
	}),

	/** 500 Internal Server Error */
	serverError: (description: string = 'Internal server error') => ({
		500: {
			description,
			content: { 'application/json': { schema: ErrorResponseSchema } },
		},
	}),
};

/**
 * Combine multiple response definitions.
 *
 * @param responses - Response definitions to combine
 * @returns Combined response definitions
 *
 * @example
 * ```typescript
 * const responses = combineResponses(
 *     OpenAPIResponses.ok(UserSchema, 'User details'),
 *     OpenAPIResponses.notFound('User not found'),
 *     OpenAPIResponses.unauthorized(),
 * );
 * ```
 */
export function combineResponses(
	...responses: Array<Record<number, unknown>>
): Record<number, unknown> {
	return Object.assign({}, ...responses);
}

/**
 * Validate request body against a Zod schema.
 *
 * @param body - Request body
 * @param schema - Zod schema to validate against
 * @returns Validated and typed body
 * @throws ZodError if validation fails
 */
export function validateBody<T extends z.ZodTypeAny>(body: unknown, schema: T): z.infer<T> {
	return schema.parse(body);
}

/**
 * Validate query parameters against a Zod schema.
 *
 * @param query - Query parameters object
 * @param schema - Zod schema to validate against
 * @returns Validated and typed query parameters
 * @throws ZodError if validation fails
 */
export function validateQuery<T extends z.ZodTypeAny>(
	query: Record<string, string | string[] | undefined>,
	schema: T,
): z.infer<T> {
	return schema.parse(query);
}

/**
 * Safe validation that returns a Result-like object instead of throwing.
 *
 * @param data - Data to validate
 * @param schema - Zod schema to validate against
 * @returns Object with success flag and either data or error
 */
export function safeValidate<T extends z.ZodTypeAny>(
	data: unknown,
	schema: T,
): { success: true; data: z.infer<T> } | { success: false; error: z.ZodError } {
	const result = schema.safeParse(data);
	if (result.success) {
		return { success: true, data: result.data };
	}
	return { success: false, error: result.error };
}
