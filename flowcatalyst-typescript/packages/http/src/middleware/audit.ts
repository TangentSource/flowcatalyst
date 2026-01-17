/**
 * Audit Middleware
 *
 * Hono middleware for authentication and audit context. Extracts principal ID
 * from session cookie or Bearer token and populates the audit context.
 *
 * This middleware integrates with AuditContext from @flowcatalyst/domain-core
 * for consistent principal tracking across the application.
 */

import type { MiddlewareHandler } from 'hono';
import { getCookie } from 'hono/cookie';
import { HTTPException } from 'hono/http-exception';
import type { FlowCatalystEnv, AuditMiddlewareConfig, AuditData } from '../types.js';

/**
 * Create audit middleware for Hono.
 *
 * Attempts to authenticate the request using:
 * 1. Session cookie (preferred for browser clients)
 * 2. Bearer token in Authorization header (for API clients)
 *
 * @param config - Configuration including token validation function
 * @returns Hono middleware handler
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono';
 * import { auditMiddleware } from '@flowcatalyst/http';
 *
 * const app = new Hono<FlowCatalystEnv>();
 *
 * app.use('*', auditMiddleware({
 *     sessionCookieName: 'session',
 *     skipPaths: ['/health', '/metrics'],
 *     validateToken: async (token) => {
 *         // Validate JWT and return principal ID
 *         const claims = await verifyJwt(token);
 *         return claims?.sub ?? null;
 *     },
 * }));
 *
 * app.get('/api/me', (c) => {
 *     const { principalId } = c.get('audit');
 *     if (!principalId) {
 *         return c.json({ error: 'Not authenticated' }, 401);
 *     }
 *     return c.json({ principalId });
 * });
 * ```
 */
export function auditMiddleware(config: AuditMiddlewareConfig): MiddlewareHandler<FlowCatalystEnv> {
	const { sessionCookieName = 'session', skipPaths = [], validateToken, loadPrincipal } = config;

	return async (c, next) => {
		const path = new URL(c.req.url).pathname;

		// Skip authentication for specified paths
		for (const skipPath of skipPaths) {
			if (path.startsWith(skipPath)) {
				c.set('audit', { principalId: null, principal: null });
				return next();
			}
		}

		let principalId: string | null = null;

		// Try session cookie first
		const sessionToken = getCookie(c, sessionCookieName);
		if (sessionToken) {
			principalId = await validateToken(sessionToken);
		}

		// Fall back to Bearer token
		if (!principalId) {
			const authHeader = c.req.header('Authorization');
			if (authHeader?.startsWith('Bearer ')) {
				const token = authHeader.substring('Bearer '.length);
				principalId = await validateToken(token);
			}
		}

		// Load full principal if configured and authenticated
		let principal = null;
		if (principalId && loadPrincipal) {
			principal = await loadPrincipal(principalId);
		}

		// Store audit data in context
		const auditData: AuditData = {
			principalId,
			principal,
		};
		c.set('audit', auditData);

		return next();
	};
}

/**
 * Require authentication - throws 401 if not authenticated.
 *
 * @param c - Hono context
 * @returns Principal ID
 * @throws HTTPException with 401 status if not authenticated
 */
export function requireAuth(c: { get: (key: 'audit') => AuditData | undefined }): string {
	const audit = c.get('audit');
	if (!audit?.principalId) {
		throw new HTTPException(401, {
			message: 'Authentication required',
		});
	}
	return audit.principalId;
}

/**
 * Get principal ID if authenticated, null otherwise.
 *
 * @param c - Hono context
 * @returns Principal ID or null
 */
export function getPrincipalId(c: { get: (key: 'audit') => AuditData | undefined }): string | null {
	return c.get('audit')?.principalId ?? null;
}

/**
 * Check if request is authenticated.
 *
 * @param c - Hono context
 * @returns True if authenticated
 */
export function isAuthenticated(c: { get: (key: 'audit') => AuditData | undefined }): boolean {
	return c.get('audit')?.principalId != null;
}

/**
 * Authorization middleware - requires specific role.
 *
 * @param roleName - Required role name
 * @returns Middleware that throws 403 if role not present
 *
 * @example
 * ```typescript
 * app.get('/admin/users', requireRole('admin'), (c) => {
 *     // Only admins can access
 * });
 * ```
 */
export function requireRole(roleName: string): MiddlewareHandler<FlowCatalystEnv> {
	return async (c, next) => {
		const audit = c.get('audit');
		if (!audit?.principal) {
			throw new HTTPException(401, { message: 'Authentication required' });
		}

		if (!audit.principal.roles.has(roleName)) {
			throw new HTTPException(403, { message: 'Insufficient permissions' });
		}

		return next();
	};
}
