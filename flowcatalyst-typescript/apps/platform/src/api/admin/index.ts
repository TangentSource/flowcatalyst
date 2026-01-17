/**
 * Admin API
 *
 * Administrative endpoints for platform management.
 */

import { Hono } from 'hono';
import type { FlowCatalystEnv } from '@flowcatalyst/http';

import { createUsersApi, type UsersApiDeps } from './users.js';

/**
 * Dependencies for the admin API.
 */
export interface AdminApiDeps extends UsersApiDeps {}

/**
 * Create the admin API routes.
 */
export function createAdminApi(deps: AdminApiDeps): Hono<FlowCatalystEnv> {
	const app = new Hono<FlowCatalystEnv>();

	// Mount users API
	app.route('/users', createUsersApi(deps));

	return app;
}

export { type UsersApiDeps } from './users.js';
