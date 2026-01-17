/**
 * API Layer
 *
 * REST API endpoints for the platform service.
 */

import { Hono } from 'hono';
import type { FlowCatalystEnv } from '@flowcatalyst/http';

import { createAdminApi, type AdminApiDeps } from './admin/index.js';

/**
 * Dependencies for all APIs.
 */
export interface ApiDeps extends AdminApiDeps {}

/**
 * Create all API routes.
 */
export function createApi(deps: ApiDeps): Hono<FlowCatalystEnv> {
	const app = new Hono<FlowCatalystEnv>();

	// Mount admin API
	app.route('/admin', createAdminApi(deps));

	return app;
}

export { type AdminApiDeps } from './admin/index.js';
