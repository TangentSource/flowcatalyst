import { createRoute, OpenAPIHono, z } from '@hono/zod-openapi';
import {
	MonitoringHealthResponseSchema,
	QueueStatsSchema,
	PoolStatsSchema,
	WarningSchema,
	CircuitBreakerStatsSchema,
	InFlightMessageSchema,
	StandbyStatusResponseSchema,
	TrafficStatusResponseSchema,
	ConsumerHealthResponseSchema,
	StatusResponseSchema,
	WarningAcknowledgeResponseSchema,
	CircuitBreakerStateResponseSchema,
} from '@flowcatalyst/shared-types';
import type { AppContext } from '../app.js';

export const monitoringRoutes = new OpenAPIHono<AppContext>();

/**
 * GET /monitoring/health - System health overview
 */
const healthRoute = createRoute({
	method: 'get',
	path: '/health',
	tags: ['Monitoring'],
	summary: 'Get system health',
	responses: {
		200: {
			description: 'System health status',
			content: {
				'application/json': {
					schema: MonitoringHealthResponseSchema,
				},
			},
		},
	},
});

monitoringRoutes.openapi(healthRoute, (c) => {
	const services = c.get('services');
	const health = services.health.getSystemHealth();
	return c.json(health);
});

/**
 * GET /monitoring/queue-stats - Queue statistics
 */
const queueStatsRoute = createRoute({
	method: 'get',
	path: '/queue-stats',
	tags: ['Monitoring'],
	summary: 'Get queue statistics',
	responses: {
		200: {
			description: 'Queue statistics by queue name',
			content: {
				'application/json': {
					schema: z.record(z.string(), QueueStatsSchema),
				},
			},
		},
	},
});

monitoringRoutes.openapi(queueStatsRoute, (c) => {
	const services = c.get('services');
	const stats = services.queueManager.getQueueStats();
	return c.json(stats);
});

/**
 * GET /monitoring/pool-stats - Pool statistics
 */
const poolStatsRoute = createRoute({
	method: 'get',
	path: '/pool-stats',
	tags: ['Monitoring'],
	summary: 'Get pool statistics',
	responses: {
		200: {
			description: 'Pool statistics by pool code',
			content: {
				'application/json': {
					schema: z.record(z.string(), PoolStatsSchema),
				},
			},
		},
	},
});

monitoringRoutes.openapi(poolStatsRoute, (c) => {
	const services = c.get('services');
	const stats = services.queueManager.getPoolStats();
	return c.json(stats);
});

/**
 * GET /monitoring/warnings - All warnings
 */
const warningsRoute = createRoute({
	method: 'get',
	path: '/warnings',
	tags: ['Monitoring'],
	summary: 'Get all warnings',
	responses: {
		200: {
			description: 'List of warnings',
			content: {
				'application/json': {
					schema: z.array(WarningSchema),
				},
			},
		},
	},
});

monitoringRoutes.openapi(warningsRoute, (c) => {
	const services = c.get('services');
	const warnings = services.warnings.getAll();
	return c.json(warnings);
});

/**
 * GET /monitoring/warnings/unacknowledged - Unacknowledged warnings
 */
const unacknowledgedWarningsRoute = createRoute({
	method: 'get',
	path: '/warnings/unacknowledged',
	tags: ['Monitoring'],
	summary: 'Get unacknowledged warnings',
	responses: {
		200: {
			description: 'List of unacknowledged warnings',
			content: {
				'application/json': {
					schema: z.array(WarningSchema),
				},
			},
		},
	},
});

monitoringRoutes.openapi(unacknowledgedWarningsRoute, (c) => {
	const services = c.get('services');
	const warnings = services.warnings.getUnacknowledged();
	return c.json(warnings);
});

/**
 * GET /monitoring/warnings/severity/:severity - Warnings by severity
 */
const warningsBySeverityRoute = createRoute({
	method: 'get',
	path: '/warnings/severity/{severity}',
	tags: ['Monitoring'],
	summary: 'Get warnings by severity',
	request: {
		params: z.object({
			severity: z.string(),
		}),
	},
	responses: {
		200: {
			description: 'List of warnings filtered by severity',
			content: {
				'application/json': {
					schema: z.array(WarningSchema),
				},
			},
		},
	},
});

monitoringRoutes.openapi(warningsBySeverityRoute, (c) => {
	const services = c.get('services');
	const { severity } = c.req.valid('param');
	const warnings = services.warnings.getBySeverity(severity);
	return c.json(warnings);
});

/**
 * POST /monitoring/warnings/:warningId/acknowledge - Acknowledge warning
 */
const acknowledgeWarningRoute = createRoute({
	method: 'post',
	path: '/warnings/{warningId}/acknowledge',
	tags: ['Monitoring'],
	summary: 'Acknowledge a warning',
	request: {
		params: z.object({
			warningId: z.string(),
		}),
	},
	responses: {
		200: {
			description: 'Warning acknowledged',
			content: {
				'application/json': {
					schema: WarningAcknowledgeResponseSchema,
				},
			},
		},
		404: {
			description: 'Warning not found',
			content: {
				'application/json': {
					schema: WarningAcknowledgeResponseSchema,
				},
			},
		},
	},
});

monitoringRoutes.openapi(acknowledgeWarningRoute, (c) => {
	const services = c.get('services');
	const { warningId } = c.req.valid('param');
	const acknowledged = services.warnings.acknowledge(warningId);

	if (acknowledged) {
		return c.json({ status: 'success' });
	}
	return c.json({ status: 'error', message: 'Warning not found' }, 404);
});

/**
 * DELETE /monitoring/warnings - Clear all warnings
 */
const clearWarningsRoute = createRoute({
	method: 'delete',
	path: '/warnings',
	tags: ['Monitoring'],
	summary: 'Clear all warnings',
	responses: {
		200: {
			description: 'Warnings cleared',
			content: {
				'application/json': {
					schema: StatusResponseSchema,
				},
			},
		},
	},
});

monitoringRoutes.openapi(clearWarningsRoute, (c) => {
	const services = c.get('services');
	services.warnings.clearAll();
	return c.json({ status: 'success' });
});

/**
 * DELETE /monitoring/warnings/old - Clear old warnings
 */
const clearOldWarningsRoute = createRoute({
	method: 'delete',
	path: '/warnings/old',
	tags: ['Monitoring'],
	summary: 'Clear old warnings',
	request: {
		query: z.object({
			hours: z.string().transform(Number).default('24'),
		}),
	},
	responses: {
		200: {
			description: 'Old warnings cleared',
			content: {
				'application/json': {
					schema: StatusResponseSchema,
				},
			},
		},
	},
});

monitoringRoutes.openapi(clearOldWarningsRoute, (c) => {
	const services = c.get('services');
	const { hours } = c.req.valid('query');
	services.warnings.clearOlderThan(hours);
	return c.json({ status: 'success' });
});

/**
 * GET /monitoring/circuit-breakers - Circuit breaker stats
 */
const circuitBreakersRoute = createRoute({
	method: 'get',
	path: '/circuit-breakers',
	tags: ['Monitoring'],
	summary: 'Get circuit breaker statistics',
	responses: {
		200: {
			description: 'Circuit breaker statistics',
			content: {
				'application/json': {
					schema: z.record(z.string(), CircuitBreakerStatsSchema),
				},
			},
		},
	},
});

monitoringRoutes.openapi(circuitBreakersRoute, (c) => {
	const services = c.get('services');
	const stats = services.circuitBreakers.getAllStats();
	return c.json(stats);
});

/**
 * GET /monitoring/circuit-breakers/:name/state - Circuit breaker state
 */
const circuitBreakerStateRoute = createRoute({
	method: 'get',
	path: '/circuit-breakers/{name}/state',
	tags: ['Monitoring'],
	summary: 'Get circuit breaker state',
	request: {
		params: z.object({
			name: z.string(),
		}),
	},
	responses: {
		200: {
			description: 'Circuit breaker state',
			content: {
				'application/json': {
					schema: CircuitBreakerStateResponseSchema,
				},
			},
		},
	},
});

monitoringRoutes.openapi(circuitBreakerStateRoute, (c) => {
	const services = c.get('services');
	const { name } = c.req.valid('param');
	const breaker = services.circuitBreakers.getAll().get(decodeURIComponent(name));

	if (breaker) {
		return c.json({ name, state: breaker.getState() });
	}
	return c.json({ name, state: 'UNKNOWN' });
});

/**
 * POST /monitoring/circuit-breakers/:name/reset - Reset circuit breaker
 */
const resetCircuitBreakerRoute = createRoute({
	method: 'post',
	path: '/circuit-breakers/{name}/reset',
	tags: ['Monitoring'],
	summary: 'Reset circuit breaker',
	request: {
		params: z.object({
			name: z.string(),
		}),
	},
	responses: {
		200: {
			description: 'Circuit breaker reset',
			content: {
				'application/json': {
					schema: StatusResponseSchema,
				},
			},
		},
	},
});

monitoringRoutes.openapi(resetCircuitBreakerRoute, (c) => {
	const services = c.get('services');
	const { name } = c.req.valid('param');
	const success = services.circuitBreakers.reset(decodeURIComponent(name));

	if (success) {
		return c.json({ status: 'success' });
	}
	return c.json({ status: 'error', message: 'Circuit breaker not found' }, 500);
});

/**
 * POST /monitoring/circuit-breakers/reset-all - Reset all circuit breakers
 */
const resetAllCircuitBreakersRoute = createRoute({
	method: 'post',
	path: '/circuit-breakers/reset-all',
	tags: ['Monitoring'],
	summary: 'Reset all circuit breakers',
	responses: {
		200: {
			description: 'All circuit breakers reset',
			content: {
				'application/json': {
					schema: StatusResponseSchema,
				},
			},
		},
	},
});

monitoringRoutes.openapi(resetAllCircuitBreakersRoute, (c) => {
	const services = c.get('services');
	services.circuitBreakers.resetAll();
	return c.json({ status: 'success' });
});

/**
 * GET /monitoring/in-flight-messages - In-flight messages
 */
const inFlightMessagesRoute = createRoute({
	method: 'get',
	path: '/in-flight-messages',
	tags: ['Monitoring'],
	summary: 'Get in-flight messages',
	request: {
		query: z.object({
			limit: z.string().transform(Number).default('100'),
			messageId: z.string().optional(),
		}),
	},
	responses: {
		200: {
			description: 'In-flight messages',
			content: {
				'application/json': {
					schema: z.array(InFlightMessageSchema),
				},
			},
		},
	},
});

monitoringRoutes.openapi(inFlightMessagesRoute, (c) => {
	const services = c.get('services');
	const { limit, messageId } = c.req.valid('query');
	const messages = services.queueManager.getInFlightMessages(limit, messageId);
	return c.json(messages);
});

/**
 * GET /monitoring/standby-status - Standby status
 */
const standbyStatusRoute = createRoute({
	method: 'get',
	path: '/standby-status',
	tags: ['Monitoring'],
	summary: 'Get standby status',
	responses: {
		200: {
			description: 'Standby status',
			content: {
				'application/json': {
					schema: StandbyStatusResponseSchema,
				},
			},
		},
	},
});

monitoringRoutes.openapi(standbyStatusRoute, (c) => {
	// Standby mode not implemented yet
	return c.json({ standbyEnabled: false });
});

/**
 * GET /monitoring/traffic-status - Traffic status
 */
const trafficStatusRoute = createRoute({
	method: 'get',
	path: '/traffic-status',
	tags: ['Monitoring'],
	summary: 'Get traffic status',
	responses: {
		200: {
			description: 'Traffic status',
			content: {
				'application/json': {
					schema: TrafficStatusResponseSchema,
				},
			},
		},
	},
});

monitoringRoutes.openapi(trafficStatusRoute, (c) => {
	// Traffic management not implemented yet
	return c.json({ enabled: false, message: 'Traffic management not available' });
});

/**
 * GET /monitoring/consumer-health - Consumer health
 */
const consumerHealthRoute = createRoute({
	method: 'get',
	path: '/consumer-health',
	tags: ['Monitoring'],
	summary: 'Get consumer health',
	responses: {
		200: {
			description: 'Consumer health status',
			content: {
				'application/json': {
					schema: ConsumerHealthResponseSchema,
				},
			},
		},
	},
});

monitoringRoutes.openapi(consumerHealthRoute, (c) => {
	const services = c.get('services');
	const health = services.queueManager.getConsumerHealth();
	return c.json(health);
});

/**
 * GET /monitoring/dashboard - Dashboard UI
 */
const dashboardRoute = createRoute({
	method: 'get',
	path: '/dashboard',
	tags: ['Monitoring'],
	summary: 'Dashboard UI',
	description: 'Returns the monitoring dashboard HTML page',
	responses: {
		200: {
			description: 'Dashboard HTML page',
			content: {
				'text/html': {
					schema: z.string(),
				},
			},
		},
		404: {
			description: 'Dashboard not found',
		},
	},
});

monitoringRoutes.openapi(dashboardRoute, async (c) => {
	try {
		const fs = await import('node:fs/promises');
		const path = await import('node:path');
		const { fileURLToPath } = await import('node:url');

		const __filename = fileURLToPath(import.meta.url);
		const __dirname = path.dirname(__filename);
		const dashboardPath = path.join(__dirname, '../../public/dashboard.html');

		const html = await fs.readFile(dashboardPath, 'utf-8');
		return c.html(html);
	} catch {
		return c.text('Dashboard not found', 404);
	}
});
