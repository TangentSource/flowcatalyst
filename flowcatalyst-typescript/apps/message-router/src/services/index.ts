import type { Logger } from '@flowcatalyst/logging';
import { CircuitBreakerManager, defaultCircuitBreakerConfig } from '@flowcatalyst/queue-core';
import { HealthService } from './health-service.js';
import { WarningService } from './warning-service.js';
import { QueueManagerService } from './queue-manager-service.js';
import { SeederService } from './seeder-service.js';
import { createNotificationService, type BatchingNotificationService } from '../notifications/index.js';
import { createTrafficManager, type TrafficManager } from '../traffic/index.js';
import { env } from '../env.js';

/**
 * All application services
 */
export interface Services {
	health: HealthService;
	warnings: WarningService;
	queueManager: QueueManagerService;
	circuitBreakers: CircuitBreakerManager;
	seeder: SeederService;
	notifications: BatchingNotificationService;
	traffic: TrafficManager;
}

/**
 * Create all services
 */
export function createServices(logger: Logger): Services {
	const warnings = new WarningService(logger);
	const circuitBreakers = new CircuitBreakerManager(defaultCircuitBreakerConfig, logger);

	// Create traffic manager (for standby mode support)
	const traffic = createTrafficManager(
		{
			enabled: env.TRAFFIC_MANAGEMENT_ENABLED,
			strategyName: env.TRAFFIC_STRATEGY_NAME,
		},
		logger,
	);

	const queueManager = new QueueManagerService(circuitBreakers, warnings, traffic, logger);
	const health = new HealthService(queueManager, warnings, logger);
	const seeder = new SeederService(queueManager, logger);

	// Create notification service
	const notifications = createNotificationService(
		{
			enabled: env.NOTIFICATION_ENABLED,
			batchIntervalMs: env.NOTIFICATION_BATCH_INTERVAL_MS,
			minSeverity: env.NOTIFICATION_MIN_SEVERITY,
			instanceId: env.INSTANCE_ID,
			email: {
				enabled: env.NOTIFICATION_EMAIL_ENABLED,
				from: env.NOTIFICATION_EMAIL_FROM,
				to: env.NOTIFICATION_EMAIL_TO,
				smtp: {
					host: env.SMTP_HOST,
					port: env.SMTP_PORT,
					secure: env.SMTP_SECURE,
					username: env.SMTP_USERNAME,
					password: env.SMTP_PASSWORD,
				},
			},
			teams: {
				enabled: env.NOTIFICATION_TEAMS_ENABLED,
				webhookUrl: env.NOTIFICATION_TEAMS_WEBHOOK_URL,
			},
		},
		logger,
	);

	// Connect notification service to warning service
	warnings.setNotificationService(notifications);

	// Start queue manager if enabled
	if (env.MESSAGE_ROUTER_ENABLED) {
		queueManager.start().catch((err) => {
			logger.error({ err }, 'Failed to start queue manager');
		});
	}

	return {
		health,
		warnings,
		queueManager,
		circuitBreakers,
		seeder,
		notifications,
		traffic,
	};
}
