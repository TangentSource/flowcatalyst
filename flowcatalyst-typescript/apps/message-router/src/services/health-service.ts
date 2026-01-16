import type { Logger } from '@flowcatalyst/logging';
import type { MonitoringHealthResponse } from '@flowcatalyst/shared-types';
import type { QueueManagerService } from './queue-manager-service.js';
import type { WarningService } from './warning-service.js';

/**
 * Simple health check result
 */
export interface HealthCheckResult {
	healthy: boolean;
	issues: string[];
}

/**
 * Health service for liveness, readiness, and system health
 */
export class HealthService {
	private readonly queueManager: QueueManagerService;
	private readonly warnings: WarningService;
	private readonly logger: Logger;
	private readonly startTime: number;
	private started = false;

	constructor(
		queueManager: QueueManagerService,
		warnings: WarningService,
		logger: Logger,
	) {
		this.queueManager = queueManager;
		this.warnings = warnings;
		this.logger = logger.child({ component: 'HealthService' });
		this.startTime = Date.now();

		// Mark as started after a short delay
		setTimeout(() => {
			this.started = true;
		}, 1000);
	}

	/**
	 * Liveness check - is the process alive?
	 */
	getLiveness(): HealthCheckResult {
		return {
			healthy: true,
			issues: [],
		};
	}

	/**
	 * Readiness check - is the service ready to accept traffic?
	 */
	getReadiness(): HealthCheckResult {
		const issues: string[] = [];

		if (!this.started) {
			issues.push('Service still starting');
		}

		// Check if queue manager is running
		if (!this.queueManager.isRunning()) {
			issues.push('Queue manager not running');
		}

		return {
			healthy: issues.length === 0,
			issues,
		};
	}

	/**
	 * Startup check - has the service completed startup?
	 */
	getStartup(): HealthCheckResult {
		return {
			healthy: this.started,
			issues: this.started ? [] : ['Service still starting'],
		};
	}

	/**
	 * Get system health overview - matches Java MonitoringHealthResponse
	 */
	getSystemHealth(): MonitoringHealthResponse {
		const queueStats = this.queueManager.getQueueStats();
		const poolStats = this.queueManager.getPoolStats();
		const allWarnings = this.warnings.getAll();

		const totalQueues = Object.keys(queueStats).length;
		const healthyQueues = Object.values(queueStats).filter(
			(s) => s.successRate30min >= 0.9,
		).length;

		const totalPools = Object.keys(poolStats).length;
		const healthyPools = Object.values(poolStats).filter(
			(s) => s.successRate30min >= 0.9,
		).length;

		const activeWarnings = allWarnings.filter((w) => !w.acknowledged).length;
		const criticalWarnings = allWarnings.filter(
			(w) => !w.acknowledged && w.severity === 'CRITICAL',
		).length;

		// Determine overall status
		let status = 'HEALTHY';
		let degradationReason: string | null = null;

		if (criticalWarnings > 0) {
			status = 'DEGRADED';
			degradationReason = `${criticalWarnings} critical warnings`;
		} else if (activeWarnings > 5) {
			status = 'WARNING';
			degradationReason = `${activeWarnings} active warnings`;
		} else if (healthyQueues < totalQueues) {
			status = 'WARNING';
			degradationReason = `${totalQueues - healthyQueues} unhealthy queues`;
		} else if (healthyPools < totalPools) {
			status = 'WARNING';
			degradationReason = `${totalPools - healthyPools} unhealthy pools`;
		}

		return {
			status,
			timestamp: new Date().toISOString(),
			uptimeMillis: Date.now() - this.startTime,
			details: {
				totalQueues,
				healthyQueues,
				totalPools,
				healthyPools,
				activeWarnings,
				criticalWarnings,
				circuitBreakersOpen: 0, // TODO: Track from circuit breaker manager
				degradationReason,
			},
		};
	}
}
