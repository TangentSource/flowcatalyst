import type { Logger } from '@flowcatalyst/logging';
import type { TrafficConfig } from './types.js';
import { TrafficManager } from './traffic-manager.js';

export * from './types.js';
export * from './traffic-manager.js';

/**
 * Create traffic manager from environment configuration
 *
 * Note: Traffic management is about standby mode support
 * (load balancer registration), NOT rate limiting.
 * Per-pool rate limiting is handled in ProcessPool.
 */
export function createTrafficManager(
	config: {
		enabled: boolean;
		strategyName?: string;
	},
	logger: Logger,
): TrafficManager {
	const trafficConfig: TrafficConfig = {
		enabled: config.enabled,
		strategyName: config.strategyName,
	};

	return new TrafficManager(trafficConfig, logger);
}
