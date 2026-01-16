import type { Logger } from '@flowcatalyst/logging';
import type { TrafficConfig, TrafficMode, TrafficStats } from './types.js';

/**
 * Traffic manager for standby mode support
 *
 * Matches Java TrafficManagementService behavior:
 * - Manages PRIMARY/STANDBY mode transitions
 * - Handles load balancer registration/deregistration
 *
 * Note: Per-pool rate limiting and concurrency are handled in ProcessPool,
 * NOT in this traffic manager.
 */
export class TrafficManager {
	private readonly config: TrafficConfig;
	private readonly logger: Logger;
	private mode: TrafficMode = 'PRIMARY';
	private isRegistered = false;

	constructor(config: TrafficConfig, logger: Logger) {
		this.config = config;
		this.logger = logger.child({ component: 'TrafficManager' });

		this.logger.info(
			{
				enabled: config.enabled,
				strategyName: config.strategyName,
			},
			'Traffic manager initialized',
		);
	}

	/**
	 * Start traffic management
	 */
	start(): void {
		if (this.config.enabled) {
			// Register as active when starting in PRIMARY mode
			if (this.mode === 'PRIMARY') {
				this.registerAsActive();
			}
			this.logger.info({ mode: this.mode }, 'Traffic manager started');
		}
	}

	/**
	 * Stop traffic management
	 */
	stop(): void {
		if (this.isRegistered) {
			this.deregisterFromActive();
		}
		this.logger.info('Traffic manager stopped');
	}

	/**
	 * Switch to PRIMARY mode
	 */
	becomePrimary(): void {
		if (this.mode === 'PRIMARY') {
			return;
		}

		this.logger.info('Transitioning to PRIMARY mode');
		this.mode = 'PRIMARY';

		if (this.config.enabled) {
			this.registerAsActive();
		}
	}

	/**
	 * Switch to STANDBY mode
	 */
	becomeStandby(): void {
		if (this.mode === 'STANDBY') {
			return;
		}

		this.logger.info('Transitioning to STANDBY mode');
		this.mode = 'STANDBY';

		if (this.isRegistered) {
			this.deregisterFromActive();
		}
	}

	/**
	 * Check if this instance is in PRIMARY mode
	 */
	isPrimary(): boolean {
		return this.mode === 'PRIMARY';
	}

	/**
	 * Check if this instance is in STANDBY mode
	 */
	isStandby(): boolean {
		return this.mode === 'STANDBY';
	}

	/**
	 * Get current mode
	 */
	getMode(): TrafficMode {
		return this.mode;
	}

	/**
	 * Check if registered with load balancer
	 */
	isRegisteredWithLoadBalancer(): boolean {
		return this.isRegistered;
	}

	/**
	 * Get traffic statistics
	 */
	getStats(): TrafficStats {
		return {
			enabled: this.config.enabled,
			mode: this.mode,
			isRegistered: this.isRegistered,
			strategyName: this.config.strategyName || 'NONE',
		};
	}

	/**
	 * Register this instance as active with load balancer
	 * Override this method to implement specific load balancer strategies
	 */
	protected registerAsActive(): void {
		// Base implementation just sets the flag
		// Subclasses can override for AWS ALB, etc.
		this.isRegistered = true;
		this.logger.info(
			{ strategyName: this.config.strategyName },
			'Registered as active',
		);
	}

	/**
	 * Deregister this instance from load balancer
	 * Override this method to implement specific load balancer strategies
	 */
	protected deregisterFromActive(): void {
		// Base implementation just clears the flag
		// Subclasses can override for AWS ALB, etc.
		this.isRegistered = false;
		this.logger.info(
			{ strategyName: this.config.strategyName },
			'Deregistered from active',
		);
	}
}
