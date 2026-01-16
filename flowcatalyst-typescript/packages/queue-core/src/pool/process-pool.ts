import type { Logger } from '@flowcatalyst/logging';
import type {
	PoolConfig,
	PoolState,
	PoolStats,
	ProcessingResult,
	QueueMessage,
} from '@flowcatalyst/shared-types';
import pLimit from 'p-limit';
import PQueue from 'p-queue';
import type { HttpMediator } from '../mediation/http-mediator.js';
import { createRateLimiter, type RateLimiter } from '../rate-limiter.js';
import { MessageGroupHandler } from './message-group-handler.js';

/**
 * Message callback for ack/nack
 */
export interface MessageCallback {
	ack(): Promise<void>;
	nack(visibilityTimeoutSeconds?: number): Promise<void>;
}

/**
 * Process pool implementation - matches Java ProcessPoolImpl
 * Uses per-message-group handlers for FIFO ordering within groups
 */
export class ProcessPool {
	private readonly config: PoolConfig;
	private readonly logger: Logger;
	private readonly mediator: HttpMediator;

	private state: PoolState = 'STARTING';
	private readonly messageGroups = new Map<string, MessageGroupHandler>();
	private readonly concurrencyLimiter: ReturnType<typeof pLimit>;
	private rateLimiter: RateLimiter | null;

	// Statistics tracking
	private totalProcessed = 0;
	private totalSucceeded = 0;
	private totalFailed = 0;
	private totalRateLimited = 0;
	private totalDeferred = 0;
	private processingTimes: number[] = [];

	// Windowed stats (simplified - use sliding window in production)
	private stats5min = { processed: 0, succeeded: 0, failed: 0, rateLimited: 0 };
	private stats30min = { processed: 0, succeeded: 0, failed: 0, rateLimited: 0 };

	// Batch+group failure tracking for FIFO
	private readonly failedBatchGroups = new Set<string>();

	// Capacity management
	private queuedMessages = 0;
	private readonly maxCapacity: number;

	constructor(config: PoolConfig, mediator: HttpMediator, logger: Logger) {
		this.config = config;
		this.mediator = mediator;
		this.logger = logger.child({ component: 'ProcessPool', poolCode: config.code });

		// Concurrency limiter (semaphore pattern)
		this.concurrencyLimiter = pLimit(config.concurrency);

		// Rate limiter (optional)
		this.rateLimiter =
			config.rateLimitPerMinute && config.rateLimitPerMinute > 0
				? createRateLimiter(config.rateLimitPerMinute)
				: null;

		// Capacity = max(concurrency * 2, 50)
		this.maxCapacity = Math.max(config.concurrency * 2, 50);

		this.state = 'RUNNING';
		this.logger.info({ concurrency: config.concurrency, capacity: this.maxCapacity }, 'Pool started');
	}

	/**
	 * Submit a message for processing
	 * Returns false if pool is at capacity or draining
	 */
	async submit(
		message: QueueMessage,
		callback: MessageCallback,
	): Promise<boolean> {
		if (this.state !== 'RUNNING') {
			this.logger.warn({ state: this.state }, 'Pool not accepting messages');
			return false;
		}

		if (this.queuedMessages >= this.maxCapacity) {
			this.logger.warn(
				{ queued: this.queuedMessages, capacity: this.maxCapacity },
				'Pool at capacity',
			);
			return false;
		}

		this.queuedMessages++;

		// Get or create message group handler
		let groupHandler = this.messageGroups.get(message.pointer.messageGroupId);
		if (!groupHandler) {
			groupHandler = new MessageGroupHandler(
				message.pointer.messageGroupId,
				this.processMessage.bind(this),
				() => {
					this.messageGroups.delete(message.pointer.messageGroupId);
					this.logger.debug(
						{ messageGroupId: message.pointer.messageGroupId },
						'Message group handler cleaned up',
					);
				},
				this.logger,
			);
			this.messageGroups.set(message.pointer.messageGroupId, groupHandler);
		}

		// Enqueue for processing
		groupHandler.enqueue(message, callback);
		return true;
	}

	/**
	 * Process a single message (called by message group handler)
	 */
	private async processMessage(
		message: QueueMessage,
		callback: MessageCallback,
	): Promise<void> {
		const startTime = Date.now();
		const batchGroupKey = `${message.batchId}|${message.pointer.messageGroupId}`;

		// Check if batch+group already failed (FIFO preservation)
		if (this.failedBatchGroups.has(batchGroupKey)) {
			this.logger.debug(
				{ messageId: message.messageId, batchGroupKey },
				'Skipping message due to batch+group failure',
			);
			await callback.nack();
			this.queuedMessages--;
			return;
		}

		// Wait for rate limit if configured
		if (this.rateLimiter) {
			const waited = await this.rateLimiter.acquire();
			if (waited) {
				this.totalRateLimited++;
				this.stats5min.rateLimited++;
				this.stats30min.rateLimited++;
			}
		}

		// Acquire concurrency permit
		await this.concurrencyLimiter(async () => {
			try {
				const result = await this.mediator.process(message);
				const durationMs = Date.now() - startTime;

				this.recordProcessingTime(durationMs);
				this.totalProcessed++;
				this.stats5min.processed++;
				this.stats30min.processed++;

				switch (result.outcome) {
					case 'SUCCESS':
						this.totalSucceeded++;
						this.stats5min.succeeded++;
						this.stats30min.succeeded++;
						await callback.ack();
						break;

					case 'ERROR_CONFIG':
						// 4xx errors - ack to prevent infinite retry
						this.totalFailed++;
						this.stats5min.failed++;
						this.stats30min.failed++;
						await callback.ack();
						break;

					case 'DEFERRED':
						// Message not ready - nack with visibility
						this.totalDeferred++;
						await callback.nack(30); // 30 second visibility
						break;

					case 'ERROR_PROCESS':
					case 'BATCH_FAILED':
					default:
						// 5xx or timeout - nack for retry, mark batch+group as failed
						this.totalFailed++;
						this.stats5min.failed++;
						this.stats30min.failed++;
						this.failedBatchGroups.add(batchGroupKey);
						await callback.nack();
						break;
				}
			} catch (error) {
				this.logger.error({ err: error, messageId: message.messageId }, 'Processing error');
				this.totalFailed++;
				this.stats5min.failed++;
				this.stats30min.failed++;
				this.failedBatchGroups.add(batchGroupKey);
				await callback.nack();
			} finally {
				this.queuedMessages--;
			}
		});
	}

	/**
	 * Get pool statistics - matches Java PoolStats exactly
	 */
	getStats(): PoolStats {
		const activeWorkers = this.config.concurrency - this.concurrencyLimiter.activeCount;
		const successRate =
			this.totalProcessed > 0 ? this.totalSucceeded / this.totalProcessed : 1.0;

		return {
			poolCode: this.config.code,
			totalProcessed: this.totalProcessed,
			totalSucceeded: this.totalSucceeded,
			totalFailed: this.totalFailed,
			totalRateLimited: this.totalRateLimited,
			successRate,
			activeWorkers: this.concurrencyLimiter.activeCount,
			availablePermits: this.concurrencyLimiter.pendingCount,
			maxConcurrency: this.config.concurrency,
			queueSize: this.queuedMessages,
			maxQueueCapacity: this.maxCapacity,
			averageProcessingTimeMs: this.getAverageProcessingTime(),
			totalProcessed5min: this.stats5min.processed,
			totalSucceeded5min: this.stats5min.succeeded,
			totalFailed5min: this.stats5min.failed,
			successRate5min:
				this.stats5min.processed > 0
					? this.stats5min.succeeded / this.stats5min.processed
					: 1.0,
			totalProcessed30min: this.stats30min.processed,
			totalSucceeded30min: this.stats30min.succeeded,
			totalFailed30min: this.stats30min.failed,
			successRate30min:
				this.stats30min.processed > 0
					? this.stats30min.succeeded / this.stats30min.processed
					: 1.0,
			totalRateLimited5min: this.stats5min.rateLimited,
			totalRateLimited30min: this.stats30min.rateLimited,
		};
	}

	/**
	 * Update pool configuration in-place (concurrency, rate limit)
	 */
	updateConfig(newConfig: Partial<PoolConfig>): void {
		if (newConfig.rateLimitPerMinute !== undefined) {
			const rateLimit = newConfig.rateLimitPerMinute;
			this.rateLimiter =
				rateLimit !== null && rateLimit > 0 ? createRateLimiter(rateLimit) : null;
			this.logger.info({ rateLimitPerMinute: rateLimit }, 'Rate limit updated');
		}
		// Note: p-limit doesn't support dynamic concurrency changes
		// Would need to recreate the limiter for concurrency changes
	}

	/**
	 * Start draining - stop accepting new messages
	 */
	drain(): void {
		this.state = 'DRAINING';
		this.logger.info('Pool draining started');
	}

	/**
	 * Check if pool is fully drained
	 */
	isDrained(): boolean {
		return this.state === 'DRAINING' && this.queuedMessages === 0;
	}

	/**
	 * Shutdown the pool
	 */
	async shutdown(): Promise<void> {
		this.state = 'STOPPED';
		this.messageGroups.clear();
		this.failedBatchGroups.clear();
		this.logger.info('Pool shutdown complete');
	}

	/**
	 * Get current state
	 */
	getState(): PoolState {
		return this.state;
	}

	/**
	 * Get pool code
	 */
	getCode(): string {
		return this.config.code;
	}

	private recordProcessingTime(durationMs: number): void {
		this.processingTimes.push(durationMs);
		// Keep last 1000 samples
		if (this.processingTimes.length > 1000) {
			this.processingTimes.shift();
		}
	}

	private getAverageProcessingTime(): number {
		if (this.processingTimes.length === 0) return 0;
		const sum = this.processingTimes.reduce((a, b) => a + b, 0);
		return sum / this.processingTimes.length;
	}
}
