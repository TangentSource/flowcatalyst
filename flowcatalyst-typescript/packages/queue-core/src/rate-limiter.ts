/**
 * Simple token bucket rate limiter
 * Matches Java's Resilience4j rate limiter behavior
 */
export interface RateLimiter {
	/**
	 * Acquire a permit, waiting if necessary
	 * Returns true if we had to wait
	 */
	acquire(): Promise<boolean>;

	/**
	 * Try to acquire a permit without waiting
	 */
	tryAcquire(): boolean;

	/**
	 * Get current available permits
	 */
	getAvailablePermits(): number;
}

/**
 * Create a rate limiter with the specified rate per minute
 */
export function createRateLimiter(permitsPerMinute: number): RateLimiter {
	const intervalMs = 60_000 / permitsPerMinute;
	let lastAcquireTime = 0;
	let availablePermits = permitsPerMinute;

	// Refill permits periodically
	const refillInterval = setInterval(() => {
		availablePermits = Math.min(availablePermits + 1, permitsPerMinute);
	}, intervalMs);

	// Ensure interval doesn't prevent process exit
	refillInterval.unref();

	return {
		async acquire(): Promise<boolean> {
			const now = Date.now();
			const timeSinceLastAcquire = now - lastAcquireTime;
			const minInterval = intervalMs;

			if (timeSinceLastAcquire < minInterval && availablePermits <= 0) {
				// Need to wait
				const waitTime = minInterval - timeSinceLastAcquire;
				await sleep(waitTime);
				lastAcquireTime = Date.now();
				availablePermits = Math.max(0, availablePermits - 1);
				return true;
			}

			lastAcquireTime = now;
			availablePermits = Math.max(0, availablePermits - 1);
			return false;
		},

		tryAcquire(): boolean {
			if (availablePermits > 0) {
				availablePermits--;
				lastAcquireTime = Date.now();
				return true;
			}
			return false;
		},

		getAvailablePermits(): number {
			return availablePermits;
		},
	};
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
