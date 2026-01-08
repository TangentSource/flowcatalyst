package tech.flowcatalyst.messagerouter.vertx.verticle;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.jboss.logging.Logger;
import tech.flowcatalyst.messagerouter.metrics.PoolMetricsService;
import tech.flowcatalyst.messagerouter.vertx.channel.MediatorChannels;
import tech.flowcatalyst.messagerouter.vertx.channel.PoolChannels;
import tech.flowcatalyst.messagerouter.vertx.channel.RouterChannels;
import tech.flowcatalyst.messagerouter.vertx.message.RouterMessages.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Pool verticle that manages message groups and enforces concurrency/rate limits.
 * <p>
 * Owns:
 * - Group queues (one BlockingQueue per message group)
 * - Active group tracking
 * - Failed batch tracking for FIFO ordering
 * - Semaphore for concurrency control
 * - Rate limiter
 * <p>
 * Threading: Virtual Thread (blocking OK)
 */
public class PoolVerticle extends AbstractVerticle {

    private static final Logger LOG = Logger.getLogger(PoolVerticle.class);

    // === OWNED STATE (plain collections - single threaded verticle) ===
    private String poolCode;
    private final Map<String, BlockingQueue<PoolMessage>> groupQueues = new HashMap<>();
    private final Map<String, Boolean> activeGroups = new HashMap<>();
    private final Set<String> failedBatchGroups = new HashSet<>();
    private final Set<Thread> workerThreads = Collections.synchronizedSet(new HashSet<>());

    private Semaphore semaphore;
    private RateLimiter rateLimiter;
    private volatile boolean running = true;

    private final PoolMetricsService poolMetrics;

    public PoolVerticle(PoolMetricsService poolMetrics) {
        this.poolMetrics = poolMetrics;
    }

    @Override
    public void start() {
        JsonObject config = config();
        this.poolCode = config.getString("code");

        int concurrency = config.getInteger("concurrency", 10);
        this.semaphore = new Semaphore(concurrency);

        Integer rateLimitPerMinute = config.getInteger("rateLimitPerMinute");
        if (rateLimitPerMinute != null && rateLimitPerMinute > 0) {
            this.rateLimiter = createRateLimiter(rateLimitPerMinute);
        }

        LOG.infof("PoolVerticle [%s] starting with concurrency=%d, rateLimit=%s",
                poolCode, concurrency, rateLimitPerMinute);

        // Listen for messages and config updates using typed channels
        PoolChannels.Address poolAddress = PoolChannels.address(poolCode);
        poolAddress.messages(vertx).consumer(this::handleMessage);
        poolAddress.config(vertx).consumer(this::handleConfigUpdate);

        // Periodic cleanup of failed batch groups (every 5 minutes)
        vertx.setPeriodic(300_000, id -> failedBatchGroups.clear());

        // Periodic metrics update
        vertx.setPeriodic(1_000, id -> updateMetrics());
    }

    @Override
    public void stop() {
        LOG.infof("PoolVerticle [%s] stopping", poolCode);
        running = false;

        // Interrupt all worker threads to unblock queue.poll()
        for (Thread worker : workerThreads) {
            worker.interrupt();
        }
        workerThreads.clear();
    }

    // === MESSAGE HANDLING ===

    private void handleMessage(Message<PoolMessage> msg) {
        PoolMessage message = msg.body();
        String groupId = message.messageGroupId() != null ? message.messageGroupId() : "__DEFAULT__";
        String batchId = message.batchId();

        // Check batch+group FIFO failure
        String batchGroupKey = batchId + "|" + groupId;
        if (failedBatchGroups.contains(batchGroupKey)) {
            LOG.debugf("Batch+group [%s] previously failed, NACKing message [%s]", batchGroupKey, message.sqsMessageId());
            sendNack(message.sqsMessageId(), 10);
            return;
        }

        // Get or create group queue (plain HashMap - single threaded)
        BlockingQueue<PoolMessage> queue = groupQueues.get(groupId);
        if (queue == null) {
            queue = new LinkedBlockingQueue<>();
            groupQueues.put(groupId, queue);
        }

        // Ensure group worker is running
        if (!activeGroups.containsKey(groupId)) {
            activeGroups.put(groupId, true);
            startGroupWorker(groupId, queue);
        }

        // Add to queue
        queue.offer(message);
        poolMetrics.recordMessageSubmitted(poolCode);

        LOG.debugf("Message [%s] queued in group [%s], queue size: %d", message.sqsMessageId(), groupId, queue.size());
    }

    private void startGroupWorker(String groupId, BlockingQueue<PoolMessage> queue) {
        // Run on virtual thread - blocking is fine
        vertx.executeBlocking(() -> {
            processGroup(groupId, queue);
            return null;
        }, false);
    }

    private void processGroup(String groupId, BlockingQueue<PoolMessage> queue) {
        LOG.debugf("Group worker started for [%s] in pool [%s]", groupId, poolCode);
        workerThreads.add(Thread.currentThread());

        try {
            while (running) {
                // Block waiting for message (5 min idle timeout)
                PoolMessage message = queue.poll(5, TimeUnit.MINUTES);

                if (message == null) {
                    // Idle timeout - cleanup if queue is still empty
                    if (queue.isEmpty()) {
                        LOG.debugf("Group [%s] idle timeout, cleaning up", groupId);
                        groupQueues.remove(groupId);
                        activeGroups.remove(groupId);
                        return;
                    }
                    continue;
                }

                // Wait for rate limit permit (blocking)
                waitForRateLimitPermit();

                // Acquire concurrency permit (blocking)
                semaphore.acquire();
                poolMetrics.recordProcessingStarted(poolCode);

                long startTime = System.currentTimeMillis();
                try {
                    // Process via mediator (blocking request-reply)
                    processMessage(message);
                } finally {
                    semaphore.release();
                    poolMetrics.recordProcessingFinished(poolCode);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.debugf("Group worker [%s] interrupted", groupId);
        } finally {
            workerThreads.remove(Thread.currentThread());
            activeGroups.remove(groupId);
            LOG.debugf("Group worker [%s] exited", groupId);
        }
    }

    private void waitForRateLimitPermit() {
        if (rateLimiter == null) {
            return;
        }

        while (running) {
            // Capture current limiter (may be replaced by config update)
            RateLimiter limiter = this.rateLimiter;
            if (limiter == null) {
                return; // Rate limiting was removed
            }

            if (limiter.acquirePermission()) {
                return; // Got permit
            }

            // No permit - wait briefly then re-check
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void processMessage(PoolMessage message) {
        long startTime = System.currentTimeMillis();

        try {
            // Build typed mediation request
            MediationRequest request = new MediationRequest(
                    message.id(),
                    message.sqsMessageId(),
                    message.authToken(),
                    message.mediationType(),
                    message.mediationTarget(),
                    message.messageGroupId()
            );

            // Blocking request-reply to mediator via typed channel
            MediationResult result = MediatorChannels.address(poolCode)
                    .mediate(vertx)
                    .requestBlocking(request);

            long durationMs = System.currentTimeMillis() - startTime;
            handleMediationResult(message, result, durationMs);

        } catch (Exception e) {
            long durationMs = System.currentTimeMillis() - startTime;
            LOG.warnf("Mediation failed for message [%s]: %s", message.sqsMessageId(), e.getMessage());

            poolMetrics.recordProcessingFailure(poolCode, durationMs, "MEDIATION_ERROR");
            sendNack(message.sqsMessageId(), 30);
            markBatchGroupFailed(message);
        }
    }

    private void handleMediationResult(PoolMessage message, MediationResult result, long durationMs) {
        switch (result.outcome()) {
            case SUCCESS -> {
                poolMetrics.recordProcessingSuccess(poolCode, durationMs);
                sendAck(message.sqsMessageId());
            }
            case NACK -> {
                poolMetrics.recordProcessingFailure(poolCode, durationMs, "MEDIATION_ERROR");
                sendNack(message.sqsMessageId(), result.delaySeconds());
                markBatchGroupFailed(message);
            }
            case ERROR_CONFIG -> {
                // Configuration error - ACK to remove from queue (won't succeed on retry)
                poolMetrics.recordProcessingSuccess(poolCode, durationMs);
                sendAck(message.sqsMessageId());
            }
        }
    }

    private void sendAck(String sqsMessageId) {
        try {
            RouterChannels.ack(vertx).requestBlocking(new AckRequest(sqsMessageId));
        } catch (Exception e) {
            LOG.errorf("Failed to send ACK for [%s]: %s", sqsMessageId, e.getMessage());
        }
    }

    private void sendNack(String sqsMessageId, int delaySeconds) {
        try {
            RouterChannels.nack(vertx).requestBlocking(new NackRequest(sqsMessageId, delaySeconds));
        } catch (Exception e) {
            LOG.errorf("Failed to send NACK for [%s]: %s", sqsMessageId, e.getMessage());
        }
    }

    private void markBatchGroupFailed(PoolMessage message) {
        String batchId = message.batchId();
        String groupId = message.messageGroupId() != null ? message.messageGroupId() : "__DEFAULT__";
        String batchGroupKey = batchId + "|" + groupId;
        failedBatchGroups.add(batchGroupKey);
        LOG.debugf("Marked batch+group [%s] as failed for FIFO ordering", batchGroupKey);
    }

    // === CONFIG UPDATE ===

    private void handleConfigUpdate(Message<PoolConfigUpdate> msg) {
        PoolConfigUpdate config = msg.body();
        LOG.infof("Received config update for pool [%s]", poolCode);

        // Update concurrency
        int newConcurrency = config.concurrency();
        this.semaphore = new Semaphore(newConcurrency);

        // Update rate limiter
        Integer newRateLimit = config.rateLimitPerMinute();
        if (newRateLimit != null && newRateLimit > 0) {
            this.rateLimiter = createRateLimiter(newRateLimit);
        } else {
            this.rateLimiter = null;
        }

        LOG.infof("Pool [%s] config updated: concurrency=%d, rateLimit=%s", poolCode, newConcurrency, newRateLimit);
    }

    // === METRICS ===

    private void updateMetrics() {
        int activeWorkers = activeGroups.size();
        int availablePermits = semaphore.availablePermits();
        int totalQueueSize = groupQueues.values().stream().mapToInt(BlockingQueue::size).sum();

        poolMetrics.updatePoolGauges(poolCode, activeWorkers, availablePermits, totalQueueSize, groupQueues.size());
    }

    // === HELPERS ===

    private RateLimiter createRateLimiter(int limitPerMinute) {
        RateLimiterConfig config = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMinutes(1))
                .limitForPeriod(limitPerMinute)
                .timeoutDuration(Duration.ZERO) // Don't wait, we poll manually
                .build();

        return RateLimiter.of("pool-" + poolCode, config);
    }

    // === ACCESSORS FOR MONITORING ===

    public String getPoolCode() {
        return poolCode;
    }

    public int getActiveWorkers() {
        return activeGroups.size();
    }

    public int getQueueSize() {
        return groupQueues.values().stream().mapToInt(BlockingQueue::size).sum();
    }

    public int getAvailablePermits() {
        return semaphore.availablePermits();
    }
}
