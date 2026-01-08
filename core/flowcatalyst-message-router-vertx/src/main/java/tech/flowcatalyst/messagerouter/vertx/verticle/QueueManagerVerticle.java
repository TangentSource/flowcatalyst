package tech.flowcatalyst.messagerouter.vertx.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.ThreadingModel;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import tech.flowcatalyst.messagerouter.config.MessageRouterConfig;
import tech.flowcatalyst.messagerouter.config.ProcessingPool;
import tech.flowcatalyst.messagerouter.metrics.PoolMetricsService;
import tech.flowcatalyst.messagerouter.metrics.QueueMetricsService;
import tech.flowcatalyst.messagerouter.model.InFlightMessage;
import tech.flowcatalyst.messagerouter.model.MessagePointer;
import tech.flowcatalyst.messagerouter.vertx.channel.PoolChannels;
import tech.flowcatalyst.messagerouter.vertx.channel.RouterChannels;
import tech.flowcatalyst.messagerouter.vertx.message.RouterMessages.*;

import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Central coordinator verticle for the message router.
 * <p>
 * Owns:
 * - In-pipeline message tracking (deduplication)
 * - Message callbacks for ACK/NACK
 * - Pool deployment lifecycle
 * <p>
 * Threading: Virtual Thread (blocking OK)
 */
public class QueueManagerVerticle extends AbstractVerticle {

    private static final Logger LOG = Logger.getLogger(QueueManagerVerticle.class);

    // === OWNED STATE (plain HashMap - single threaded verticle) ===
    private final Map<String, String> poolDeploymentIds = new HashMap<>();
    private final Map<String, MessagePointer> inPipeline = new HashMap<>();
    private final Map<String, MessageCallbackInfo> messageCallbacks = new HashMap<>();
    private final Map<String, String> appMessageIdToPipelineKey = new HashMap<>();
    private final Map<String, Instant> messageSubmitTimes = new HashMap<>();

    // Injected dependencies
    private final QueueMetricsService queueMetrics;
    private final PoolMetricsService poolMetrics;
    private final Supplier<SqsClient> sqsClientSupplier;
    private final Supplier<MessageRouterConfig> configSupplier;

    private SqsClient sqsClient;
    private long configSyncTimerId;
    private long visibilityExtensionTimerId;

    public QueueManagerVerticle(
            QueueMetricsService queueMetrics,
            PoolMetricsService poolMetrics,
            Supplier<SqsClient> sqsClientSupplier,
            Supplier<MessageRouterConfig> configSupplier) {
        this.queueMetrics = queueMetrics;
        this.poolMetrics = poolMetrics;
        this.sqsClientSupplier = sqsClientSupplier;
        this.configSupplier = configSupplier;
    }

    @Override
    public void start() {
        LOG.info("Starting QueueManagerVerticle");

        // Initialize SQS client (blocking - fine on virtual thread)
        this.sqsClient = sqsClientSupplier.get();

        // Register typed consumers using channel addresses
        RouterChannels.batch(vertx).consumer(this::handleBatch);
        RouterChannels.ack(vertx).consumer(this::handleAck);
        RouterChannels.nack(vertx).consumer(this::handleNack);
        RouterChannels.queryInFlight(vertx).consumer(this::handleQueryInFlight);
        RouterChannels.queryPoolStats(vertx).consumer(this::handleQueryPoolStats);

        // Periodic config sync (5 minutes)
        configSyncTimerId = vertx.setPeriodic(300_000, id -> syncConfiguration());

        // Periodic visibility extension (55 seconds - before 60s default timeout)
        visibilityExtensionTimerId = vertx.setPeriodic(55_000, id -> extendMessageVisibility());

        // Initial config sync
        syncConfiguration();

        LOG.info("QueueManagerVerticle started");
    }

    @Override
    public void stop() {
        LOG.info("Stopping QueueManagerVerticle");

        // Cancel timers
        vertx.cancelTimer(configSyncTimerId);
        vertx.cancelTimer(visibilityExtensionTimerId);

        // Undeploy all pools
        for (String deploymentId : poolDeploymentIds.values()) {
            try {
                vertx.undeploy(deploymentId).toCompletionStage().toCompletableFuture().join();
            } catch (Exception e) {
                LOG.warnf("Failed to undeploy pool: %s", e.getMessage());
            }
        }

        LOG.info("QueueManagerVerticle stopped");
    }

    // === MESSAGE HANDLING ===

    private void handleBatch(Message<BatchRequest> msg) {
        BatchRequest batch = msg.body();
        String queueIdentifier = batch.queueIdentifier();
        String batchId = UUID.randomUUID().toString();

        LOG.debugf("Received batch of %d messages from queue [%s]", batch.messages().size(), queueIdentifier);

        for (QueuedMessage queuedMsg : batch.messages()) {
            String sqsMessageId = queuedMsg.sqsMessageId();

            // Deduplication check (safe - single threaded)
            if (inPipeline.containsKey(sqsMessageId)) {
                LOG.debugf("Duplicate message [%s] - already in pipeline, NACKing with 0 delay", sqsMessageId);
                nackMessageDirect(queuedMsg.queueUrl(), queuedMsg.receiptHandle(), 0);
                continue;
            }

            String poolCode = queuedMsg.poolCode();

            // Check if pool exists
            if (!poolDeploymentIds.containsKey(poolCode)) {
                LOG.warnf("Unknown pool [%s] for message [%s], NACKing with delay", poolCode, sqsMessageId);
                nackMessageDirect(queuedMsg.queueUrl(), queuedMsg.receiptHandle(), 30);
                continue;
            }

            // Convert to MessagePointer for internal tracking
            MessagePointer pointer = new MessagePointer(
                    queuedMsg.id(),
                    queuedMsg.poolCode(),
                    queuedMsg.authToken(),
                    queuedMsg.mediationType(),
                    queuedMsg.mediationTarget(),
                    queuedMsg.messageGroupId(),
                    batchId,
                    sqsMessageId
            );

            // Track in pipeline
            inPipeline.put(sqsMessageId, pointer);
            messageCallbacks.put(sqsMessageId, new MessageCallbackInfo(
                    queuedMsg.queueUrl(),
                    queuedMsg.receiptHandle(),
                    queueIdentifier
            ));
            messageSubmitTimes.put(sqsMessageId, Instant.now());

            if (pointer.id() != null) {
                appMessageIdToPipelineKey.put(pointer.id(), sqsMessageId);
            }

            // Route to pool using typed channel (fire-and-forget, pool handles ACK/NACK)
            PoolMessage poolMessage = new PoolMessage(
                    queuedMsg.id(),
                    sqsMessageId,
                    poolCode,
                    queuedMsg.authToken(),
                    queuedMsg.mediationType(),
                    queuedMsg.mediationTarget(),
                    queuedMsg.messageGroupId(),
                    batchId
            );
            PoolChannels.address(poolCode).messagesFireAndForget(vertx).send(poolMessage);

            queueMetrics.recordMessageReceived(queueIdentifier);
        }

        msg.reply(new OkReply());
    }

    private void handleAck(Message<AckRequest> msg) {
        String sqsMessageId = msg.body().sqsMessageId();
        LOG.debugf("ACK received for message [%s]", sqsMessageId);

        MessageCallbackInfo callback = messageCallbacks.remove(sqsMessageId);
        MessagePointer pointer = inPipeline.remove(sqsMessageId);
        messageSubmitTimes.remove(sqsMessageId);

        if (pointer != null && pointer.id() != null) {
            appMessageIdToPipelineKey.remove(pointer.id());
        }

        if (callback != null) {
            try {
                // Blocking SQS delete - fine on virtual thread
                sqsClient.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(callback.queueUrl())
                        .receiptHandle(callback.receiptHandle())
                        .build());

                queueMetrics.recordMessageProcessed(callback.queueIdentifier(), true);
            } catch (Exception e) {
                LOG.errorf("Failed to delete message from SQS: %s", e.getMessage());
            }
        }

        msg.reply(new OkReply());
    }

    private void handleNack(Message<NackRequest> msg) {
        NackRequest nack = msg.body();
        String sqsMessageId = nack.sqsMessageId();
        int delaySeconds = nack.delaySeconds();
        LOG.debugf("NACK received for message [%s] with delay %d", sqsMessageId, delaySeconds);

        MessageCallbackInfo callback = messageCallbacks.remove(sqsMessageId);
        MessagePointer pointer = inPipeline.remove(sqsMessageId);
        messageSubmitTimes.remove(sqsMessageId);

        if (pointer != null && pointer.id() != null) {
            appMessageIdToPipelineKey.remove(pointer.id());
        }

        if (callback != null) {
            try {
                // Blocking SQS visibility change - fine on virtual thread
                sqsClient.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                        .queueUrl(callback.queueUrl())
                        .receiptHandle(callback.receiptHandle())
                        .visibilityTimeout(delaySeconds)
                        .build());

                queueMetrics.recordMessageDeferred(callback.queueIdentifier());
            } catch (Exception e) {
                LOG.errorf("Failed to change message visibility: %s", e.getMessage());
            }
        }

        msg.reply(new OkReply());
    }

    private void nackMessageDirect(String queueUrl, String receiptHandle, int delaySeconds) {
        if (queueUrl != null && receiptHandle != null) {
            try {
                sqsClient.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(receiptHandle)
                        .visibilityTimeout(delaySeconds)
                        .build());
            } catch (Exception e) {
                LOG.warnf("Failed to NACK message directly: %s", e.getMessage());
            }
        }
    }

    // === QUERIES ===

    private void handleQueryInFlight(Message<InFlightQuery> msg) {
        InFlightQuery query = msg.body();
        int limit = query.limit();
        String filter = query.filter() != null ? query.filter() : "";

        List<InFlightMessage> result = inPipeline.entrySet().stream()
                .filter(e -> filter.isEmpty() || (e.getValue().id() != null && e.getValue().id().contains(filter)))
                .limit(limit)
                .map(e -> {
                    Instant submitTime = messageSubmitTimes.get(e.getKey());
                    long addedAtMs = submitTime != null ? submitTime.toEpochMilli() : System.currentTimeMillis();
                    MessageCallbackInfo callback = messageCallbacks.get(e.getKey());
                    String queueId = callback != null ? callback.queueIdentifier() : "unknown";
                    return InFlightMessage.from(
                            e.getValue().id(),
                            e.getKey(),  // brokerMessageId = sqsMessageId
                            queueId,
                            addedAtMs,
                            e.getValue().poolCode()
                    );
                })
                .toList();

        // Reply with typed result (not JsonArray)
        msg.reply(new InFlightQueryResult(result));
    }

    private void handleQueryPoolStats(Message<String> msg) {
        // Reply with typed result
        msg.reply(new PoolStatsResult(new HashSet<>(poolDeploymentIds.keySet())));
    }

    // === CONFIGURATION ===

    private void syncConfiguration() {
        LOG.debug("Syncing configuration");

        try {
            MessageRouterConfig config = configSupplier.get();
            if (config == null || config.processingPools() == null) {
                LOG.warn("No configuration available");
                return;
            }

            Set<String> configPoolCodes = config.processingPools().stream()
                    .map(ProcessingPool::code)
                    .collect(Collectors.toSet());

            // Deploy new pools
            for (ProcessingPool poolConfig : config.processingPools()) {
                if (!poolDeploymentIds.containsKey(poolConfig.code())) {
                    deployPool(poolConfig);
                } else {
                    // Send config update using typed channel
                    PoolConfigUpdate update = new PoolConfigUpdate(
                            poolConfig.concurrency(),
                            poolConfig.rateLimitPerMinute()
                    );
                    PoolChannels.address(poolConfig.code()).config(vertx).send(update);
                }
            }

            // Remove pools no longer in config
            for (String existingCode : new ArrayList<>(poolDeploymentIds.keySet())) {
                if (!configPoolCodes.contains(existingCode)) {
                    undeployPool(existingCode);
                }
            }

            LOG.debugf("Config sync complete. Active pools: %d", poolDeploymentIds.size());

        } catch (Exception e) {
            LOG.errorf("Failed to sync configuration: %s", e.getMessage());
        }
    }

    private void deployPool(ProcessingPool config) {
        LOG.infof("Deploying pool [%s] with concurrency=%d, rateLimit=%s",
                config.code(), config.concurrency(), config.rateLimitPerMinute());

        DeploymentOptions options = new DeploymentOptions()
                .setThreadingModel(ThreadingModel.VIRTUAL_THREAD)
                .setConfig(JsonObject.mapFrom(config));

        try {
            String deploymentId = vertx.deployVerticle(
                            new PoolVerticle(poolMetrics),
                            options)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();

            poolDeploymentIds.put(config.code(), deploymentId);

            // Also deploy mediator for this pool
            DeploymentOptions mediatorOptions = new DeploymentOptions()
                    .setThreadingModel(ThreadingModel.VIRTUAL_THREAD)
                    .setConfig(new JsonObject()
                            .put("poolCode", config.code()));

            vertx.deployVerticle(new MediatorVerticle(), mediatorOptions);

            LOG.infof("Pool [%s] deployed successfully", config.code());

        } catch (Exception e) {
            LOG.errorf("Failed to deploy pool [%s]: %s", config.code(), e.getMessage());
        }
    }

    private void undeployPool(String poolCode) {
        LOG.infof("Undeploying pool [%s]", poolCode);

        String deploymentId = poolDeploymentIds.remove(poolCode);
        if (deploymentId != null) {
            try {
                vertx.undeploy(deploymentId).toCompletionStage().toCompletableFuture().join();
                LOG.infof("Pool [%s] undeployed successfully", poolCode);
            } catch (Exception e) {
                LOG.errorf("Failed to undeploy pool [%s]: %s", poolCode, e.getMessage());
            }
        }
    }

    // === VISIBILITY EXTENSION ===

    private void extendMessageVisibility() {
        if (inPipeline.isEmpty()) {
            return;
        }

        LOG.debugf("Extending visibility for %d in-flight messages", inPipeline.size());

        for (Map.Entry<String, MessagePointer> entry : inPipeline.entrySet()) {
            MessageCallbackInfo callback = messageCallbacks.get(entry.getKey());
            if (callback != null) {
                try {
                    sqsClient.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                            .queueUrl(callback.queueUrl())
                            .receiptHandle(callback.receiptHandle())
                            .visibilityTimeout(120) // 2 minutes
                            .build());
                } catch (Exception e) {
                    LOG.warnf("Failed to extend visibility for [%s]: %s", entry.getKey(), e.getMessage());
                }
            }
        }
    }

    // === HELPER CLASSES ===

    private record MessageCallbackInfo(
            String queueUrl,
            String receiptHandle,
            String queueIdentifier
    ) {}

    // === ACCESSORS FOR MONITORING ===

    public int getInFlightCount() {
        return inPipeline.size();
    }

    public Set<String> getActivePoolCodes() {
        return new HashSet<>(poolDeploymentIds.keySet());
    }
}
