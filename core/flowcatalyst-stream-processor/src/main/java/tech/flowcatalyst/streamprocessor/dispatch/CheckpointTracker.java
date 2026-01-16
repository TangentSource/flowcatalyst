package tech.flowcatalyst.streamprocessor.dispatch;

import org.bson.BsonDocument;
import tech.flowcatalyst.streamprocessor.checkpoint.CheckpointStore;

import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * Tracks in-flight batch processing and manages checkpoint advancement for a single stream.
 *
 * <p>Batches are processed concurrently, but checkpoints can only be advanced
 * when all prior batches have completed. This ensures we don't skip events
 * on restart.</p>
 *
 * <p>Example:</p>
 * <ul>
 *   <li>Batches 1, 2, 3 dispatched</li>
 *   <li>Batch 3 completes → checkpoint stays at 0 (waiting for 1, 2)</li>
 *   <li>Batch 1 completes → checkpoint advances to 1</li>
 *   <li>Batch 2 completes → checkpoint advances to 2, then 3</li>
 * </ul>
 *
 * <p>Note: This class is NOT a CDI bean. Each stream gets its own instance
 * created by the StreamProcessorStarter.</p>
 */
public class CheckpointTracker {

    private static final Logger LOG = Logger.getLogger(CheckpointTracker.class.getName());

    private final CheckpointStore checkpointStore;
    private final String streamName;
    private final String checkpointKey;

    private final TreeMap<Long, BatchResult> batches = new TreeMap<>();
    private long lastCheckpointedSeq = 0;
    // Use ReentrantLock instead of synchronized to avoid pinning virtual threads
    private final ReentrantLock lock = new ReentrantLock();

    // Track if we've had a fatal error
    private volatile Exception fatalError = null;

    /**
     * Create a new checkpoint tracker for a stream.
     *
     * @param checkpointStore the store for persisting checkpoints
     * @param streamName      name of the stream (for logging)
     * @param checkpointKey   Redis/storage key for this stream's checkpoint
     */
    public CheckpointTracker(CheckpointStore checkpointStore, String streamName, String checkpointKey) {
        this.checkpointStore = checkpointStore;
        this.streamName = streamName;
        this.checkpointKey = checkpointKey;
    }

    /**
     * Mark a batch as successfully completed.
     *
     * @param seq         the batch sequence number
     * @param resumeToken the change stream resume token for this batch
     */
    public void markComplete(long seq, BsonDocument resumeToken) {
        lock.lock();
        try {
            batches.put(seq, new BatchResult(resumeToken, true, null));
            advanceCheckpoint();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Mark a batch as failed.
     *
     * @param seq   the batch sequence number
     * @param error the error that caused the failure
     */
    public void markFailed(long seq, Exception error) {
        lock.lock();
        try {
            batches.put(seq, new BatchResult(null, false, error));
            this.fatalError = error;
            // Don't advance checkpoint - we're failing
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check if a fatal error has occurred.
     */
    public boolean hasFatalError() {
        return fatalError != null;
    }

    /**
     * Get the fatal error if one occurred.
     */
    public Exception getFatalError() {
        return fatalError;
    }

    /**
     * Advance the checkpoint to the highest contiguous completed batch.
     */
    private void advanceCheckpoint() {
        while (batches.containsKey(lastCheckpointedSeq + 1)) {
            BatchResult result = batches.get(lastCheckpointedSeq + 1);
            if (!result.success()) {
                break; // Stop at failed batch
            }

            lastCheckpointedSeq++;
            BsonDocument token = batches.remove(lastCheckpointedSeq).resumeToken();

            // Persist checkpoint
            checkpointStore.saveCheckpoint(checkpointKey, token);
            LOG.fine("[" + streamName + "] Checkpoint advanced to batch " + lastCheckpointedSeq);
        }
    }

    /**
     * Get the number of batches currently in flight.
     */
    public int getInFlightCount() {
        lock.lock();
        try {
            return batches.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the last checkpointed sequence number.
     */
    public long getLastCheckpointedSeq() {
        return lastCheckpointedSeq;
    }

    /**
     * Get the stream name.
     */
    public String getStreamName() {
        return streamName;
    }

    /**
     * Reset state (for testing).
     */
    public void reset() {
        lock.lock();
        try {
            batches.clear();
            lastCheckpointedSeq = 0;
            fatalError = null;
        } finally {
            lock.unlock();
        }
    }

    private record BatchResult(BsonDocument resumeToken, boolean success, Exception error) {}
}
