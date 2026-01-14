package tech.flowcatalyst.dispatchjob.repository;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import tech.flowcatalyst.dispatchjob.entity.DispatchJob;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for DispatchJob entity.
 */
@RegisterRowMapper(DispatchJobRowMapper.class)
public interface DispatchJobDao {

    @SqlQuery("SELECT * FROM dispatch_jobs WHERE id = :id")
    Optional<DispatchJob> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM dispatch_jobs ORDER BY created_at DESC")
    List<DispatchJob> listAll();

    @SqlQuery("SELECT * FROM dispatch_jobs ORDER BY created_at DESC LIMIT :size OFFSET :offset")
    List<DispatchJob> findRecentPaged(@Bind("offset") int offset, @Bind("size") int size);

    @SqlQuery("SELECT COUNT(*) FROM dispatch_jobs")
    long count();

    @SqlQuery("""
        SELECT * FROM dispatch_jobs
        WHERE status = 'PENDING' AND (scheduled_for IS NULL OR scheduled_for <= NOW())
        ORDER BY created_at
        LIMIT :limit
        """)
    List<DispatchJob> findPendingJobs(@Bind("limit") int limit);

    @SqlQuery("""
        SELECT COUNT(*) FROM dispatch_jobs
        WHERE message_group = :messageGroup AND status = :status
        """)
    long countByMessageGroupAndStatus(@Bind("messageGroup") String messageGroup, @Bind("status") String status);

    @SqlQuery("""
        SELECT * FROM dispatch_jobs
        WHERE status = 'QUEUED' AND updated_at < :threshold
        """)
    List<DispatchJob> findStaleQueued(@Bind("threshold") Instant threshold);

    @SqlQuery("""
        SELECT * FROM dispatch_jobs
        WHERE status = 'QUEUED' AND updated_at < :threshold
        LIMIT :limit
        """)
    List<DispatchJob> findStaleQueuedLimited(@Bind("threshold") Instant threshold, @Bind("limit") int limit);

    @SqlUpdate("""
        INSERT INTO dispatch_jobs (id, external_id, source, kind, code, subject, event_id, correlation_id,
                                  metadata, target_url, protocol, headers, payload, payload_content_type,
                                  data_only, service_account_id, client_id, subscription_id, mode,
                                  dispatch_pool_id, message_group, sequence, timeout_seconds, schema_id,
                                  status, max_retries, retry_strategy, scheduled_for, expires_at,
                                  attempt_count, last_attempt_at, completed_at, duration_millis, last_error,
                                  idempotency_key, attempts, created_at, updated_at)
        VALUES (:id, :externalId, :source, :kind, :code, :subject, :eventId, :correlationId,
                :metadata::jsonb, :targetUrl, :protocol, :headers::jsonb, :payload, :payloadContentType,
                :dataOnly, :serviceAccountId, :clientId, :subscriptionId, :mode,
                :dispatchPoolId, :messageGroup, :sequence, :timeoutSeconds, :schemaId,
                :status, :maxRetries, :retryStrategy, :scheduledFor, :expiresAt,
                :attemptCount, :lastAttemptAt, :completedAt, :durationMillis, :lastError,
                :idempotencyKey, :attempts::jsonb, :createdAt, :updatedAt)
        """)
    void insert(@BindBean DispatchJob job,
                @Bind("metadata") String metadataJson,
                @Bind("headers") String headersJson,
                @Bind("attempts") String attemptsJson);

    @SqlUpdate("""
        UPDATE dispatch_jobs SET external_id = :externalId, source = :source, kind = :kind, code = :code,
               subject = :subject, event_id = :eventId, correlation_id = :correlationId,
               metadata = :metadata::jsonb, target_url = :targetUrl, protocol = :protocol,
               headers = :headers::jsonb, payload = :payload, payload_content_type = :payloadContentType,
               data_only = :dataOnly, service_account_id = :serviceAccountId, client_id = :clientId,
               subscription_id = :subscriptionId, mode = :mode, dispatch_pool_id = :dispatchPoolId,
               message_group = :messageGroup, sequence = :sequence, timeout_seconds = :timeoutSeconds,
               schema_id = :schemaId, status = :status, max_retries = :maxRetries,
               retry_strategy = :retryStrategy, scheduled_for = :scheduledFor, expires_at = :expiresAt,
               attempt_count = :attemptCount, last_attempt_at = :lastAttemptAt, completed_at = :completedAt,
               duration_millis = :durationMillis, last_error = :lastError, idempotency_key = :idempotencyKey,
               attempts = :attempts::jsonb, updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindBean DispatchJob job,
                @Bind("metadata") String metadataJson,
                @Bind("headers") String headersJson,
                @Bind("attempts") String attemptsJson);

    @SqlUpdate("""
        UPDATE dispatch_jobs SET status = :status, completed_at = :completedAt,
               duration_millis = :durationMillis, last_error = :lastError, updated_at = NOW()
        WHERE id = :jobId
        """)
    void updateStatus(@Bind("jobId") String jobId,
                      @Bind("status") String status,
                      @Bind("completedAt") Instant completedAt,
                      @Bind("durationMillis") Long durationMillis,
                      @Bind("lastError") String lastError);

    @SqlUpdate("DELETE FROM dispatch_jobs WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
