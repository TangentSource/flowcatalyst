package tech.flowcatalyst.dispatchjob.repository;

import com.fasterxml.jackson.core.type.TypeReference;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.dispatch.DispatchMode;
import tech.flowcatalyst.dispatchjob.entity.DispatchAttempt;
import tech.flowcatalyst.dispatchjob.entity.DispatchJob;
import tech.flowcatalyst.dispatchjob.entity.DispatchJobMetadata;
import tech.flowcatalyst.dispatchjob.model.DispatchKind;
import tech.flowcatalyst.dispatchjob.model.DispatchProtocol;
import tech.flowcatalyst.dispatchjob.model.DispatchStatus;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * JDBI row mapper for DispatchJob entity.
 */
public class DispatchJobRowMapper implements RowMapper<DispatchJob> {

    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    @Override
    public DispatchJob map(ResultSet rs, StatementContext ctx) throws SQLException {
        DispatchJob job = new DispatchJob();

        job.id = rs.getString("id");
        job.externalId = rs.getString("external_id");
        job.source = rs.getString("source");

        String kindStr = rs.getString("kind");
        job.kind = kindStr != null ? DispatchKind.valueOf(kindStr) : DispatchKind.EVENT;

        job.code = rs.getString("code");
        job.subject = rs.getString("subject");
        job.eventId = rs.getString("event_id");
        job.correlationId = rs.getString("correlation_id");

        // Parse JSONB metadata array
        String metadataJson = rs.getString("metadata");
        job.metadata = metadataJson != null
            ? JsonHelper.fromJsonList(metadataJson, DispatchJobMetadata.class)
            : new ArrayList<>();

        job.targetUrl = rs.getString("target_url");

        String protocolStr = rs.getString("protocol");
        job.protocol = protocolStr != null ? DispatchProtocol.valueOf(protocolStr) : DispatchProtocol.HTTP_WEBHOOK;

        // Parse JSONB headers map
        String headersJson = rs.getString("headers");
        job.headers = headersJson != null
            ? JsonHelper.fromJson(headersJson, MAP_TYPE)
            : new HashMap<>();
        if (job.headers == null) {
            job.headers = new HashMap<>();
        }

        job.payload = rs.getString("payload");
        job.payloadContentType = rs.getString("payload_content_type");
        job.dataOnly = rs.getBoolean("data_only");
        job.serviceAccountId = rs.getString("service_account_id");
        job.clientId = rs.getString("client_id");
        job.subscriptionId = rs.getString("subscription_id");

        String modeStr = rs.getString("mode");
        job.mode = modeStr != null ? DispatchMode.valueOf(modeStr) : DispatchMode.IMMEDIATE;

        job.dispatchPoolId = rs.getString("dispatch_pool_id");
        job.messageGroup = rs.getString("message_group");
        job.sequence = rs.getInt("sequence");
        job.timeoutSeconds = rs.getInt("timeout_seconds");
        job.schemaId = rs.getString("schema_id");

        String statusStr = rs.getString("status");
        job.status = statusStr != null ? DispatchStatus.valueOf(statusStr) : DispatchStatus.PENDING;

        job.maxRetries = rs.getInt("max_retries");
        job.retryStrategy = rs.getString("retry_strategy");

        Timestamp scheduledFor = rs.getTimestamp("scheduled_for");
        job.scheduledFor = scheduledFor != null ? scheduledFor.toInstant() : null;

        Timestamp expiresAt = rs.getTimestamp("expires_at");
        job.expiresAt = expiresAt != null ? expiresAt.toInstant() : null;

        job.attemptCount = rs.getInt("attempt_count");

        Timestamp lastAttemptAt = rs.getTimestamp("last_attempt_at");
        job.lastAttemptAt = lastAttemptAt != null ? lastAttemptAt.toInstant() : null;

        Timestamp completedAt = rs.getTimestamp("completed_at");
        job.completedAt = completedAt != null ? completedAt.toInstant() : null;

        job.durationMillis = rs.getObject("duration_millis") != null ? rs.getLong("duration_millis") : null;
        job.lastError = rs.getString("last_error");
        job.idempotencyKey = rs.getString("idempotency_key");

        // Parse JSONB attempts array
        String attemptsJson = rs.getString("attempts");
        job.attempts = attemptsJson != null
            ? JsonHelper.fromJsonList(attemptsJson, DispatchAttempt.class)
            : new ArrayList<>();

        Timestamp createdAt = rs.getTimestamp("created_at");
        job.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp updatedAt = rs.getTimestamp("updated_at");
        job.updatedAt = updatedAt != null ? updatedAt.toInstant() : null;

        return job;
    }
}
