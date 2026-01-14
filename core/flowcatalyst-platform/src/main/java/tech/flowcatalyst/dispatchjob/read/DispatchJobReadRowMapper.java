package tech.flowcatalyst.dispatchjob.read;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for DispatchJobRead entity.
 */
public class DispatchJobReadRowMapper implements RowMapper<DispatchJobRead> {

    @Override
    public DispatchJobRead map(ResultSet rs, StatementContext ctx) throws SQLException {
        DispatchJobRead job = new DispatchJobRead();

        job.id = rs.getString("id");
        job.dispatchJobId = rs.getString("id");  // Same as id
        job.externalId = rs.getString("external_id");
        job.source = rs.getString("source");
        job.kind = rs.getString("kind");
        job.code = rs.getString("code");
        job.subject = rs.getString("subject");

        // Parsed code segments
        job.application = rs.getString("application");
        job.subdomain = rs.getString("subdomain");
        job.aggregate = rs.getString("aggregate");

        job.eventId = rs.getString("event_id");
        job.correlationId = rs.getString("correlation_id");
        job.targetUrl = rs.getString("target_url");
        job.protocol = rs.getString("protocol");
        job.clientId = rs.getString("client_id");
        job.subscriptionId = rs.getString("subscription_id");
        job.serviceAccountId = rs.getString("service_account_id");
        job.dispatchPoolId = rs.getString("dispatch_pool_id");
        job.messageGroup = rs.getString("message_group");
        job.mode = rs.getString("mode");
        job.status = rs.getString("status");
        job.attemptCount = rs.getInt("attempt_count");
        job.maxRetries = rs.getInt("max_retries");
        job.lastError = rs.getString("last_error");

        Timestamp lastAttemptAt = rs.getTimestamp("last_attempt_at");
        job.lastAttemptAt = lastAttemptAt != null ? lastAttemptAt.toInstant() : null;

        Timestamp completedAt = rs.getTimestamp("completed_at");
        job.completedAt = completedAt != null ? completedAt.toInstant() : null;

        job.durationMillis = rs.getObject("duration_millis") != null ? rs.getLong("duration_millis") : null;

        Timestamp createdAt = rs.getTimestamp("created_at");
        job.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp updatedAt = rs.getTimestamp("updated_at");
        job.updatedAt = updatedAt != null ? updatedAt.toInstant() : null;

        // Computed fields
        String status = job.status;
        job.isCompleted = "COMPLETED".equals(status);
        job.isTerminal = "COMPLETED".equals(status) || "ERROR".equals(status) || "CANCELLED".equals(status);

        return job;
    }
}
