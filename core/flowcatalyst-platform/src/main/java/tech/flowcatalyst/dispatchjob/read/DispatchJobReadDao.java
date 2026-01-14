package tech.flowcatalyst.dispatchjob.read;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for DispatchJobRead entity (read-optimized projection).
 */
@RegisterRowMapper(DispatchJobReadRowMapper.class)
public interface DispatchJobReadDao {

    @SqlQuery("SELECT * FROM dispatch_jobs_read WHERE id = :id")
    Optional<DispatchJobRead> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM dispatch_jobs_read ORDER BY created_at DESC")
    List<DispatchJobRead> listAll();

    @SqlQuery("SELECT COUNT(*) FROM dispatch_jobs_read")
    long count();

    // Distinct queries for filter options
    @SqlQuery("SELECT DISTINCT client_id FROM dispatch_jobs_read WHERE client_id IS NOT NULL ORDER BY client_id")
    List<String> findDistinctClients();

    @SqlQuery("SELECT DISTINCT application FROM dispatch_jobs_read WHERE application IS NOT NULL ORDER BY application")
    List<String> findDistinctApplications();

    @SqlQuery("SELECT DISTINCT subdomain FROM dispatch_jobs_read WHERE subdomain IS NOT NULL ORDER BY subdomain")
    List<String> findDistinctSubdomains();

    @SqlQuery("SELECT DISTINCT aggregate FROM dispatch_jobs_read WHERE aggregate IS NOT NULL ORDER BY aggregate")
    List<String> findDistinctAggregates();

    @SqlQuery("SELECT DISTINCT code FROM dispatch_jobs_read ORDER BY code")
    List<String> findDistinctCodes();

    @SqlQuery("SELECT DISTINCT status FROM dispatch_jobs_read ORDER BY status")
    List<String> findDistinctStatuses();

    @SqlUpdate("""
        INSERT INTO dispatch_jobs_read (id, external_id, source, kind, code, subject, event_id, correlation_id,
                                        target_url, protocol, service_account_id, client_id, subscription_id,
                                        mode, dispatch_pool_id, message_group, status, max_retries, attempt_count,
                                        last_attempt_at, completed_at, duration_millis, last_error,
                                        created_at, updated_at, application, subdomain, aggregate)
        VALUES (:id, :externalId, :source, :kind, :code, :subject, :eventId, :correlationId,
                :targetUrl, :protocol, :serviceAccountId, :clientId, :subscriptionId,
                :mode, :dispatchPoolId, :messageGroup, :status, :maxRetries, :attemptCount,
                :lastAttemptAt, :completedAt, :durationMillis, :lastError,
                :createdAt, :updatedAt, :application, :subdomain, :aggregate)
        """)
    void insert(@BindBean DispatchJobRead job);

    @SqlUpdate("""
        UPDATE dispatch_jobs_read SET external_id = :externalId, source = :source, kind = :kind, code = :code,
               subject = :subject, event_id = :eventId, correlation_id = :correlationId,
               target_url = :targetUrl, protocol = :protocol, service_account_id = :serviceAccountId,
               client_id = :clientId, subscription_id = :subscriptionId, mode = :mode,
               dispatch_pool_id = :dispatchPoolId, message_group = :messageGroup, status = :status,
               max_retries = :maxRetries, attempt_count = :attemptCount, last_attempt_at = :lastAttemptAt,
               completed_at = :completedAt, duration_millis = :durationMillis, last_error = :lastError,
               updated_at = :updatedAt, application = :application, subdomain = :subdomain, aggregate = :aggregate
        WHERE id = :id
        """)
    void update(@BindBean DispatchJobRead job);

    @SqlUpdate("DELETE FROM dispatch_jobs_read WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
