package tech.flowcatalyst.dispatchjob.repository;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import tech.flowcatalyst.dispatch.DispatchMode;
import tech.flowcatalyst.dispatchjob.dto.CreateDispatchJobRequest;
import tech.flowcatalyst.dispatchjob.dto.DispatchJobFilter;
import tech.flowcatalyst.dispatchjob.entity.DispatchAttempt;
import tech.flowcatalyst.dispatchjob.entity.DispatchJob;
import tech.flowcatalyst.dispatchjob.entity.DispatchJobMetadata;
import tech.flowcatalyst.dispatchjob.model.DispatchKind;
import tech.flowcatalyst.dispatchjob.model.DispatchProtocol;
import tech.flowcatalyst.dispatchjob.model.DispatchStatus;
import tech.flowcatalyst.platform.shared.TsidGenerator;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.time.Instant;
import java.util.*;

/**
 * PostgreSQL implementation of DispatchJobRepository using JDBI.
 */
@ApplicationScoped
@Typed(DispatchJobRepository.class)
class PostgresDispatchJobRepository implements DispatchJobRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public DispatchJob findById(String id) {
        return jdbi.withExtension(DispatchJobDao.class, dao -> dao.findById(id).orElse(null));
    }

    @Override
    public Optional<DispatchJob> findByIdOptional(String id) {
        return jdbi.withExtension(DispatchJobDao.class, dao -> dao.findById(id));
    }

    @Override
    public List<DispatchJob> findWithFilter(DispatchJobFilter filter) {
        StringBuilder sql = new StringBuilder("SELECT * FROM dispatch_jobs WHERE 1=1");

        if (filter.status() != null) {
            sql.append(" AND status = :status");
        }
        if (filter.source() != null) {
            sql.append(" AND source = :source");
        }
        if (filter.kind() != null) {
            sql.append(" AND kind = :kind");
        }
        if (filter.code() != null) {
            sql.append(" AND code = :code");
        }
        if (filter.clientId() != null) {
            sql.append(" AND client_id = :clientId");
        }
        if (filter.subscriptionId() != null) {
            sql.append(" AND subscription_id = :subscriptionId");
        }
        if (filter.dispatchPoolId() != null) {
            sql.append(" AND dispatch_pool_id = :dispatchPoolId");
        }
        if (filter.messageGroup() != null) {
            sql.append(" AND message_group = :messageGroup");
        }
        if (filter.createdAfter() != null) {
            sql.append(" AND created_at >= :createdAfter");
        }
        if (filter.createdBefore() != null) {
            sql.append(" AND created_at <= :createdBefore");
        }

        sql.append(" ORDER BY created_at DESC");
        sql.append(" LIMIT :size OFFSET :offset");

        int offset = filter.page() * filter.size();
        String finalSql = sql.toString();

        return jdbi.withHandle(handle -> {
            var query = handle.createQuery(finalSql)
                .registerRowMapper(new DispatchJobRowMapper());

            if (filter.status() != null) {
                query.bind("status", filter.status().name());
            }
            if (filter.source() != null) {
                query.bind("source", filter.source());
            }
            if (filter.kind() != null) {
                query.bind("kind", filter.kind().name());
            }
            if (filter.code() != null) {
                query.bind("code", filter.code());
            }
            if (filter.clientId() != null) {
                query.bind("clientId", filter.clientId());
            }
            if (filter.subscriptionId() != null) {
                query.bind("subscriptionId", filter.subscriptionId());
            }
            if (filter.dispatchPoolId() != null) {
                query.bind("dispatchPoolId", filter.dispatchPoolId());
            }
            if (filter.messageGroup() != null) {
                query.bind("messageGroup", filter.messageGroup());
            }
            if (filter.createdAfter() != null) {
                query.bind("createdAfter", filter.createdAfter());
            }
            if (filter.createdBefore() != null) {
                query.bind("createdBefore", filter.createdBefore());
            }

            query.bind("size", filter.size());
            query.bind("offset", offset);

            return query.mapTo(DispatchJob.class).list();
        });
    }

    @Override
    public List<DispatchJob> findByMetadata(String key, String value) {
        return jdbi.withHandle(handle ->
            handle.createQuery("""
                SELECT * FROM dispatch_jobs
                WHERE EXISTS (
                    SELECT 1 FROM jsonb_array_elements(metadata) elem
                    WHERE elem->>'key' = :key AND elem->>'value' = :value
                )
                ORDER BY created_at DESC
                """)
                .bind("key", key)
                .bind("value", value)
                .registerRowMapper(new DispatchJobRowMapper())
                .mapTo(DispatchJob.class)
                .list()
        );
    }

    @Override
    public List<DispatchJob> findByMetadataFilters(Map<String, String> metadataFilters) {
        if (metadataFilters == null || metadataFilters.isEmpty()) {
            return listAll();
        }

        StringBuilder sql = new StringBuilder("SELECT * FROM dispatch_jobs WHERE 1=1");
        int idx = 0;
        for (var entry : metadataFilters.entrySet()) {
            sql.append(String.format("""
                 AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(metadata) elem
                    WHERE elem->>'key' = :key%d AND elem->>'value' = :value%d
                )
                """, idx, idx));
            idx++;
        }
        sql.append(" ORDER BY created_at DESC");

        String finalSql = sql.toString();
        return jdbi.withHandle(handle -> {
            var query = handle.createQuery(finalSql)
                .registerRowMapper(new DispatchJobRowMapper());

            int i = 0;
            for (var entry : metadataFilters.entrySet()) {
                query.bind("key" + i, entry.getKey());
                query.bind("value" + i, entry.getValue());
                i++;
            }

            return query.mapTo(DispatchJob.class).list();
        });
    }

    @Override
    public List<DispatchJob> findRecentPaged(int page, int size) {
        int offset = page * size;
        return jdbi.withExtension(DispatchJobDao.class, dao -> dao.findRecentPaged(offset, size));
    }

    @Override
    public List<DispatchJob> listAll() {
        return jdbi.withExtension(DispatchJobDao.class, DispatchJobDao::listAll);
    }

    @Override
    public long count() {
        return jdbi.withExtension(DispatchJobDao.class, DispatchJobDao::count);
    }

    @Override
    public long countWithFilter(DispatchJobFilter filter) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM dispatch_jobs WHERE 1=1");

        if (filter.status() != null) {
            sql.append(" AND status = :status");
        }
        if (filter.source() != null) {
            sql.append(" AND source = :source");
        }
        if (filter.kind() != null) {
            sql.append(" AND kind = :kind");
        }
        if (filter.code() != null) {
            sql.append(" AND code = :code");
        }
        if (filter.clientId() != null) {
            sql.append(" AND client_id = :clientId");
        }
        if (filter.subscriptionId() != null) {
            sql.append(" AND subscription_id = :subscriptionId");
        }
        if (filter.dispatchPoolId() != null) {
            sql.append(" AND dispatch_pool_id = :dispatchPoolId");
        }
        if (filter.messageGroup() != null) {
            sql.append(" AND message_group = :messageGroup");
        }
        if (filter.createdAfter() != null) {
            sql.append(" AND created_at >= :createdAfter");
        }
        if (filter.createdBefore() != null) {
            sql.append(" AND created_at <= :createdBefore");
        }

        String finalSql = sql.toString();
        return jdbi.withHandle(handle -> {
            var query = handle.createQuery(finalSql);

            if (filter.status() != null) {
                query.bind("status", filter.status().name());
            }
            if (filter.source() != null) {
                query.bind("source", filter.source());
            }
            if (filter.kind() != null) {
                query.bind("kind", filter.kind().name());
            }
            if (filter.code() != null) {
                query.bind("code", filter.code());
            }
            if (filter.clientId() != null) {
                query.bind("clientId", filter.clientId());
            }
            if (filter.subscriptionId() != null) {
                query.bind("subscriptionId", filter.subscriptionId());
            }
            if (filter.dispatchPoolId() != null) {
                query.bind("dispatchPoolId", filter.dispatchPoolId());
            }
            if (filter.messageGroup() != null) {
                query.bind("messageGroup", filter.messageGroup());
            }
            if (filter.createdAfter() != null) {
                query.bind("createdAfter", filter.createdAfter());
            }
            if (filter.createdBefore() != null) {
                query.bind("createdBefore", filter.createdBefore());
            }

            return query.mapTo(Long.class).one();
        });
    }

    @Override
    public List<DispatchJob> findPendingJobs(int limit) {
        return jdbi.withExtension(DispatchJobDao.class, dao -> dao.findPendingJobs(limit));
    }

    @Override
    public long countByMessageGroupAndStatus(String messageGroup, DispatchStatus status) {
        return jdbi.withExtension(DispatchJobDao.class, dao ->
            dao.countByMessageGroupAndStatus(messageGroup, status.name()));
    }

    @Override
    public Set<String> findGroupsWithErrors(Set<String> messageGroups) {
        if (messageGroups == null || messageGroups.isEmpty()) {
            return new HashSet<>();
        }

        return jdbi.withHandle(handle ->
            new HashSet<>(handle.createQuery("""
                SELECT DISTINCT message_group FROM dispatch_jobs
                WHERE message_group = ANY(:messageGroups)
                AND status IN ('ERROR', 'IN_PROGRESS')
                """)
                .bindArray("messageGroups", String.class, messageGroups.toArray(new String[0]))
                .mapTo(String.class)
                .list())
        );
    }

    @Override
    public List<DispatchJob> findStaleQueued(Instant threshold) {
        return jdbi.withExtension(DispatchJobDao.class, dao -> dao.findStaleQueued(threshold));
    }

    @Override
    public List<DispatchJob> findStaleQueued(Instant threshold, int limit) {
        return jdbi.withExtension(DispatchJobDao.class, dao -> dao.findStaleQueuedLimited(threshold, limit));
    }

    @Override
    public DispatchJob create(CreateDispatchJobRequest request) {
        DispatchJob job = new DispatchJob();
        job.id = TsidGenerator.generate();
        job.externalId = request.externalId();
        job.source = request.source();
        job.kind = request.kind() != null ? request.kind() : DispatchKind.EVENT;
        job.code = request.code();
        job.subject = request.subject();
        job.eventId = request.eventId();
        job.correlationId = request.correlationId();
        job.targetUrl = request.targetUrl();
        job.protocol = request.protocol() != null ? request.protocol() : DispatchProtocol.HTTP_WEBHOOK;
        job.payload = request.payload();
        job.payloadContentType = request.payloadContentType() != null ? request.payloadContentType() : "application/json";
        job.dataOnly = request.dataOnly() != null ? request.dataOnly() : true;
        job.serviceAccountId = request.serviceAccountId();
        job.clientId = request.clientId();
        job.subscriptionId = request.subscriptionId();
        job.mode = request.mode() != null ? request.mode() : DispatchMode.IMMEDIATE;
        job.dispatchPoolId = request.dispatchPoolId();
        job.messageGroup = request.messageGroup();
        job.sequence = request.sequence() != null ? request.sequence() : 99;
        job.timeoutSeconds = request.timeoutSeconds() != null ? request.timeoutSeconds() : 30;
        job.schemaId = request.schemaId();
        job.maxRetries = request.maxRetries() != null ? request.maxRetries() : 3;
        job.retryStrategy = request.retryStrategy() != null ? request.retryStrategy() : "exponential";
        job.scheduledFor = request.scheduledFor();
        job.expiresAt = request.expiresAt();
        job.idempotencyKey = request.idempotencyKey();
        job.status = DispatchStatus.PENDING;
        job.attemptCount = 0;
        job.createdAt = Instant.now();
        job.updatedAt = Instant.now();

        // Convert Map<String, String> metadata to List<DispatchJobMetadata>
        if (request.metadata() != null) {
            job.metadata = new ArrayList<>();
            for (var entry : request.metadata().entrySet()) {
                job.metadata.add(new DispatchJobMetadata(entry.getKey(), entry.getValue()));
            }
        }
        if (request.headers() != null) {
            job.headers = new HashMap<>(request.headers());
        }

        persist(job);
        return job;
    }

    @Override
    public void addAttempt(String jobId, DispatchAttempt attempt) {
        jdbi.useTransaction(handle -> {
            // Get current job
            var job = handle.attach(DispatchJobDao.class).findById(jobId).orElse(null);
            if (job == null) return;

            // Add attempt
            job.attempts.add(attempt);
            job.attemptCount = job.attempts.size();
            job.lastAttemptAt = attempt.attemptedAt;
            job.updatedAt = Instant.now();

            // Update
            handle.attach(DispatchJobDao.class).update(job,
                JsonHelper.toJsonArray(job.metadata),
                JsonHelper.toJson(job.headers),
                JsonHelper.toJsonArray(job.attempts));
        });
    }

    @Override
    public void updateStatus(String jobId, DispatchStatus status, Instant completedAt, Long durationMillis, String lastError) {
        jdbi.useExtension(DispatchJobDao.class, dao ->
            dao.updateStatus(jobId, status.name(), completedAt, durationMillis, lastError));
    }

    @Override
    public void updateStatusBatch(List<String> ids, DispatchStatus status) {
        if (ids == null || ids.isEmpty()) return;

        jdbi.useHandle(handle ->
            handle.createUpdate("""
                UPDATE dispatch_jobs SET status = :status, updated_at = NOW()
                WHERE id = ANY(:ids)
                """)
                .bind("status", status.name())
                .bindArray("ids", String.class, ids.toArray(new String[0]))
                .execute()
        );
    }

    @Override
    public void persist(DispatchJob job) {
        jdbi.useExtension(DispatchJobDao.class, dao ->
            dao.insert(job,
                JsonHelper.toJsonArray(job.metadata),
                JsonHelper.toJson(job.headers),
                JsonHelper.toJsonArray(job.attempts)));
    }

    @Override
    public void persistAll(List<DispatchJob> jobs) {
        jdbi.useTransaction(handle -> {
            var dao = handle.attach(DispatchJobDao.class);
            for (DispatchJob job : jobs) {
                dao.insert(job,
                    JsonHelper.toJsonArray(job.metadata),
                    JsonHelper.toJson(job.headers),
                    JsonHelper.toJsonArray(job.attempts));
            }
        });
    }

    @Override
    public void update(DispatchJob job) {
        job.updatedAt = Instant.now();
        jdbi.useExtension(DispatchJobDao.class, dao ->
            dao.update(job,
                JsonHelper.toJsonArray(job.metadata),
                JsonHelper.toJson(job.headers),
                JsonHelper.toJsonArray(job.attempts)));
    }

    @Override
    public void delete(DispatchJob job) {
        jdbi.useExtension(DispatchJobDao.class, dao -> dao.deleteById(job.id));
    }

    @Override
    public boolean deleteById(String id) {
        return jdbi.withExtension(DispatchJobDao.class, dao -> dao.deleteById(id) > 0);
    }
}
