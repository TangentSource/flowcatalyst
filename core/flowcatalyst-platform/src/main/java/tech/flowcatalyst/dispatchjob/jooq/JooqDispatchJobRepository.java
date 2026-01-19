package tech.flowcatalyst.dispatchjob.jooq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import tech.flowcatalyst.dispatch.DispatchMode;
import tech.flowcatalyst.dispatchjob.dto.CreateDispatchJobRequest;
import tech.flowcatalyst.dispatchjob.dto.DispatchJobFilter;
import tech.flowcatalyst.dispatchjob.entity.DispatchAttempt;
import tech.flowcatalyst.dispatchjob.entity.DispatchJob;
import tech.flowcatalyst.dispatchjob.entity.DispatchJobMetadata;
import tech.flowcatalyst.dispatchjob.model.DispatchKind;
import tech.flowcatalyst.dispatchjob.model.DispatchProtocol;
import tech.flowcatalyst.dispatchjob.model.DispatchStatus;
import tech.flowcatalyst.dispatchjob.repository.DispatchJobRepository;
import tech.flowcatalyst.platform.common.Page;
import tech.flowcatalyst.platform.jooq.generated.tables.records.DispatchJobsRecord;
import tech.flowcatalyst.platform.shared.TsidGenerator;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

import static tech.flowcatalyst.platform.jooq.generated.tables.DispatchJobs.DISPATCH_JOBS;

/**
 * JOOQ-based implementation of DispatchJobRepository.
 */
@ApplicationScoped
public class JooqDispatchJobRepository implements DispatchJobRepository {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    DSLContext dsl;

    @Override
    public DispatchJob findById(String id) {
        return dsl.selectFrom(DISPATCH_JOBS)
            .where(DISPATCH_JOBS.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public Optional<DispatchJob> findByIdOptional(String id) {
        return Optional.ofNullable(findById(id));
    }

    @Override
    public List<DispatchJob> findWithFilter(DispatchJobFilter filter) {
        Condition condition = buildFilterCondition(filter);
        return dsl.selectFrom(DISPATCH_JOBS)
            .where(condition)
            .orderBy(DISPATCH_JOBS.CREATED_AT.desc())
            .limit(filter.size())
            .offset(filter.page() * filter.size())
            .fetch(this::toDomain);
    }

    @Override
    public List<DispatchJob> findByMetadata(String key, String value) {
        // Query JSONB array for matching metadata
        return dsl.selectFrom(DISPATCH_JOBS)
            .where(DSL.condition(
                "metadata::jsonb @> ?::jsonb",
                "[{\"key\":\"" + key + "\",\"value\":\"" + value + "\"}]"
            ))
            .orderBy(DISPATCH_JOBS.CREATED_AT.desc())
            .fetch(this::toDomain);
    }

    @Override
    public List<DispatchJob> findByMetadataFilters(Map<String, String> metadataFilters) {
        if (metadataFilters == null || metadataFilters.isEmpty()) {
            return new ArrayList<>();
        }

        Condition condition = DSL.noCondition();
        for (Map.Entry<String, String> entry : metadataFilters.entrySet()) {
            condition = condition.and(DSL.condition(
                "metadata::jsonb @> ?::jsonb",
                "[{\"key\":\"" + entry.getKey() + "\",\"value\":\"" + entry.getValue() + "\"}]"
            ));
        }

        return dsl.selectFrom(DISPATCH_JOBS)
            .where(condition)
            .orderBy(DISPATCH_JOBS.CREATED_AT.desc())
            .fetch(this::toDomain);
    }

    @Override
    public List<DispatchJob> findRecentPaged(int page, int size) {
        return dsl.selectFrom(DISPATCH_JOBS)
            .orderBy(DISPATCH_JOBS.CREATED_AT.desc())
            .limit(size)
            .offset(page * size)
            .fetch(this::toDomain);
    }

    @Override
    public List<DispatchJob> listAll() {
        return dsl.selectFrom(DISPATCH_JOBS)
            .fetch(this::toDomain);
    }

    @Override
    @Deprecated
    public long count() {
        return dsl.selectCount()
            .from(DISPATCH_JOBS)
            .fetchOne(0, Long.class);
    }

    @Override
    @Deprecated
    public long countWithFilter(DispatchJobFilter filter) {
        Condition condition = buildFilterCondition(filter);
        return dsl.selectCount()
            .from(DISPATCH_JOBS)
            .where(condition)
            .fetchOne(0, Long.class);
    }

    @Override
    public Page<DispatchJob> findPage(String afterCursor, int limit) {
        // Fetch one extra to detect if there are more pages
        List<DispatchJob> jobs = dsl.selectFrom(DISPATCH_JOBS)
            .where(afterCursor != null ? DISPATCH_JOBS.ID.gt(afterCursor) : DSL.noCondition())
            .orderBy(DISPATCH_JOBS.ID.asc())
            .limit(limit + 1)
            .fetch(this::toDomain);

        return Page.of(jobs, limit, j -> j.id);
    }

    // ========================================================================
    // Scheduler Query Methods
    // ========================================================================

    @Override
    public List<DispatchJob> findPendingJobs(int limit) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        return dsl.selectFrom(DISPATCH_JOBS)
            .where(DISPATCH_JOBS.STATUS.eq(DispatchStatus.PENDING.name()))
            .and(DISPATCH_JOBS.SCHEDULED_FOR.isNull().or(DISPATCH_JOBS.SCHEDULED_FOR.le(now)))
            .and(DISPATCH_JOBS.EXPIRES_AT.isNull().or(DISPATCH_JOBS.EXPIRES_AT.gt(now)))
            .orderBy(DISPATCH_JOBS.SEQUENCE, DISPATCH_JOBS.CREATED_AT)
            .limit(limit)
            .fetch(this::toDomain);
    }

    @Override
    public long countByMessageGroupAndStatus(String messageGroup, DispatchStatus status) {
        return dsl.selectCount()
            .from(DISPATCH_JOBS)
            .where(DISPATCH_JOBS.MESSAGE_GROUP.eq(messageGroup))
            .and(DISPATCH_JOBS.STATUS.eq(status.name()))
            .fetchOne(0, Long.class);
    }

    @Override
    public Set<String> findGroupsWithErrors(Set<String> messageGroups) {
        if (messageGroups == null || messageGroups.isEmpty()) {
            return new HashSet<>();
        }
        return dsl.selectDistinct(DISPATCH_JOBS.MESSAGE_GROUP)
            .from(DISPATCH_JOBS)
            .where(DISPATCH_JOBS.MESSAGE_GROUP.in(messageGroups))
            .and(DISPATCH_JOBS.STATUS.eq(DispatchStatus.ERROR.name()))
            .fetch(DISPATCH_JOBS.MESSAGE_GROUP)
            .stream()
            .collect(Collectors.toSet());
    }

    // ========================================================================
    // Stale Job Queries
    // ========================================================================

    @Override
    public List<DispatchJob> findStaleQueued(Instant threshold) {
        return dsl.selectFrom(DISPATCH_JOBS)
            .where(DISPATCH_JOBS.STATUS.eq(DispatchStatus.QUEUED.name()))
            .and(DISPATCH_JOBS.UPDATED_AT.lt(toOffsetDateTime(threshold)))
            .orderBy(DISPATCH_JOBS.CREATED_AT)
            .fetch(this::toDomain);
    }

    @Override
    public List<DispatchJob> findStaleQueued(Instant threshold, int limit) {
        return dsl.selectFrom(DISPATCH_JOBS)
            .where(DISPATCH_JOBS.STATUS.eq(DispatchStatus.QUEUED.name()))
            .and(DISPATCH_JOBS.UPDATED_AT.lt(toOffsetDateTime(threshold)))
            .orderBy(DISPATCH_JOBS.CREATED_AT)
            .limit(limit)
            .fetch(this::toDomain);
    }

    // ========================================================================
    // Write Operations
    // ========================================================================

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
        job.headers = request.headers() != null ? new HashMap<>(request.headers()) : new HashMap<>();
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
        job.status = DispatchStatus.PENDING;
        job.maxRetries = request.maxRetries() != null ? request.maxRetries() : 3;
        job.retryStrategy = request.retryStrategy() != null ? request.retryStrategy() : "exponential";
        job.scheduledFor = request.scheduledFor();
        job.expiresAt = request.expiresAt();
        job.idempotencyKey = request.idempotencyKey();

        // Convert metadata map to list
        if (request.metadata() != null) {
            job.metadata = request.metadata().entrySet().stream()
                .map(e -> new DispatchJobMetadata(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        }

        job.createdAt = Instant.now();
        job.updatedAt = Instant.now();

        persist(job);
        return job;
    }

    @Override
    public void addAttempt(String jobId, DispatchAttempt attempt) {
        // Read current attempts, add new one, and update
        DispatchJob job = findById(jobId);
        if (job != null) {
            if (job.attempts == null) {
                job.attempts = new ArrayList<>();
            }
            job.attempts.add(attempt);
            job.attemptCount = job.attempts.size();
            job.lastAttemptAt = attempt.attemptedAt;
            job.updatedAt = Instant.now();

            dsl.update(DISPATCH_JOBS)
                .set(DISPATCH_JOBS.ATTEMPTS, toJson(job.attempts))
                .set(DISPATCH_JOBS.ATTEMPT_COUNT, job.attemptCount)
                .set(DISPATCH_JOBS.LAST_ATTEMPT_AT, toOffsetDateTime(job.lastAttemptAt))
                .set(DISPATCH_JOBS.UPDATED_AT, toOffsetDateTime(job.updatedAt))
                .where(DISPATCH_JOBS.ID.eq(jobId))
                .execute();
        }
    }

    @Override
    public void updateStatus(String jobId, DispatchStatus status, Instant completedAt, Long durationMillis, String lastError) {
        dsl.update(DISPATCH_JOBS)
            .set(DISPATCH_JOBS.STATUS, status.name())
            .set(DISPATCH_JOBS.COMPLETED_AT, toOffsetDateTime(completedAt))
            .set(DISPATCH_JOBS.DURATION_MILLIS, durationMillis)
            .set(DISPATCH_JOBS.LAST_ERROR, lastError)
            .set(DISPATCH_JOBS.UPDATED_AT, toOffsetDateTime(Instant.now()))
            .where(DISPATCH_JOBS.ID.eq(jobId))
            .execute();
    }

    @Override
    public void updateStatusBatch(List<String> ids, DispatchStatus status) {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        dsl.update(DISPATCH_JOBS)
            .set(DISPATCH_JOBS.STATUS, status.name())
            .set(DISPATCH_JOBS.UPDATED_AT, toOffsetDateTime(Instant.now()))
            .where(DISPATCH_JOBS.ID.in(ids))
            .execute();
    }

    @Override
    public void persist(DispatchJob job) {
        DispatchJobsRecord record = toRecord(job);
        record.setCreatedAt(toOffsetDateTime(job.createdAt));
        record.setUpdatedAt(toOffsetDateTime(job.updatedAt));
        dsl.insertInto(DISPATCH_JOBS).set(record).execute();
    }

    @Override
    public void persistAll(List<DispatchJob> jobs) {
        if (jobs == null || jobs.isEmpty()) {
            return;
        }
        List<DispatchJobsRecord> records = jobs.stream()
            .map(job -> {
                DispatchJobsRecord rec = toRecord(job);
                rec.setCreatedAt(toOffsetDateTime(job.createdAt));
                rec.setUpdatedAt(toOffsetDateTime(job.updatedAt));
                return rec;
            })
            .toList();
        dsl.batchInsert(records).execute();
    }

    @Override
    public void update(DispatchJob job) {
        job.updatedAt = Instant.now();
        DispatchJobsRecord record = toRecord(job);
        record.setUpdatedAt(toOffsetDateTime(job.updatedAt));
        dsl.update(DISPATCH_JOBS)
            .set(record)
            .where(DISPATCH_JOBS.ID.eq(job.id))
            .execute();
    }

    @Override
    public void delete(DispatchJob job) {
        deleteById(job.id);
    }

    @Override
    public boolean deleteById(String id) {
        return dsl.deleteFrom(DISPATCH_JOBS)
            .where(DISPATCH_JOBS.ID.eq(id))
            .execute() > 0;
    }

    // ========================================================================
    // Filter Condition Builder
    // ========================================================================

    private Condition buildFilterCondition(DispatchJobFilter filter) {
        Condition condition = DSL.noCondition();

        if (filter.status() != null) {
            condition = condition.and(DISPATCH_JOBS.STATUS.eq(filter.status().name()));
        }
        if (filter.source() != null && !filter.source().isBlank()) {
            condition = condition.and(DISPATCH_JOBS.SOURCE.eq(filter.source()));
        }
        if (filter.kind() != null) {
            condition = condition.and(DISPATCH_JOBS.KIND.eq(filter.kind().name()));
        }
        if (filter.code() != null && !filter.code().isBlank()) {
            condition = condition.and(DISPATCH_JOBS.CODE.eq(filter.code()));
        }
        if (filter.clientId() != null && !filter.clientId().isBlank()) {
            condition = condition.and(DISPATCH_JOBS.CLIENT_ID.eq(filter.clientId()));
        }
        if (filter.subscriptionId() != null && !filter.subscriptionId().isBlank()) {
            condition = condition.and(DISPATCH_JOBS.SUBSCRIPTION_ID.eq(filter.subscriptionId()));
        }
        if (filter.dispatchPoolId() != null && !filter.dispatchPoolId().isBlank()) {
            condition = condition.and(DISPATCH_JOBS.DISPATCH_POOL_ID.eq(filter.dispatchPoolId()));
        }
        if (filter.messageGroup() != null && !filter.messageGroup().isBlank()) {
            condition = condition.and(DISPATCH_JOBS.MESSAGE_GROUP.eq(filter.messageGroup()));
        }
        if (filter.createdAfter() != null) {
            condition = condition.and(DISPATCH_JOBS.CREATED_AT.gt(toOffsetDateTime(filter.createdAfter())));
        }
        if (filter.createdBefore() != null) {
            condition = condition.and(DISPATCH_JOBS.CREATED_AT.lt(toOffsetDateTime(filter.createdBefore())));
        }

        return condition;
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private DispatchJob toDomain(Record record) {
        if (record == null) return null;

        DispatchJob job = new DispatchJob();
        job.id = record.get(DISPATCH_JOBS.ID);
        job.externalId = record.get(DISPATCH_JOBS.EXTERNAL_ID);
        job.source = record.get(DISPATCH_JOBS.SOURCE);
        job.kind = parseEnum(record.get(DISPATCH_JOBS.KIND), DispatchKind.class);
        job.code = record.get(DISPATCH_JOBS.CODE);
        job.subject = record.get(DISPATCH_JOBS.SUBJECT);
        job.eventId = record.get(DISPATCH_JOBS.EVENT_ID);
        job.correlationId = record.get(DISPATCH_JOBS.CORRELATION_ID);
        job.targetUrl = record.get(DISPATCH_JOBS.TARGET_URL);
        job.protocol = parseEnum(record.get(DISPATCH_JOBS.PROTOCOL), DispatchProtocol.class);
        job.payload = record.get(DISPATCH_JOBS.PAYLOAD);
        job.payloadContentType = record.get(DISPATCH_JOBS.PAYLOAD_CONTENT_TYPE);
        job.dataOnly = record.get(DISPATCH_JOBS.DATA_ONLY);
        job.serviceAccountId = record.get(DISPATCH_JOBS.SERVICE_ACCOUNT_ID);
        job.clientId = record.get(DISPATCH_JOBS.CLIENT_ID);
        job.subscriptionId = record.get(DISPATCH_JOBS.SUBSCRIPTION_ID);
        job.mode = parseEnum(record.get(DISPATCH_JOBS.MODE), DispatchMode.class);
        job.dispatchPoolId = record.get(DISPATCH_JOBS.DISPATCH_POOL_ID);
        job.messageGroup = record.get(DISPATCH_JOBS.MESSAGE_GROUP);
        job.sequence = record.get(DISPATCH_JOBS.SEQUENCE);
        job.timeoutSeconds = record.get(DISPATCH_JOBS.TIMEOUT_SECONDS);
        job.schemaId = record.get(DISPATCH_JOBS.SCHEMA_ID);
        job.status = parseEnum(record.get(DISPATCH_JOBS.STATUS), DispatchStatus.class);
        job.maxRetries = record.get(DISPATCH_JOBS.MAX_RETRIES);
        job.retryStrategy = record.get(DISPATCH_JOBS.RETRY_STRATEGY);
        job.scheduledFor = toInstant(record.get(DISPATCH_JOBS.SCHEDULED_FOR));
        job.expiresAt = toInstant(record.get(DISPATCH_JOBS.EXPIRES_AT));
        job.attemptCount = record.get(DISPATCH_JOBS.ATTEMPT_COUNT);
        job.lastAttemptAt = toInstant(record.get(DISPATCH_JOBS.LAST_ATTEMPT_AT));
        job.completedAt = toInstant(record.get(DISPATCH_JOBS.COMPLETED_AT));
        job.durationMillis = record.get(DISPATCH_JOBS.DURATION_MILLIS);
        job.lastError = record.get(DISPATCH_JOBS.LAST_ERROR);
        job.idempotencyKey = record.get(DISPATCH_JOBS.IDEMPOTENCY_KEY);
        job.createdAt = toInstant(record.get(DISPATCH_JOBS.CREATED_AT));
        job.updatedAt = toInstant(record.get(DISPATCH_JOBS.UPDATED_AT));

        // JSONB fields
        String metadataJson = record.get(DISPATCH_JOBS.METADATA);
        if (metadataJson != null && !metadataJson.isBlank()) {
            job.metadata = parseJson(metadataJson, new TypeReference<List<DispatchJobMetadata>>() {});
            if (job.metadata == null) {
                job.metadata = new ArrayList<>();
            }
        }

        String headersJson = record.get(DISPATCH_JOBS.HEADERS);
        if (headersJson != null && !headersJson.isBlank()) {
            job.headers = parseJson(headersJson, new TypeReference<Map<String, String>>() {});
            if (job.headers == null) {
                job.headers = new HashMap<>();
            }
        }

        String attemptsJson = record.get(DISPATCH_JOBS.ATTEMPTS);
        if (attemptsJson != null && !attemptsJson.isBlank()) {
            job.attempts = parseJson(attemptsJson, new TypeReference<List<DispatchAttempt>>() {});
            if (job.attempts == null) {
                job.attempts = new ArrayList<>();
            }
        }

        return job;
    }

    private DispatchJobsRecord toRecord(DispatchJob job) {
        DispatchJobsRecord rec = new DispatchJobsRecord();
        rec.setId(job.id);
        rec.setExternalId(job.externalId);
        rec.setSource(job.source);
        rec.setKind(job.kind != null ? job.kind.name() : DispatchKind.EVENT.name());
        rec.setCode(job.code);
        rec.setSubject(job.subject);
        rec.setEventId(job.eventId);
        rec.setCorrelationId(job.correlationId);
        rec.setTargetUrl(job.targetUrl);
        rec.setProtocol(job.protocol != null ? job.protocol.name() : DispatchProtocol.HTTP_WEBHOOK.name());
        rec.setPayload(job.payload);
        rec.setPayloadContentType(job.payloadContentType);
        rec.setDataOnly(job.dataOnly);
        rec.setServiceAccountId(job.serviceAccountId);
        rec.setClientId(job.clientId);
        rec.setSubscriptionId(job.subscriptionId);
        rec.setMode(job.mode != null ? job.mode.name() : DispatchMode.IMMEDIATE.name());
        rec.setDispatchPoolId(job.dispatchPoolId);
        rec.setMessageGroup(job.messageGroup);
        rec.setSequence(job.sequence);
        rec.setTimeoutSeconds(job.timeoutSeconds);
        rec.setSchemaId(job.schemaId);
        rec.setStatus(job.status != null ? job.status.name() : DispatchStatus.PENDING.name());
        rec.setMaxRetries(job.maxRetries);
        rec.setRetryStrategy(job.retryStrategy);
        rec.setScheduledFor(toOffsetDateTime(job.scheduledFor));
        rec.setExpiresAt(toOffsetDateTime(job.expiresAt));
        rec.setAttemptCount(job.attemptCount);
        rec.setLastAttemptAt(toOffsetDateTime(job.lastAttemptAt));
        rec.setCompletedAt(toOffsetDateTime(job.completedAt));
        rec.setDurationMillis(job.durationMillis);
        rec.setLastError(job.lastError);
        rec.setIdempotencyKey(job.idempotencyKey);

        // JSONB fields
        rec.setMetadata(toJson(job.metadata != null ? job.metadata : new ArrayList<>()));
        rec.setHeaders(toJson(job.headers != null ? job.headers : new HashMap<>()));
        rec.setAttempts(toJson(job.attempts != null ? job.attempts : new ArrayList<>()));

        return rec;
    }

    // ========================================================================
    // Utility Methods
    // ========================================================================

    private <E extends Enum<E>> E parseEnum(String value, Class<E> enumClass) {
        if (value == null) return null;
        try {
            return Enum.valueOf(enumClass, value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private Instant toInstant(OffsetDateTime odt) {
        return odt != null ? odt.toInstant() : null;
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant != null ? instant.atOffset(ZoneOffset.UTC) : null;
    }

    private <T> T parseJson(String json, TypeReference<T> typeRef) {
        if (json == null || json.isBlank()) return null;
        try {
            return objectMapper.readValue(json, typeRef);
        } catch (Exception ex) {
            return null;
        }
    }

    private String toJson(Object obj) {
        if (obj == null) return null;
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception ex) {
            return null;
        }
    }
}
