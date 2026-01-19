package tech.flowcatalyst.dispatchjob.read.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import tech.flowcatalyst.dispatchjob.read.DispatchJobRead;
import tech.flowcatalyst.dispatchjob.read.DispatchJobReadRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.DispatchJobsReadRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.DispatchJobsRead.DISPATCH_JOBS_READ;

/**
 * JOOQ-based implementation of DispatchJobReadRepository.
 */
@ApplicationScoped
public class JooqDispatchJobReadRepository implements DispatchJobReadRepository {

    @Inject
    DSLContext dsl;

    @Override
    public DispatchJobRead findById(String id) {
        return dsl.selectFrom(DISPATCH_JOBS_READ)
            .where(DISPATCH_JOBS_READ.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public Optional<DispatchJobRead> findByIdOptional(String id) {
        return Optional.ofNullable(findById(id));
    }

    @Override
    public List<DispatchJobRead> findWithFilter(DispatchJobReadFilter filter) {
        Condition condition = buildFilterCondition(filter);
        return dsl.selectFrom(DISPATCH_JOBS_READ)
            .where(condition)
            .orderBy(DISPATCH_JOBS_READ.CREATED_AT.desc())
            .limit(filter.size())
            .offset(filter.page() * filter.size())
            .fetch(this::toDomain);
    }

    @Override
    public List<DispatchJobRead> listAll() {
        return dsl.selectFrom(DISPATCH_JOBS_READ)
            .fetch(this::toDomain);
    }

    @Override
    public long count() {
        return dsl.selectCount()
            .from(DISPATCH_JOBS_READ)
            .fetchOne(0, Long.class);
    }

    @Override
    public long countWithFilter(DispatchJobReadFilter filter) {
        Condition condition = buildFilterCondition(filter);
        return dsl.selectCount()
            .from(DISPATCH_JOBS_READ)
            .where(condition)
            .fetchOne(0, Long.class);
    }

    @Override
    public FilterOptions getFilterOptions(FilterOptionsRequest request) {
        // Build base condition from request constraints
        Condition baseCondition = DSL.noCondition();

        if (request.clientIds() != null && !request.clientIds().isEmpty()) {
            baseCondition = baseCondition.and(DISPATCH_JOBS_READ.CLIENT_ID.in(request.clientIds()));
        }
        if (request.applications() != null && !request.applications().isEmpty()) {
            baseCondition = baseCondition.and(DISPATCH_JOBS_READ.APPLICATION.in(request.applications()));
        }
        if (request.subdomains() != null && !request.subdomains().isEmpty()) {
            baseCondition = baseCondition.and(DISPATCH_JOBS_READ.SUBDOMAIN.in(request.subdomains()));
        }
        if (request.aggregates() != null && !request.aggregates().isEmpty()) {
            baseCondition = baseCondition.and(DISPATCH_JOBS_READ.AGGREGATE.in(request.aggregates()));
        }

        // Fetch distinct values
        List<String> clients = dsl.selectDistinct(DISPATCH_JOBS_READ.CLIENT_ID)
            .from(DISPATCH_JOBS_READ)
            .where(baseCondition)
            .and(DISPATCH_JOBS_READ.CLIENT_ID.isNotNull())
            .orderBy(DISPATCH_JOBS_READ.CLIENT_ID)
            .fetch(DISPATCH_JOBS_READ.CLIENT_ID);

        List<String> applications = dsl.selectDistinct(DISPATCH_JOBS_READ.APPLICATION)
            .from(DISPATCH_JOBS_READ)
            .where(baseCondition)
            .and(DISPATCH_JOBS_READ.APPLICATION.isNotNull())
            .orderBy(DISPATCH_JOBS_READ.APPLICATION)
            .fetch(DISPATCH_JOBS_READ.APPLICATION);

        List<String> subdomains = dsl.selectDistinct(DISPATCH_JOBS_READ.SUBDOMAIN)
            .from(DISPATCH_JOBS_READ)
            .where(baseCondition)
            .and(DISPATCH_JOBS_READ.SUBDOMAIN.isNotNull())
            .orderBy(DISPATCH_JOBS_READ.SUBDOMAIN)
            .fetch(DISPATCH_JOBS_READ.SUBDOMAIN);

        List<String> aggregates = dsl.selectDistinct(DISPATCH_JOBS_READ.AGGREGATE)
            .from(DISPATCH_JOBS_READ)
            .where(baseCondition)
            .and(DISPATCH_JOBS_READ.AGGREGATE.isNotNull())
            .orderBy(DISPATCH_JOBS_READ.AGGREGATE)
            .fetch(DISPATCH_JOBS_READ.AGGREGATE);

        List<String> codes = dsl.selectDistinct(DISPATCH_JOBS_READ.CODE)
            .from(DISPATCH_JOBS_READ)
            .where(baseCondition)
            .and(DISPATCH_JOBS_READ.CODE.isNotNull())
            .orderBy(DISPATCH_JOBS_READ.CODE)
            .fetch(DISPATCH_JOBS_READ.CODE);

        List<String> statuses = dsl.selectDistinct(DISPATCH_JOBS_READ.STATUS)
            .from(DISPATCH_JOBS_READ)
            .where(baseCondition)
            .and(DISPATCH_JOBS_READ.STATUS.isNotNull())
            .orderBy(DISPATCH_JOBS_READ.STATUS)
            .fetch(DISPATCH_JOBS_READ.STATUS);

        return new FilterOptions(clients, applications, subdomains, aggregates, codes, statuses);
    }

    @Override
    public void persist(DispatchJobRead job) {
        DispatchJobsReadRecord record = toRecord(job);
        record.setProjectedAt(toOffsetDateTime(Instant.now()));
        dsl.insertInto(DISPATCH_JOBS_READ).set(record).execute();
    }

    @Override
    public void update(DispatchJobRead job) {
        DispatchJobsReadRecord record = toRecord(job);
        dsl.update(DISPATCH_JOBS_READ)
            .set(record)
            .where(DISPATCH_JOBS_READ.ID.eq(job.id))
            .execute();
    }

    @Override
    public void delete(DispatchJobRead job) {
        deleteById(job.id);
    }

    @Override
    public boolean deleteById(String id) {
        return dsl.deleteFrom(DISPATCH_JOBS_READ)
            .where(DISPATCH_JOBS_READ.ID.eq(id))
            .execute() > 0;
    }

    // ========================================================================
    // Filter Condition Builder
    // ========================================================================

    private Condition buildFilterCondition(DispatchJobReadFilter filter) {
        Condition condition = DSL.noCondition();

        if (filter.clientIds() != null && !filter.clientIds().isEmpty()) {
            condition = condition.and(DISPATCH_JOBS_READ.CLIENT_ID.in(filter.clientIds()));
        }
        if (filter.statuses() != null && !filter.statuses().isEmpty()) {
            condition = condition.and(DISPATCH_JOBS_READ.STATUS.in(filter.statuses()));
        }
        if (filter.applications() != null && !filter.applications().isEmpty()) {
            condition = condition.and(DISPATCH_JOBS_READ.APPLICATION.in(filter.applications()));
        }
        if (filter.subdomains() != null && !filter.subdomains().isEmpty()) {
            condition = condition.and(DISPATCH_JOBS_READ.SUBDOMAIN.in(filter.subdomains()));
        }
        if (filter.aggregates() != null && !filter.aggregates().isEmpty()) {
            condition = condition.and(DISPATCH_JOBS_READ.AGGREGATE.in(filter.aggregates()));
        }
        if (filter.codes() != null && !filter.codes().isEmpty()) {
            condition = condition.and(DISPATCH_JOBS_READ.CODE.in(filter.codes()));
        }
        if (filter.source() != null && !filter.source().isBlank()) {
            condition = condition.and(DISPATCH_JOBS_READ.SOURCE.eq(filter.source()));
        }
        if (filter.kind() != null && !filter.kind().isBlank()) {
            condition = condition.and(DISPATCH_JOBS_READ.KIND.eq(filter.kind()));
        }
        if (filter.subscriptionId() != null && !filter.subscriptionId().isBlank()) {
            condition = condition.and(DISPATCH_JOBS_READ.SUBSCRIPTION_ID.eq(filter.subscriptionId()));
        }
        if (filter.dispatchPoolId() != null && !filter.dispatchPoolId().isBlank()) {
            condition = condition.and(DISPATCH_JOBS_READ.DISPATCH_POOL_ID.eq(filter.dispatchPoolId()));
        }
        if (filter.messageGroup() != null && !filter.messageGroup().isBlank()) {
            condition = condition.and(DISPATCH_JOBS_READ.MESSAGE_GROUP.eq(filter.messageGroup()));
        }
        if (filter.createdAfter() != null) {
            condition = condition.and(DISPATCH_JOBS_READ.CREATED_AT.gt(toOffsetDateTime(filter.createdAfter())));
        }
        if (filter.createdBefore() != null) {
            condition = condition.and(DISPATCH_JOBS_READ.CREATED_AT.lt(toOffsetDateTime(filter.createdBefore())));
        }

        return condition;
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private DispatchJobRead toDomain(Record record) {
        if (record == null) return null;

        DispatchJobRead job = new DispatchJobRead();
        job.id = record.get(DISPATCH_JOBS_READ.ID);
        job.externalId = record.get(DISPATCH_JOBS_READ.EXTERNAL_ID);
        job.source = record.get(DISPATCH_JOBS_READ.SOURCE);
        job.kind = record.get(DISPATCH_JOBS_READ.KIND);
        job.code = record.get(DISPATCH_JOBS_READ.CODE);
        job.subject = record.get(DISPATCH_JOBS_READ.SUBJECT);
        job.application = record.get(DISPATCH_JOBS_READ.APPLICATION);
        job.subdomain = record.get(DISPATCH_JOBS_READ.SUBDOMAIN);
        job.aggregate = record.get(DISPATCH_JOBS_READ.AGGREGATE);
        job.eventId = record.get(DISPATCH_JOBS_READ.EVENT_ID);
        job.correlationId = record.get(DISPATCH_JOBS_READ.CORRELATION_ID);
        job.targetUrl = record.get(DISPATCH_JOBS_READ.TARGET_URL);
        job.protocol = record.get(DISPATCH_JOBS_READ.PROTOCOL);
        job.clientId = record.get(DISPATCH_JOBS_READ.CLIENT_ID);
        job.subscriptionId = record.get(DISPATCH_JOBS_READ.SUBSCRIPTION_ID);
        job.serviceAccountId = record.get(DISPATCH_JOBS_READ.SERVICE_ACCOUNT_ID);
        job.dispatchPoolId = record.get(DISPATCH_JOBS_READ.DISPATCH_POOL_ID);
        job.messageGroup = record.get(DISPATCH_JOBS_READ.MESSAGE_GROUP);
        job.mode = record.get(DISPATCH_JOBS_READ.MODE);
        job.sequence = record.get(DISPATCH_JOBS_READ.SEQUENCE);
        job.status = record.get(DISPATCH_JOBS_READ.STATUS);
        job.attemptCount = record.get(DISPATCH_JOBS_READ.ATTEMPT_COUNT);
        job.maxRetries = record.get(DISPATCH_JOBS_READ.MAX_RETRIES);
        job.lastError = record.get(DISPATCH_JOBS_READ.LAST_ERROR);
        job.timeoutSeconds = record.get(DISPATCH_JOBS_READ.TIMEOUT_SECONDS);
        job.retryStrategy = record.get(DISPATCH_JOBS_READ.RETRY_STRATEGY);
        job.createdAt = toInstant(record.get(DISPATCH_JOBS_READ.CREATED_AT));
        job.updatedAt = toInstant(record.get(DISPATCH_JOBS_READ.UPDATED_AT));
        job.scheduledFor = toInstant(record.get(DISPATCH_JOBS_READ.SCHEDULED_FOR));
        job.expiresAt = toInstant(record.get(DISPATCH_JOBS_READ.EXPIRES_AT));
        job.completedAt = toInstant(record.get(DISPATCH_JOBS_READ.COMPLETED_AT));
        job.lastAttemptAt = toInstant(record.get(DISPATCH_JOBS_READ.LAST_ATTEMPT_AT));
        job.durationMillis = record.get(DISPATCH_JOBS_READ.DURATION_MILLIS);
        job.idempotencyKey = record.get(DISPATCH_JOBS_READ.IDEMPOTENCY_KEY);
        job.isCompleted = record.get(DISPATCH_JOBS_READ.IS_COMPLETED);
        job.isTerminal = record.get(DISPATCH_JOBS_READ.IS_TERMINAL);
        job.projectedAt = toInstant(record.get(DISPATCH_JOBS_READ.PROJECTED_AT));

        return job;
    }

    private DispatchJobsReadRecord toRecord(DispatchJobRead job) {
        DispatchJobsReadRecord rec = new DispatchJobsReadRecord();
        rec.setId(job.id);
        rec.setExternalId(job.externalId);
        rec.setSource(job.source);
        rec.setKind(job.kind);
        rec.setCode(job.code);
        rec.setSubject(job.subject);
        rec.setApplication(job.application);
        rec.setSubdomain(job.subdomain);
        rec.setAggregate(job.aggregate);
        rec.setEventId(job.eventId);
        rec.setCorrelationId(job.correlationId);
        rec.setTargetUrl(job.targetUrl);
        rec.setProtocol(job.protocol);
        rec.setClientId(job.clientId);
        rec.setSubscriptionId(job.subscriptionId);
        rec.setServiceAccountId(job.serviceAccountId);
        rec.setDispatchPoolId(job.dispatchPoolId);
        rec.setMessageGroup(job.messageGroup);
        rec.setMode(job.mode);
        rec.setSequence(job.sequence);
        rec.setStatus(job.status);
        rec.setAttemptCount(job.attemptCount);
        rec.setMaxRetries(job.maxRetries);
        rec.setLastError(job.lastError);
        rec.setTimeoutSeconds(job.timeoutSeconds);
        rec.setRetryStrategy(job.retryStrategy);
        rec.setCreatedAt(toOffsetDateTime(job.createdAt));
        rec.setUpdatedAt(toOffsetDateTime(job.updatedAt));
        rec.setScheduledFor(toOffsetDateTime(job.scheduledFor));
        rec.setExpiresAt(toOffsetDateTime(job.expiresAt));
        rec.setCompletedAt(toOffsetDateTime(job.completedAt));
        rec.setLastAttemptAt(toOffsetDateTime(job.lastAttemptAt));
        rec.setDurationMillis(job.durationMillis);
        rec.setIdempotencyKey(job.idempotencyKey);
        rec.setIsCompleted(job.isCompleted);
        rec.setIsTerminal(job.isTerminal);

        return rec;
    }

    // ========================================================================
    // Utility Methods
    // ========================================================================

    private Instant toInstant(OffsetDateTime odt) {
        return odt != null ? odt.toInstant() : null;
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant != null ? instant.atOffset(ZoneOffset.UTC) : null;
    }
}
