package tech.flowcatalyst.event.read.jooq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import tech.flowcatalyst.event.read.EventRead;
import tech.flowcatalyst.event.read.EventRead.ContextDataRead;
import tech.flowcatalyst.event.read.EventReadRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.EventsReadRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.EventsRead.EVENTS_READ;

/**
 * JOOQ-based implementation of EventReadRepository.
 */
@ApplicationScoped
public class JooqEventReadRepository implements EventReadRepository {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    DSLContext dsl;

    @Override
    public EventRead findById(String id) {
        return dsl.selectFrom(EVENTS_READ)
            .where(EVENTS_READ.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public Optional<EventRead> findByIdOptional(String id) {
        return Optional.ofNullable(findById(id));
    }

    @Override
    public List<EventRead> findWithFilter(EventFilter filter) {
        Condition condition = buildFilterCondition(filter);
        return dsl.selectFrom(EVENTS_READ)
            .where(condition)
            .orderBy(EVENTS_READ.TIME.desc())
            .limit(filter.size())
            .offset(filter.page() * filter.size())
            .fetch(this::toDomain);
    }

    @Override
    public List<EventRead> listAll() {
        return dsl.selectFrom(EVENTS_READ)
            .fetch(this::toDomain);
    }

    @Override
    public long count() {
        return dsl.selectCount()
            .from(EVENTS_READ)
            .fetchOne(0, Long.class);
    }

    @Override
    public long countWithFilter(EventFilter filter) {
        Condition condition = buildFilterCondition(filter);
        return dsl.selectCount()
            .from(EVENTS_READ)
            .where(condition)
            .fetchOne(0, Long.class);
    }

    @Override
    public FilterOptions getFilterOptions(FilterOptionsRequest request) {
        // Build base condition from request constraints
        Condition baseCondition = DSL.noCondition();

        if (request.clientIds() != null && !request.clientIds().isEmpty()) {
            baseCondition = baseCondition.and(EVENTS_READ.CLIENT_ID.in(request.clientIds()));
        }
        if (request.applications() != null && !request.applications().isEmpty()) {
            baseCondition = baseCondition.and(EVENTS_READ.APPLICATION.in(request.applications()));
        }
        if (request.subdomains() != null && !request.subdomains().isEmpty()) {
            baseCondition = baseCondition.and(EVENTS_READ.SUBDOMAIN.in(request.subdomains()));
        }
        if (request.aggregates() != null && !request.aggregates().isEmpty()) {
            baseCondition = baseCondition.and(EVENTS_READ.AGGREGATE.in(request.aggregates()));
        }

        // Fetch distinct values
        List<String> clients = dsl.selectDistinct(EVENTS_READ.CLIENT_ID)
            .from(EVENTS_READ)
            .where(baseCondition)
            .and(EVENTS_READ.CLIENT_ID.isNotNull())
            .orderBy(EVENTS_READ.CLIENT_ID)
            .fetch(EVENTS_READ.CLIENT_ID);

        List<String> applications = dsl.selectDistinct(EVENTS_READ.APPLICATION)
            .from(EVENTS_READ)
            .where(baseCondition)
            .and(EVENTS_READ.APPLICATION.isNotNull())
            .orderBy(EVENTS_READ.APPLICATION)
            .fetch(EVENTS_READ.APPLICATION);

        List<String> subdomains = dsl.selectDistinct(EVENTS_READ.SUBDOMAIN)
            .from(EVENTS_READ)
            .where(baseCondition)
            .and(EVENTS_READ.SUBDOMAIN.isNotNull())
            .orderBy(EVENTS_READ.SUBDOMAIN)
            .fetch(EVENTS_READ.SUBDOMAIN);

        List<String> aggregates = dsl.selectDistinct(EVENTS_READ.AGGREGATE)
            .from(EVENTS_READ)
            .where(baseCondition)
            .and(EVENTS_READ.AGGREGATE.isNotNull())
            .orderBy(EVENTS_READ.AGGREGATE)
            .fetch(EVENTS_READ.AGGREGATE);

        List<String> types = dsl.selectDistinct(EVENTS_READ.TYPE)
            .from(EVENTS_READ)
            .where(baseCondition)
            .and(EVENTS_READ.TYPE.isNotNull())
            .orderBy(EVENTS_READ.TYPE)
            .fetch(EVENTS_READ.TYPE);

        return new FilterOptions(clients, applications, subdomains, aggregates, types);
    }

    @Override
    public void persist(EventRead event) {
        EventsReadRecord record = toRecord(event);
        record.setProjectedAt(toOffsetDateTime(Instant.now()));
        dsl.insertInto(EVENTS_READ).set(record).execute();
    }

    @Override
    public void update(EventRead event) {
        EventsReadRecord record = toRecord(event);
        dsl.update(EVENTS_READ)
            .set(record)
            .where(EVENTS_READ.ID.eq(event.id))
            .execute();
    }

    @Override
    public void delete(EventRead event) {
        deleteById(event.id);
    }

    @Override
    public boolean deleteById(String id) {
        return dsl.deleteFrom(EVENTS_READ)
            .where(EVENTS_READ.ID.eq(id))
            .execute() > 0;
    }

    // ========================================================================
    // Filter Condition Builder
    // ========================================================================

    private Condition buildFilterCondition(EventFilter filter) {
        Condition condition = DSL.noCondition();

        if (filter.clientIds() != null && !filter.clientIds().isEmpty()) {
            condition = condition.and(EVENTS_READ.CLIENT_ID.in(filter.clientIds()));
        }
        if (filter.applications() != null && !filter.applications().isEmpty()) {
            condition = condition.and(EVENTS_READ.APPLICATION.in(filter.applications()));
        }
        if (filter.subdomains() != null && !filter.subdomains().isEmpty()) {
            condition = condition.and(EVENTS_READ.SUBDOMAIN.in(filter.subdomains()));
        }
        if (filter.aggregates() != null && !filter.aggregates().isEmpty()) {
            condition = condition.and(EVENTS_READ.AGGREGATE.in(filter.aggregates()));
        }
        if (filter.types() != null && !filter.types().isEmpty()) {
            condition = condition.and(EVENTS_READ.TYPE.in(filter.types()));
        }
        if (filter.source() != null && !filter.source().isBlank()) {
            condition = condition.and(EVENTS_READ.SOURCE.eq(filter.source()));
        }
        if (filter.subject() != null && !filter.subject().isBlank()) {
            condition = condition.and(EVENTS_READ.SUBJECT.eq(filter.subject()));
        }
        if (filter.correlationId() != null && !filter.correlationId().isBlank()) {
            condition = condition.and(EVENTS_READ.CORRELATION_ID.eq(filter.correlationId()));
        }
        if (filter.messageGroup() != null && !filter.messageGroup().isBlank()) {
            condition = condition.and(EVENTS_READ.MESSAGE_GROUP.eq(filter.messageGroup()));
        }
        if (filter.timeAfter() != null) {
            condition = condition.and(EVENTS_READ.TIME.gt(toOffsetDateTime(filter.timeAfter())));
        }
        if (filter.timeBefore() != null) {
            condition = condition.and(EVENTS_READ.TIME.lt(toOffsetDateTime(filter.timeBefore())));
        }

        return condition;
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private EventRead toDomain(Record record) {
        if (record == null) return null;

        EventRead e = new EventRead();
        e.id = record.get(EVENTS_READ.ID);
        e.eventId = record.get(EVENTS_READ.EVENT_ID);
        e.specVersion = record.get(EVENTS_READ.SPEC_VERSION);
        e.type = record.get(EVENTS_READ.TYPE);
        e.application = record.get(EVENTS_READ.APPLICATION);
        e.subdomain = record.get(EVENTS_READ.SUBDOMAIN);
        e.aggregate = record.get(EVENTS_READ.AGGREGATE);
        e.source = record.get(EVENTS_READ.SOURCE);
        e.subject = record.get(EVENTS_READ.SUBJECT);
        e.time = toInstant(record.get(EVENTS_READ.TIME));
        e.data = record.get(EVENTS_READ.DATA);
        e.messageGroup = record.get(EVENTS_READ.MESSAGE_GROUP);
        e.correlationId = record.get(EVENTS_READ.CORRELATION_ID);
        e.causationId = record.get(EVENTS_READ.CAUSATION_ID);
        e.deduplicationId = record.get(EVENTS_READ.DEDUPLICATION_ID);
        e.clientId = record.get(EVENTS_READ.CLIENT_ID);
        e.projectedAt = toInstant(record.get(EVENTS_READ.PROJECTED_AT));

        // Context data (JSONB array)
        String contextDataJson = record.get(EVENTS_READ.CONTEXT_DATA);
        if (contextDataJson != null && !contextDataJson.isBlank()) {
            e.contextData = parseJson(contextDataJson, new TypeReference<List<ContextDataRead>>() {});
            if (e.contextData == null) {
                e.contextData = new ArrayList<>();
            }
        }

        return e;
    }

    private EventsReadRecord toRecord(EventRead e) {
        EventsReadRecord rec = new EventsReadRecord();
        rec.setId(e.id);
        rec.setEventId(e.eventId);
        rec.setSpecVersion(e.specVersion);
        rec.setType(e.type);
        rec.setApplication(e.application);
        rec.setSubdomain(e.subdomain);
        rec.setAggregate(e.aggregate);
        rec.setSource(e.source);
        rec.setSubject(e.subject);
        rec.setTime(toOffsetDateTime(e.time));
        rec.setData(e.data);
        rec.setMessageGroup(e.messageGroup);
        rec.setCorrelationId(e.correlationId);
        rec.setCausationId(e.causationId);
        rec.setDeduplicationId(e.deduplicationId);
        rec.setClientId(e.clientId);

        // Context data -> JSONB array
        rec.setContextData(toJson(e.contextData != null ? e.contextData : new ArrayList<>()));

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
