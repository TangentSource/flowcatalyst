package tech.flowcatalyst.event.jooq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import tech.flowcatalyst.event.ContextData;
import tech.flowcatalyst.event.Event;
import tech.flowcatalyst.event.EventRepository;
import tech.flowcatalyst.platform.common.Page;
import tech.flowcatalyst.platform.jooq.generated.tables.records.EventsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.Events.EVENTS;

/**
 * JOOQ-based implementation of EventRepository.
 */
@ApplicationScoped
public class JooqEventRepository implements EventRepository {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    DSLContext dsl;

    @Override
    public Event findById(String id) {
        return dsl.selectFrom(EVENTS)
            .where(EVENTS.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public Optional<Event> findByIdOptional(String id) {
        return Optional.ofNullable(findById(id));
    }

    @Override
    public Optional<Event> findByDeduplicationId(String deduplicationId) {
        return Optional.ofNullable(
            dsl.selectFrom(EVENTS)
                .where(EVENTS.DEDUPLICATION_ID.eq(deduplicationId))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<Event> listAll() {
        return dsl.selectFrom(EVENTS)
            .fetch(this::toDomain);
    }

    @Override
    public List<Event> findRecentPaged(int page, int size) {
        return dsl.selectFrom(EVENTS)
            .orderBy(EVENTS.TIME.desc())
            .limit(size)
            .offset(page * size)
            .fetch(this::toDomain);
    }

    @Override
    @Deprecated
    public long count() {
        return dsl.selectCount()
            .from(EVENTS)
            .fetchOne(0, Long.class);
    }

    @Override
    public Page<Event> findPage(String afterCursor, int limit) {
        // Fetch one extra to detect if there are more pages
        List<Event> events = dsl.selectFrom(EVENTS)
            .where(afterCursor != null ? EVENTS.ID.gt(afterCursor) : DSL.noCondition())
            .orderBy(EVENTS.ID.asc())
            .limit(limit + 1)
            .fetch(this::toDomain);

        return Page.of(events, limit, e -> e.id);
    }

    @Override
    public boolean existsByDeduplicationId(String deduplicationId) {
        return dsl.fetchExists(
            dsl.selectFrom(EVENTS)
                .where(EVENTS.DEDUPLICATION_ID.eq(deduplicationId))
        );
    }

    @Override
    public void insert(Event event) {
        EventsRecord record = toRecord(event);
        record.setCreatedAt(toOffsetDateTime(Instant.now()));
        dsl.insertInto(EVENTS).set(record).execute();
    }

    @Override
    public void persist(Event event) {
        insert(event);
    }

    @Override
    public void persistAll(List<Event> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        OffsetDateTime now = toOffsetDateTime(Instant.now());
        List<EventsRecord> records = events.stream()
            .map(e -> {
                EventsRecord rec = toRecord(e);
                rec.setCreatedAt(now);
                return rec;
            })
            .toList();
        dsl.batchInsert(records).execute();
    }

    @Override
    public void update(Event event) {
        EventsRecord record = toRecord(event);
        dsl.update(EVENTS)
            .set(record)
            .where(EVENTS.ID.eq(event.id))
            .execute();
    }

    @Override
    public void delete(Event event) {
        deleteById(event.id);
    }

    @Override
    public boolean deleteById(String id) {
        return dsl.deleteFrom(EVENTS)
            .where(EVENTS.ID.eq(id))
            .execute() > 0;
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private Event toDomain(Record record) {
        if (record == null) return null;

        Event e = new Event();
        e.id = record.get(EVENTS.ID);
        e.specVersion = record.get(EVENTS.SPEC_VERSION);
        e.type = record.get(EVENTS.TYPE);
        e.source = record.get(EVENTS.SOURCE);
        e.subject = record.get(EVENTS.SUBJECT);
        e.time = toInstant(record.get(EVENTS.TIME));
        e.data = record.get(EVENTS.DATA);
        e.correlationId = record.get(EVENTS.CORRELATION_ID);
        e.causationId = record.get(EVENTS.CAUSATION_ID);
        e.deduplicationId = record.get(EVENTS.DEDUPLICATION_ID);
        e.messageGroup = record.get(EVENTS.MESSAGE_GROUP);
        e.clientId = record.get(EVENTS.CLIENT_ID);

        // Context data (JSONB array)
        String contextDataJson = record.get(EVENTS.CONTEXT_DATA);
        if (contextDataJson != null && !contextDataJson.isBlank()) {
            e.contextData = parseJson(contextDataJson, new TypeReference<List<ContextData>>() {});
            if (e.contextData == null) {
                e.contextData = new ArrayList<>();
            }
        }

        return e;
    }

    private EventsRecord toRecord(Event e) {
        EventsRecord rec = new EventsRecord();
        rec.setId(e.id);
        rec.setSpecVersion(e.specVersion);
        rec.setType(e.type);
        rec.setSource(e.source);
        rec.setSubject(e.subject);
        rec.setTime(toOffsetDateTime(e.time));
        rec.setData(e.data);
        rec.setCorrelationId(e.correlationId);
        rec.setCausationId(e.causationId);
        rec.setDeduplicationId(e.deduplicationId);
        rec.setMessageGroup(e.messageGroup);
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
