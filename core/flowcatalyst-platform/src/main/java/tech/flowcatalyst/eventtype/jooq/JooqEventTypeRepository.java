package tech.flowcatalyst.eventtype.jooq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import tech.flowcatalyst.eventtype.EventType;
import tech.flowcatalyst.eventtype.EventTypeRepository;
import tech.flowcatalyst.eventtype.EventTypeStatus;
import tech.flowcatalyst.eventtype.SpecVersion;
import tech.flowcatalyst.platform.jooq.generated.tables.records.EventTypesRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static tech.flowcatalyst.platform.jooq.generated.tables.EventTypes.EVENT_TYPES;

/**
 * JOOQ-based implementation of EventTypeRepository.
 */
@ApplicationScoped
public class JooqEventTypeRepository implements EventTypeRepository {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    DSLContext dsl;

    @Override
    public EventType findById(String id) {
        return dsl.selectFrom(EVENT_TYPES)
            .where(EVENT_TYPES.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public Optional<EventType> findByIdOptional(String id) {
        return Optional.ofNullable(findById(id));
    }

    @Override
    public Optional<EventType> findByCode(String code) {
        return Optional.ofNullable(
            dsl.selectFrom(EVENT_TYPES)
                .where(EVENT_TYPES.CODE.eq(code))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<EventType> findAllOrdered() {
        return dsl.selectFrom(EVENT_TYPES)
            .orderBy(EVENT_TYPES.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<EventType> findCurrent() {
        return dsl.selectFrom(EVENT_TYPES)
            .where(EVENT_TYPES.STATUS.eq(EventTypeStatus.CURRENT.name()))
            .orderBy(EVENT_TYPES.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<EventType> findArchived() {
        return dsl.selectFrom(EVENT_TYPES)
            .where(EVENT_TYPES.STATUS.eq(EventTypeStatus.ARCHIVE.name()))
            .orderBy(EVENT_TYPES.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<EventType> findByCodePrefix(String prefix) {
        return dsl.selectFrom(EVENT_TYPES)
            .where(EVENT_TYPES.CODE.startsWith(prefix))
            .orderBy(EVENT_TYPES.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<EventType> listAll() {
        return dsl.selectFrom(EVENT_TYPES)
            .fetch(this::toDomain);
    }

    @Override
    public long count() {
        return dsl.selectCount()
            .from(EVENT_TYPES)
            .fetchOne(0, Long.class);
    }

    @Override
    public boolean existsByCode(String code) {
        return dsl.fetchExists(
            dsl.selectFrom(EVENT_TYPES)
                .where(EVENT_TYPES.CODE.eq(code))
        );
    }

    // ========================================================================
    // Aggregation Queries for Code Segments
    // Code format: {application}:{subdomain}:{aggregate}:{event}
    // ========================================================================

    @Override
    public List<String> findDistinctApplications() {
        // Extract first segment of code (before first ':')
        return dsl.selectDistinct(DSL.field("split_part(code, ':', 1)", String.class))
            .from(EVENT_TYPES)
            .orderBy(1)
            .fetch()
            .map(r -> r.value1());
    }

    @Override
    public List<String> findDistinctSubdomains(String application) {
        // Extract second segment of code for matching application
        return dsl.selectDistinct(DSL.field("split_part(code, ':', 2)", String.class))
            .from(EVENT_TYPES)
            .where(DSL.field("split_part(code, ':', 1)", String.class).eq(application))
            .orderBy(1)
            .fetch()
            .map(r -> r.value1());
    }

    @Override
    public List<String> findAllDistinctSubdomains() {
        return dsl.selectDistinct(DSL.field("split_part(code, ':', 2)", String.class))
            .from(EVENT_TYPES)
            .orderBy(1)
            .fetch()
            .map(r -> r.value1());
    }

    @Override
    public List<String> findDistinctSubdomains(List<String> applications) {
        if (applications == null || applications.isEmpty()) {
            return findAllDistinctSubdomains();
        }
        return dsl.selectDistinct(DSL.field("split_part(code, ':', 2)", String.class))
            .from(EVENT_TYPES)
            .where(DSL.field("split_part(code, ':', 1)", String.class).in(applications))
            .orderBy(1)
            .fetch()
            .map(r -> r.value1());
    }

    @Override
    public List<String> findDistinctAggregates(String application, String subdomain) {
        // Extract third segment of code for matching application and subdomain
        return dsl.selectDistinct(DSL.field("split_part(code, ':', 3)", String.class))
            .from(EVENT_TYPES)
            .where(DSL.field("split_part(code, ':', 1)", String.class).eq(application))
            .and(DSL.field("split_part(code, ':', 2)", String.class).eq(subdomain))
            .orderBy(1)
            .fetch()
            .map(r -> r.value1());
    }

    @Override
    public List<String> findAllDistinctAggregates() {
        return dsl.selectDistinct(DSL.field("split_part(code, ':', 3)", String.class))
            .from(EVENT_TYPES)
            .orderBy(1)
            .fetch()
            .map(r -> r.value1());
    }

    @Override
    public List<String> findDistinctAggregates(List<String> applications, List<String> subdomains) {
        Condition condition = DSL.noCondition();

        if (applications != null && !applications.isEmpty()) {
            condition = condition.and(
                DSL.field("split_part(code, ':', 1)", String.class).in(applications)
            );
        }
        if (subdomains != null && !subdomains.isEmpty()) {
            condition = condition.and(
                DSL.field("split_part(code, ':', 2)", String.class).in(subdomains)
            );
        }

        return dsl.selectDistinct(DSL.field("split_part(code, ':', 3)", String.class))
            .from(EVENT_TYPES)
            .where(condition)
            .orderBy(1)
            .fetch()
            .map(r -> r.value1());
    }

    @Override
    public List<EventType> findWithFilters(
        List<String> applications,
        List<String> subdomains,
        List<String> aggregates,
        EventTypeStatus status
    ) {
        Condition condition = DSL.noCondition();

        if (applications != null && !applications.isEmpty()) {
            condition = condition.and(
                DSL.field("split_part(code, ':', 1)", String.class).in(applications)
            );
        }
        if (subdomains != null && !subdomains.isEmpty()) {
            condition = condition.and(
                DSL.field("split_part(code, ':', 2)", String.class).in(subdomains)
            );
        }
        if (aggregates != null && !aggregates.isEmpty()) {
            condition = condition.and(
                DSL.field("split_part(code, ':', 3)", String.class).in(aggregates)
            );
        }
        if (status != null) {
            condition = condition.and(EVENT_TYPES.STATUS.eq(status.name()));
        }

        return dsl.selectFrom(EVENT_TYPES)
            .where(condition)
            .orderBy(EVENT_TYPES.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public void persist(EventType eventType) {
        EventTypesRecord record = toRecord(eventType);
        record.setCreatedAt(toOffsetDateTime(eventType.createdAt()));
        record.setUpdatedAt(toOffsetDateTime(eventType.updatedAt()));
        dsl.insertInto(EVENT_TYPES).set(record).execute();
    }

    @Override
    public void update(EventType eventType) {
        EventTypesRecord record = toRecord(eventType);
        record.setUpdatedAt(toOffsetDateTime(Instant.now()));
        dsl.update(EVENT_TYPES)
            .set(record)
            .where(EVENT_TYPES.ID.eq(eventType.id()))
            .execute();
    }

    @Override
    public void delete(EventType eventType) {
        deleteById(eventType.id());
    }

    @Override
    public boolean deleteById(String id) {
        return dsl.deleteFrom(EVENT_TYPES)
            .where(EVENT_TYPES.ID.eq(id))
            .execute() > 0;
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private EventType toDomain(Record record) {
        if (record == null) return null;

        String specVersionsJson = record.get(EVENT_TYPES.SPEC_VERSIONS);
        List<SpecVersion> specVersions = new ArrayList<>();
        if (specVersionsJson != null && !specVersionsJson.isBlank()) {
            specVersions = parseJson(specVersionsJson, new TypeReference<List<SpecVersion>>() {});
            if (specVersions == null) {
                specVersions = new ArrayList<>();
            }
        }

        return new EventType(
            record.get(EVENT_TYPES.ID),
            record.get(EVENT_TYPES.CODE),
            record.get(EVENT_TYPES.NAME),
            record.get(EVENT_TYPES.DESCRIPTION),
            specVersions,
            parseEnum(record.get(EVENT_TYPES.STATUS), EventTypeStatus.class),
            toInstant(record.get(EVENT_TYPES.CREATED_AT)),
            toInstant(record.get(EVENT_TYPES.UPDATED_AT))
        );
    }

    private EventTypesRecord toRecord(EventType e) {
        EventTypesRecord rec = new EventTypesRecord();
        rec.setId(e.id());
        rec.setCode(e.code());
        rec.setName(e.name());
        rec.setDescription(e.description());
        rec.setStatus(e.status() != null ? e.status().name() : EventTypeStatus.CURRENT.name());

        // SpecVersions -> JSONB array
        rec.setSpecVersions(toJson(e.specVersions() != null ? e.specVersions() : new ArrayList<>()));

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
