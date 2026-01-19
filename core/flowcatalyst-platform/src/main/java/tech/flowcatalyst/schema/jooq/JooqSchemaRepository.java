package tech.flowcatalyst.schema.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.eventtype.SchemaType;
import tech.flowcatalyst.schema.Schema;
import tech.flowcatalyst.schema.SchemaRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.SchemasRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.Schemas.SCHEMAS;

/**
 * JOOQ-based implementation of SchemaRepository.
 */
@ApplicationScoped
public class JooqSchemaRepository implements SchemaRepository {

    @Inject
    DSLContext dsl;

    @Override
    public Schema findById(String id) {
        return dsl.selectFrom(SCHEMAS)
            .where(SCHEMAS.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public Optional<Schema> findByIdOptional(String id) {
        return Optional.ofNullable(findById(id));
    }

    @Override
    public Optional<Schema> findByEventTypeAndVersion(String eventTypeId, String version) {
        return Optional.ofNullable(
            dsl.selectFrom(SCHEMAS)
                .where(SCHEMAS.EVENT_TYPE_ID.eq(eventTypeId))
                .and(SCHEMAS.VERSION.eq(version))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<Schema> findByEventType(String eventTypeId) {
        return dsl.selectFrom(SCHEMAS)
            .where(SCHEMAS.EVENT_TYPE_ID.eq(eventTypeId))
            .orderBy(SCHEMAS.VERSION)
            .fetch(this::toDomain);
    }

    @Override
    public List<Schema> findStandalone() {
        return dsl.selectFrom(SCHEMAS)
            .where(SCHEMAS.EVENT_TYPE_ID.isNull())
            .orderBy(SCHEMAS.NAME)
            .fetch(this::toDomain);
    }

    @Override
    public List<Schema> findBySchemaType(SchemaType schemaType) {
        return dsl.selectFrom(SCHEMAS)
            .where(SCHEMAS.SCHEMA_TYPE.eq(schemaType.name()))
            .orderBy(SCHEMAS.NAME)
            .fetch(this::toDomain);
    }

    @Override
    public List<Schema> listAll() {
        return dsl.selectFrom(SCHEMAS)
            .fetch(this::toDomain);
    }

    @Override
    public long count() {
        return dsl.selectCount()
            .from(SCHEMAS)
            .fetchOne(0, Long.class);
    }

    @Override
    public boolean existsByEventTypeAndVersion(String eventTypeId, String version) {
        return dsl.fetchExists(
            dsl.selectFrom(SCHEMAS)
                .where(SCHEMAS.EVENT_TYPE_ID.eq(eventTypeId))
                .and(SCHEMAS.VERSION.eq(version))
        );
    }

    @Override
    public void persist(Schema schema) {
        SchemasRecord record = toRecord(schema);
        record.setCreatedAt(toOffsetDateTime(schema.createdAt()));
        record.setUpdatedAt(toOffsetDateTime(schema.updatedAt()));
        dsl.insertInto(SCHEMAS).set(record).execute();
    }

    @Override
    public void update(Schema schema) {
        SchemasRecord record = toRecord(schema);
        record.setUpdatedAt(toOffsetDateTime(Instant.now()));
        dsl.update(SCHEMAS)
            .set(record)
            .where(SCHEMAS.ID.eq(schema.id()))
            .execute();
    }

    @Override
    public void delete(Schema schema) {
        deleteById(schema.id());
    }

    @Override
    public boolean deleteById(String id) {
        return dsl.deleteFrom(SCHEMAS)
            .where(SCHEMAS.ID.eq(id))
            .execute() > 0;
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private Schema toDomain(Record record) {
        if (record == null) return null;

        return new Schema(
            record.get(SCHEMAS.ID),
            record.get(SCHEMAS.NAME),
            record.get(SCHEMAS.DESCRIPTION),
            record.get(SCHEMAS.MIME_TYPE),
            parseEnum(record.get(SCHEMAS.SCHEMA_TYPE), SchemaType.class),
            record.get(SCHEMAS.CONTENT),
            record.get(SCHEMAS.EVENT_TYPE_ID),
            record.get(SCHEMAS.VERSION),
            toInstant(record.get(SCHEMAS.CREATED_AT)),
            toInstant(record.get(SCHEMAS.UPDATED_AT))
        );
    }

    private SchemasRecord toRecord(Schema s) {
        SchemasRecord rec = new SchemasRecord();
        rec.setId(s.id());
        rec.setName(s.name());
        rec.setDescription(s.description());
        rec.setMimeType(s.mimeType());
        rec.setSchemaType(s.schemaType() != null ? s.schemaType().name() : null);
        rec.setContent(s.content());
        rec.setEventTypeId(s.eventTypeId());
        rec.setVersion(s.version());
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
}
