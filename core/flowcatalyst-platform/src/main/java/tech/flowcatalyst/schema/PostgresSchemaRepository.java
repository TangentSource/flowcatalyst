package tech.flowcatalyst.schema;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import tech.flowcatalyst.eventtype.SchemaType;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of SchemaRepository using JDBI.
 */
@ApplicationScoped
@Typed(SchemaRepository.class)
class PostgresSchemaRepository implements SchemaRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Schema findById(String id) {
        return jdbi.withExtension(SchemaDao.class, dao -> dao.findById(id).orElse(null));
    }

    @Override
    public Optional<Schema> findByIdOptional(String id) {
        return jdbi.withExtension(SchemaDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<Schema> findByEventTypeAndVersion(String eventTypeId, String version) {
        return jdbi.withExtension(SchemaDao.class, dao -> dao.findByEventTypeAndVersion(eventTypeId, version));
    }

    @Override
    public List<Schema> findByEventType(String eventTypeId) {
        return jdbi.withExtension(SchemaDao.class, dao -> dao.findByEventType(eventTypeId));
    }

    @Override
    public List<Schema> findStandalone() {
        return jdbi.withExtension(SchemaDao.class, SchemaDao::findStandalone);
    }

    @Override
    public List<Schema> findBySchemaType(SchemaType schemaType) {
        return jdbi.withExtension(SchemaDao.class, dao -> dao.findBySchemaType(schemaType.name()));
    }

    @Override
    public List<Schema> listAll() {
        return jdbi.withExtension(SchemaDao.class, SchemaDao::listAll);
    }

    @Override
    public long count() {
        return jdbi.withExtension(SchemaDao.class, SchemaDao::count);
    }

    @Override
    public boolean existsByEventTypeAndVersion(String eventTypeId, String version) {
        return jdbi.withExtension(SchemaDao.class, dao -> dao.existsByEventTypeAndVersion(eventTypeId, version));
    }

    @Override
    public void persist(Schema schema) {
        jdbi.useExtension(SchemaDao.class, dao -> dao.insert(schema));
    }

    @Override
    public void update(Schema schema) {
        Schema updated = schema.toBuilder()
            .updatedAt(Instant.now())
            .build();
        jdbi.useExtension(SchemaDao.class, dao -> dao.update(updated));
    }

    @Override
    public void delete(Schema schema) {
        jdbi.useExtension(SchemaDao.class, dao -> dao.deleteById(schema.id()));
    }

    @Override
    public boolean deleteById(String id) {
        return jdbi.withExtension(SchemaDao.class, dao -> dao.deleteById(id) > 0);
    }
}
