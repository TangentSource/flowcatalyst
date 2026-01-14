package tech.flowcatalyst.schema;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for Schema entity.
 */
@RegisterRowMapper(SchemaRowMapper.class)
public interface SchemaDao {

    @SqlQuery("SELECT * FROM schemas WHERE id = :id")
    Optional<Schema> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM schemas WHERE event_type_id = :eventTypeId AND version = :version")
    Optional<Schema> findByEventTypeAndVersion(@Bind("eventTypeId") String eventTypeId, @Bind("version") String version);

    @SqlQuery("SELECT * FROM schemas WHERE event_type_id = :eventTypeId ORDER BY version")
    List<Schema> findByEventType(@Bind("eventTypeId") String eventTypeId);

    @SqlQuery("SELECT * FROM schemas WHERE event_type_id IS NULL ORDER BY created_at DESC")
    List<Schema> findStandalone();

    @SqlQuery("SELECT * FROM schemas WHERE schema_type = :schemaType ORDER BY created_at DESC")
    List<Schema> findBySchemaType(@Bind("schemaType") String schemaType);

    @SqlQuery("SELECT * FROM schemas ORDER BY created_at DESC")
    List<Schema> listAll();

    @SqlQuery("SELECT COUNT(*) FROM schemas")
    long count();

    @SqlQuery("""
        SELECT EXISTS(
            SELECT 1 FROM schemas
            WHERE event_type_id = :eventTypeId AND version = :version
        )
        """)
    boolean existsByEventTypeAndVersion(@Bind("eventTypeId") String eventTypeId, @Bind("version") String version);

    @SqlUpdate("""
        INSERT INTO schemas (id, name, description, mime_type, schema_type, content,
                            event_type_id, version, created_at, updated_at)
        VALUES (:id, :name, :description, :mimeType, :schemaType, :content,
                :eventTypeId, :version, :createdAt, :updatedAt)
        """)
    void insert(@BindFields Schema schema);

    @SqlUpdate("""
        UPDATE schemas SET name = :name, description = :description, mime_type = :mimeType,
               schema_type = :schemaType, content = :content, event_type_id = :eventTypeId,
               version = :version, updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindFields Schema schema);

    @SqlUpdate("DELETE FROM schemas WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
