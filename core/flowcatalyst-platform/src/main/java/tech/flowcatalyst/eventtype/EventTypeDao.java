package tech.flowcatalyst.eventtype;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for EventType entity.
 */
@RegisterRowMapper(EventTypeRowMapper.class)
public interface EventTypeDao {

    @SqlQuery("SELECT * FROM event_types WHERE id = :id")
    Optional<EventType> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM event_types WHERE code = :code")
    Optional<EventType> findByCode(@Bind("code") String code);

    @SqlQuery("SELECT * FROM event_types ORDER BY code")
    List<EventType> listAll();

    @SqlQuery("SELECT * FROM event_types ORDER BY code")
    List<EventType> findAllOrdered();

    @SqlQuery("SELECT * FROM event_types WHERE status = 'CURRENT' ORDER BY code")
    List<EventType> findCurrent();

    @SqlQuery("SELECT * FROM event_types WHERE status = 'ARCHIVE' ORDER BY code")
    List<EventType> findArchived();

    @SqlQuery("SELECT * FROM event_types WHERE code LIKE :prefix || '%' ORDER BY code")
    List<EventType> findByCodePrefix(@Bind("prefix") String prefix);

    @SqlQuery("SELECT COUNT(*) FROM event_types")
    long count();

    @SqlQuery("SELECT EXISTS(SELECT 1 FROM event_types WHERE code = :code)")
    boolean existsByCode(@Bind("code") String code);

    // Aggregation queries - extract code segments
    @SqlQuery("SELECT DISTINCT split_part(code, ':', 1) as application FROM event_types ORDER BY application")
    List<String> findDistinctApplications();

    @SqlQuery("""
        SELECT DISTINCT split_part(code, ':', 2) as subdomain
        FROM event_types
        WHERE split_part(code, ':', 1) = :application
        ORDER BY subdomain
        """)
    List<String> findDistinctSubdomains(@Bind("application") String application);

    @SqlQuery("SELECT DISTINCT split_part(code, ':', 2) as subdomain FROM event_types ORDER BY subdomain")
    List<String> findAllDistinctSubdomains();

    @SqlQuery("""
        SELECT DISTINCT split_part(code, ':', 3) as aggregate
        FROM event_types
        WHERE split_part(code, ':', 1) = :application
        AND split_part(code, ':', 2) = :subdomain
        ORDER BY aggregate
        """)
    List<String> findDistinctAggregates(@Bind("application") String application, @Bind("subdomain") String subdomain);

    @SqlQuery("SELECT DISTINCT split_part(code, ':', 3) as aggregate FROM event_types ORDER BY aggregate")
    List<String> findAllDistinctAggregates();

    @SqlUpdate("""
        INSERT INTO event_types (id, code, name, description, spec_versions, status, created_at, updated_at)
        VALUES (:id, :code, :name, :description, :specVersions::jsonb, :status, :createdAt, :updatedAt)
        """)
    void insert(@BindFields EventType eventType, @Bind("specVersions") String specVersionsJson);

    @SqlUpdate("""
        UPDATE event_types SET code = :code, name = :name, description = :description,
               spec_versions = :specVersions::jsonb, status = :status, updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindFields EventType eventType, @Bind("specVersions") String specVersionsJson);

    @SqlUpdate("DELETE FROM event_types WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
