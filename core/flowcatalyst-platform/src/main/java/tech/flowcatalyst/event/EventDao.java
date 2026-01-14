package tech.flowcatalyst.event;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for Event entity.
 */
@RegisterRowMapper(EventRowMapper.class)
public interface EventDao {

    @SqlQuery("SELECT * FROM events WHERE id = :id")
    Optional<Event> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM events WHERE deduplication_id = :deduplicationId")
    Optional<Event> findByDeduplicationId(@Bind("deduplicationId") String deduplicationId);

    @SqlQuery("SELECT * FROM events ORDER BY created_at DESC")
    List<Event> listAll();

    @SqlQuery("SELECT * FROM events ORDER BY created_at DESC LIMIT :size OFFSET :offset")
    List<Event> findRecentPaged(@Bind("offset") int offset, @Bind("size") int size);

    @SqlQuery("SELECT COUNT(*) FROM events")
    long count();

    @SqlQuery("SELECT EXISTS(SELECT 1 FROM events WHERE deduplication_id = :deduplicationId)")
    boolean existsByDeduplicationId(@Bind("deduplicationId") String deduplicationId);

    @SqlUpdate("""
        INSERT INTO events (id, type, source, subject, time, data, correlation_id, causation_id,
                           deduplication_id, message_group, client_id, context_data, created_at)
        VALUES (:id, :type, :source, :subject, :time, :data::jsonb, :correlationId, :causationId,
                :deduplicationId, :messageGroup, :clientId, :contextData::jsonb, NOW())
        """)
    void insert(@BindFields Event event, @Bind("contextData") String contextDataJson);

    @SqlUpdate("""
        UPDATE events SET type = :type, source = :source, subject = :subject, time = :time,
               data = :data::jsonb, correlation_id = :correlationId, causation_id = :causationId,
               deduplication_id = :deduplicationId, message_group = :messageGroup, client_id = :clientId,
               context_data = :contextData::jsonb
        WHERE id = :id
        """)
    void update(@BindFields Event event, @Bind("contextData") String contextDataJson);

    @SqlUpdate("DELETE FROM events WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
