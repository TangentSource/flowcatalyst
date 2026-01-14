package tech.flowcatalyst.event.read;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for EventRead entity.
 */
@RegisterRowMapper(EventReadRowMapper.class)
public interface EventReadDao {

    @SqlQuery("SELECT * FROM events_read WHERE id = :id")
    Optional<EventRead> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM events_read ORDER BY time DESC")
    List<EventRead> listAll();

    @SqlQuery("SELECT COUNT(*) FROM events_read")
    long count();

    @SqlQuery("SELECT DISTINCT application FROM events_read WHERE client_id = ANY(:clientIds::varchar[]) ORDER BY application")
    List<String> findDistinctApplicationsByClientIds(@Bind("clientIds") String[] clientIds);

    @SqlQuery("SELECT DISTINCT subdomain FROM events_read WHERE client_id = ANY(:clientIds::varchar[]) AND application = ANY(:applications::varchar[]) ORDER BY subdomain")
    List<String> findDistinctSubdomainsByClientIdsAndApplications(@Bind("clientIds") String[] clientIds, @Bind("applications") String[] applications);

    @SqlQuery("SELECT DISTINCT aggregate FROM events_read WHERE client_id = ANY(:clientIds::varchar[]) AND application = ANY(:applications::varchar[]) AND subdomain = ANY(:subdomains::varchar[]) ORDER BY aggregate")
    List<String> findDistinctAggregatesByClientIdsApplicationsAndSubdomains(@Bind("clientIds") String[] clientIds, @Bind("applications") String[] applications, @Bind("subdomains") String[] subdomains);

    @SqlQuery("SELECT DISTINCT type FROM events_read WHERE client_id = ANY(:clientIds::varchar[]) AND application = ANY(:applications::varchar[]) AND subdomain = ANY(:subdomains::varchar[]) AND aggregate = ANY(:aggregates::varchar[]) ORDER BY type")
    List<String> findDistinctTypesByFilters(@Bind("clientIds") String[] clientIds, @Bind("applications") String[] applications, @Bind("subdomains") String[] subdomains, @Bind("aggregates") String[] aggregates);

    @SqlUpdate("""
        INSERT INTO events_read (id, event_id, spec_version, type, application, subdomain, aggregate,
                                 source, subject, time, data, message_group, correlation_id, causation_id,
                                 deduplication_id, context_data, client_id, projected_at)
        VALUES (:id, :eventId, :specVersion, :type, :application, :subdomain, :aggregate,
                :source, :subject, :time, :data, :messageGroup, :correlationId, :causationId,
                :deduplicationId, :contextData::jsonb, :clientId, :projectedAt)
        """)
    void insert(@BindFields EventRead event, @Bind("contextData") String contextDataJson);

    @SqlUpdate("""
        UPDATE events_read SET event_id = :eventId, spec_version = :specVersion, type = :type,
               application = :application, subdomain = :subdomain, aggregate = :aggregate,
               source = :source, subject = :subject, time = :time, data = :data,
               message_group = :messageGroup, correlation_id = :correlationId, causation_id = :causationId,
               deduplication_id = :deduplicationId, context_data = :contextData::jsonb, client_id = :clientId,
               projected_at = :projectedAt
        WHERE id = :id
        """)
    void update(@BindFields EventRead event, @Bind("contextData") String contextDataJson);

    @SqlUpdate("DELETE FROM events_read WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
