package tech.flowcatalyst.event.read;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of EventReadRepository using JDBI.
 */
@ApplicationScoped
@Typed(EventReadRepository.class)
class PostgresEventReadRepository implements EventReadRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public EventRead findById(String id) {
        return jdbi.withExtension(EventReadDao.class, dao -> dao.findById(id).orElse(null));
    }

    @Override
    public Optional<EventRead> findByIdOptional(String id) {
        return jdbi.withExtension(EventReadDao.class, dao -> dao.findById(id));
    }

    @Override
    public List<EventRead> findWithFilter(EventFilter filter) {
        return jdbi.withHandle(handle -> {
            StringBuilder sql = new StringBuilder("SELECT * FROM events_read WHERE 1=1");
            List<Object> params = new ArrayList<>();

            if (filter.clientIds() != null && !filter.clientIds().isEmpty()) {
                sql.append(" AND client_id = ANY(?)");
                params.add(filter.clientIds().toArray(new String[0]));
            }

            if (filter.applications() != null && !filter.applications().isEmpty()) {
                sql.append(" AND application = ANY(?)");
                params.add(filter.applications().toArray(new String[0]));
            }

            if (filter.subdomains() != null && !filter.subdomains().isEmpty()) {
                sql.append(" AND subdomain = ANY(?)");
                params.add(filter.subdomains().toArray(new String[0]));
            }

            if (filter.aggregates() != null && !filter.aggregates().isEmpty()) {
                sql.append(" AND aggregate = ANY(?)");
                params.add(filter.aggregates().toArray(new String[0]));
            }

            if (filter.types() != null && !filter.types().isEmpty()) {
                sql.append(" AND type = ANY(?)");
                params.add(filter.types().toArray(new String[0]));
            }

            if (filter.source() != null && !filter.source().isBlank()) {
                sql.append(" AND source LIKE ?");
                params.add("%" + filter.source() + "%");
            }

            if (filter.subject() != null && !filter.subject().isBlank()) {
                sql.append(" AND subject LIKE ?");
                params.add("%" + filter.subject() + "%");
            }

            if (filter.correlationId() != null && !filter.correlationId().isBlank()) {
                sql.append(" AND correlation_id = ?");
                params.add(filter.correlationId());
            }

            if (filter.messageGroup() != null && !filter.messageGroup().isBlank()) {
                sql.append(" AND message_group = ?");
                params.add(filter.messageGroup());
            }

            if (filter.timeAfter() != null) {
                sql.append(" AND time >= ?");
                params.add(filter.timeAfter());
            }

            if (filter.timeBefore() != null) {
                sql.append(" AND time <= ?");
                params.add(filter.timeBefore());
            }

            sql.append(" ORDER BY time DESC");
            sql.append(" LIMIT ? OFFSET ?");
            params.add(filter.size());
            params.add(filter.page() * filter.size());

            var query = handle.createQuery(sql.toString());
            for (int i = 0; i < params.size(); i++) {
                query.bind(i, params.get(i));
            }

            return query.registerRowMapper(new EventReadRowMapper())
                .mapTo(EventRead.class)
                .list();
        });
    }

    @Override
    public List<EventRead> listAll() {
        return jdbi.withExtension(EventReadDao.class, EventReadDao::listAll);
    }

    @Override
    public long count() {
        return jdbi.withExtension(EventReadDao.class, EventReadDao::count);
    }

    @Override
    public long countWithFilter(EventFilter filter) {
        return jdbi.withHandle(handle -> {
            StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM events_read WHERE 1=1");
            List<Object> params = new ArrayList<>();

            if (filter.clientIds() != null && !filter.clientIds().isEmpty()) {
                sql.append(" AND client_id = ANY(?)");
                params.add(filter.clientIds().toArray(new String[0]));
            }

            if (filter.applications() != null && !filter.applications().isEmpty()) {
                sql.append(" AND application = ANY(?)");
                params.add(filter.applications().toArray(new String[0]));
            }

            if (filter.subdomains() != null && !filter.subdomains().isEmpty()) {
                sql.append(" AND subdomain = ANY(?)");
                params.add(filter.subdomains().toArray(new String[0]));
            }

            if (filter.aggregates() != null && !filter.aggregates().isEmpty()) {
                sql.append(" AND aggregate = ANY(?)");
                params.add(filter.aggregates().toArray(new String[0]));
            }

            if (filter.types() != null && !filter.types().isEmpty()) {
                sql.append(" AND type = ANY(?)");
                params.add(filter.types().toArray(new String[0]));
            }

            if (filter.source() != null && !filter.source().isBlank()) {
                sql.append(" AND source LIKE ?");
                params.add("%" + filter.source() + "%");
            }

            if (filter.subject() != null && !filter.subject().isBlank()) {
                sql.append(" AND subject LIKE ?");
                params.add("%" + filter.subject() + "%");
            }

            if (filter.correlationId() != null && !filter.correlationId().isBlank()) {
                sql.append(" AND correlation_id = ?");
                params.add(filter.correlationId());
            }

            if (filter.messageGroup() != null && !filter.messageGroup().isBlank()) {
                sql.append(" AND message_group = ?");
                params.add(filter.messageGroup());
            }

            if (filter.timeAfter() != null) {
                sql.append(" AND time >= ?");
                params.add(filter.timeAfter());
            }

            if (filter.timeBefore() != null) {
                sql.append(" AND time <= ?");
                params.add(filter.timeBefore());
            }

            var query = handle.createQuery(sql.toString());
            for (int i = 0; i < params.size(); i++) {
                query.bind(i, params.get(i));
            }

            return query.mapTo(Long.class).one();
        });
    }

    @Override
    public FilterOptions getFilterOptions(FilterOptionsRequest request) {
        return jdbi.withHandle(handle -> {
            String[] clientIds = request.clientIds() != null ? request.clientIds().toArray(new String[0]) : new String[0];
            String[] applications = request.applications() != null ? request.applications().toArray(new String[0]) : new String[0];
            String[] subdomains = request.subdomains() != null ? request.subdomains().toArray(new String[0]) : new String[0];
            String[] aggregates = request.aggregates() != null ? request.aggregates().toArray(new String[0]) : new String[0];

            // Get distinct clients
            List<String> clients = handle.createQuery("SELECT DISTINCT client_id FROM events_read WHERE client_id IS NOT NULL ORDER BY client_id")
                .mapTo(String.class)
                .list();

            // Get applications filtered by clientIds
            List<String> apps;
            if (clientIds.length > 0) {
                apps = handle.createQuery("SELECT DISTINCT application FROM events_read WHERE client_id = ANY(:clientIds::varchar[]) ORDER BY application")
                    .bind("clientIds", clientIds)
                    .mapTo(String.class)
                    .list();
            } else {
                apps = handle.createQuery("SELECT DISTINCT application FROM events_read WHERE application IS NOT NULL ORDER BY application")
                    .mapTo(String.class)
                    .list();
            }

            // Get subdomains filtered by clientIds and applications
            List<String> subs;
            if (clientIds.length > 0 && applications.length > 0) {
                subs = handle.createQuery("SELECT DISTINCT subdomain FROM events_read WHERE client_id = ANY(:clientIds::varchar[]) AND application = ANY(:applications::varchar[]) ORDER BY subdomain")
                    .bind("clientIds", clientIds)
                    .bind("applications", applications)
                    .mapTo(String.class)
                    .list();
            } else if (clientIds.length > 0) {
                subs = handle.createQuery("SELECT DISTINCT subdomain FROM events_read WHERE client_id = ANY(:clientIds::varchar[]) ORDER BY subdomain")
                    .bind("clientIds", clientIds)
                    .mapTo(String.class)
                    .list();
            } else {
                subs = handle.createQuery("SELECT DISTINCT subdomain FROM events_read WHERE subdomain IS NOT NULL ORDER BY subdomain")
                    .mapTo(String.class)
                    .list();
            }

            // Get aggregates filtered by clientIds, applications, and subdomains
            List<String> aggs;
            if (clientIds.length > 0 && applications.length > 0 && subdomains.length > 0) {
                aggs = handle.createQuery("SELECT DISTINCT aggregate FROM events_read WHERE client_id = ANY(:clientIds::varchar[]) AND application = ANY(:applications::varchar[]) AND subdomain = ANY(:subdomains::varchar[]) ORDER BY aggregate")
                    .bind("clientIds", clientIds)
                    .bind("applications", applications)
                    .bind("subdomains", subdomains)
                    .mapTo(String.class)
                    .list();
            } else if (clientIds.length > 0 && applications.length > 0) {
                aggs = handle.createQuery("SELECT DISTINCT aggregate FROM events_read WHERE client_id = ANY(:clientIds::varchar[]) AND application = ANY(:applications::varchar[]) ORDER BY aggregate")
                    .bind("clientIds", clientIds)
                    .bind("applications", applications)
                    .mapTo(String.class)
                    .list();
            } else {
                aggs = handle.createQuery("SELECT DISTINCT aggregate FROM events_read WHERE aggregate IS NOT NULL ORDER BY aggregate")
                    .mapTo(String.class)
                    .list();
            }

            // Get types filtered by all criteria
            List<String> types;
            if (clientIds.length > 0 && applications.length > 0 && subdomains.length > 0 && aggregates.length > 0) {
                types = handle.createQuery("SELECT DISTINCT type FROM events_read WHERE client_id = ANY(:clientIds::varchar[]) AND application = ANY(:applications::varchar[]) AND subdomain = ANY(:subdomains::varchar[]) AND aggregate = ANY(:aggregates::varchar[]) ORDER BY type")
                    .bind("clientIds", clientIds)
                    .bind("applications", applications)
                    .bind("subdomains", subdomains)
                    .bind("aggregates", aggregates)
                    .mapTo(String.class)
                    .list();
            } else {
                types = handle.createQuery("SELECT DISTINCT type FROM events_read WHERE type IS NOT NULL ORDER BY type")
                    .mapTo(String.class)
                    .list();
            }

            return new FilterOptions(clients, apps, subs, aggs, types);
        });
    }

    @Override
    public void persist(EventRead event) {
        if (event.projectedAt == null) {
            event.projectedAt = Instant.now();
        }
        jdbi.useExtension(EventReadDao.class, dao ->
            dao.insert(event, JsonHelper.toJson(event.contextData)));
    }

    @Override
    public void update(EventRead event) {
        jdbi.useExtension(EventReadDao.class, dao ->
            dao.update(event, JsonHelper.toJson(event.contextData)));
    }

    @Override
    public void delete(EventRead event) {
        jdbi.useExtension(EventReadDao.class, dao -> dao.deleteById(event.id));
    }

    @Override
    public boolean deleteById(String id) {
        return jdbi.withExtension(EventReadDao.class, dao -> dao.deleteById(id) > 0);
    }
}
