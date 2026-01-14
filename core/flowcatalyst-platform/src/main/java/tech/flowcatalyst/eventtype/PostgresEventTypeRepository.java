package tech.flowcatalyst.eventtype;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of EventTypeRepository using JDBI.
 */
@ApplicationScoped
@Typed(EventTypeRepository.class)
class PostgresEventTypeRepository implements EventTypeRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public EventType findById(String id) {
        return jdbi.withExtension(EventTypeDao.class, dao -> dao.findById(id).orElse(null));
    }

    @Override
    public Optional<EventType> findByIdOptional(String id) {
        return jdbi.withExtension(EventTypeDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<EventType> findByCode(String code) {
        return jdbi.withExtension(EventTypeDao.class, dao -> dao.findByCode(code));
    }

    @Override
    public List<EventType> findAllOrdered() {
        return jdbi.withExtension(EventTypeDao.class, EventTypeDao::findAllOrdered);
    }

    @Override
    public List<EventType> findCurrent() {
        return jdbi.withExtension(EventTypeDao.class, EventTypeDao::findCurrent);
    }

    @Override
    public List<EventType> findArchived() {
        return jdbi.withExtension(EventTypeDao.class, EventTypeDao::findArchived);
    }

    @Override
    public List<EventType> findByCodePrefix(String prefix) {
        return jdbi.withExtension(EventTypeDao.class, dao -> dao.findByCodePrefix(prefix));
    }

    @Override
    public List<EventType> listAll() {
        return jdbi.withExtension(EventTypeDao.class, EventTypeDao::listAll);
    }

    @Override
    public long count() {
        return jdbi.withExtension(EventTypeDao.class, EventTypeDao::count);
    }

    @Override
    public boolean existsByCode(String code) {
        return jdbi.withExtension(EventTypeDao.class, dao -> dao.existsByCode(code));
    }

    @Override
    public List<String> findDistinctApplications() {
        return jdbi.withExtension(EventTypeDao.class, EventTypeDao::findDistinctApplications);
    }

    @Override
    public List<String> findDistinctSubdomains(String application) {
        return jdbi.withExtension(EventTypeDao.class, dao -> dao.findDistinctSubdomains(application));
    }

    @Override
    public List<String> findAllDistinctSubdomains() {
        return jdbi.withExtension(EventTypeDao.class, EventTypeDao::findAllDistinctSubdomains);
    }

    @Override
    public List<String> findDistinctSubdomains(List<String> applications) {
        if (applications == null || applications.isEmpty()) {
            return findAllDistinctSubdomains();
        }
        return jdbi.withHandle(handle ->
            handle.createQuery("""
                SELECT DISTINCT split_part(code, ':', 2) as subdomain
                FROM event_types
                WHERE split_part(code, ':', 1) = ANY(:applications)
                ORDER BY subdomain
                """)
                .bindArray("applications", String.class, applications.toArray(new String[0]))
                .mapTo(String.class)
                .list()
        );
    }

    @Override
    public List<String> findDistinctAggregates(String application, String subdomain) {
        return jdbi.withExtension(EventTypeDao.class, dao -> dao.findDistinctAggregates(application, subdomain));
    }

    @Override
    public List<String> findAllDistinctAggregates() {
        return jdbi.withExtension(EventTypeDao.class, EventTypeDao::findAllDistinctAggregates);
    }

    @Override
    public List<String> findDistinctAggregates(List<String> applications, List<String> subdomains) {
        if ((applications == null || applications.isEmpty()) && (subdomains == null || subdomains.isEmpty())) {
            return findAllDistinctAggregates();
        }

        StringBuilder sql = new StringBuilder("""
            SELECT DISTINCT split_part(code, ':', 3) as aggregate
            FROM event_types
            WHERE 1=1
            """);

        if (applications != null && !applications.isEmpty()) {
            sql.append(" AND split_part(code, ':', 1) = ANY(:applications)");
        }
        if (subdomains != null && !subdomains.isEmpty()) {
            sql.append(" AND split_part(code, ':', 2) = ANY(:subdomains)");
        }
        sql.append(" ORDER BY aggregate");

        String finalSql = sql.toString();
        return jdbi.withHandle(handle -> {
            var query = handle.createQuery(finalSql);
            if (applications != null && !applications.isEmpty()) {
                query.bindArray("applications", String.class, applications.toArray(new String[0]));
            }
            if (subdomains != null && !subdomains.isEmpty()) {
                query.bindArray("subdomains", String.class, subdomains.toArray(new String[0]));
            }
            return query.mapTo(String.class).list();
        });
    }

    @Override
    public List<EventType> findWithFilters(
        List<String> applications,
        List<String> subdomains,
        List<String> aggregates,
        EventTypeStatus status
    ) {
        StringBuilder sql = new StringBuilder("SELECT * FROM event_types WHERE 1=1");

        if (applications != null && !applications.isEmpty()) {
            sql.append(" AND split_part(code, ':', 1) = ANY(:applications)");
        }
        if (subdomains != null && !subdomains.isEmpty()) {
            sql.append(" AND split_part(code, ':', 2) = ANY(:subdomains)");
        }
        if (aggregates != null && !aggregates.isEmpty()) {
            sql.append(" AND split_part(code, ':', 3) = ANY(:aggregates)");
        }
        if (status != null) {
            sql.append(" AND status = :status");
        }
        sql.append(" ORDER BY code");

        String finalSql = sql.toString();
        return jdbi.withHandle(handle -> {
            var query = handle.createQuery(finalSql)
                .registerRowMapper(new EventTypeRowMapper());

            if (applications != null && !applications.isEmpty()) {
                query.bindArray("applications", String.class, applications.toArray(new String[0]));
            }
            if (subdomains != null && !subdomains.isEmpty()) {
                query.bindArray("subdomains", String.class, subdomains.toArray(new String[0]));
            }
            if (aggregates != null && !aggregates.isEmpty()) {
                query.bindArray("aggregates", String.class, aggregates.toArray(new String[0]));
            }
            if (status != null) {
                query.bind("status", status.name());
            }

            return query.mapTo(EventType.class).list();
        });
    }

    @Override
    public void persist(EventType eventType) {
        jdbi.useExtension(EventTypeDao.class, dao ->
            dao.insert(eventType, JsonHelper.toJsonArray(eventType.specVersions())));
    }

    @Override
    public void update(EventType eventType) {
        EventType updated = eventType.toBuilder()
            .updatedAt(Instant.now())
            .build();
        jdbi.useExtension(EventTypeDao.class, dao ->
            dao.update(updated, JsonHelper.toJsonArray(updated.specVersions())));
    }

    @Override
    public void delete(EventType eventType) {
        jdbi.useExtension(EventTypeDao.class, dao -> dao.deleteById(eventType.id()));
    }

    @Override
    public boolean deleteById(String id) {
        return jdbi.withExtension(EventTypeDao.class, dao -> dao.deleteById(id) > 0);
    }
}
