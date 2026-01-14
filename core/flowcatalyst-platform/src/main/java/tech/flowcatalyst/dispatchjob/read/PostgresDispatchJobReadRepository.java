package tech.flowcatalyst.dispatchjob.read;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of DispatchJobReadRepository using JDBI.
 */
@ApplicationScoped
@Typed(DispatchJobReadRepository.class)
class PostgresDispatchJobReadRepository implements DispatchJobReadRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public DispatchJobRead findById(String id) {
        return jdbi.withExtension(DispatchJobReadDao.class, dao -> dao.findById(id).orElse(null));
    }

    @Override
    public Optional<DispatchJobRead> findByIdOptional(String id) {
        return jdbi.withExtension(DispatchJobReadDao.class, dao -> dao.findById(id));
    }

    @Override
    public List<DispatchJobRead> findWithFilter(DispatchJobReadFilter filter) {
        StringBuilder sql = new StringBuilder("SELECT * FROM dispatch_jobs_read WHERE 1=1");

        if (filter.clientIds() != null && !filter.clientIds().isEmpty()) {
            sql.append(" AND client_id = ANY(:clientIds)");
        }
        if (filter.statuses() != null && !filter.statuses().isEmpty()) {
            sql.append(" AND status = ANY(:statuses)");
        }
        if (filter.applications() != null && !filter.applications().isEmpty()) {
            sql.append(" AND application = ANY(:applications)");
        }
        if (filter.subdomains() != null && !filter.subdomains().isEmpty()) {
            sql.append(" AND subdomain = ANY(:subdomains)");
        }
        if (filter.aggregates() != null && !filter.aggregates().isEmpty()) {
            sql.append(" AND aggregate = ANY(:aggregates)");
        }
        if (filter.codes() != null && !filter.codes().isEmpty()) {
            sql.append(" AND code = ANY(:codes)");
        }
        if (filter.source() != null) {
            sql.append(" AND source = :source");
        }
        if (filter.kind() != null) {
            sql.append(" AND kind = :kind");
        }
        if (filter.subscriptionId() != null) {
            sql.append(" AND subscription_id = :subscriptionId");
        }
        if (filter.dispatchPoolId() != null) {
            sql.append(" AND dispatch_pool_id = :dispatchPoolId");
        }
        if (filter.messageGroup() != null) {
            sql.append(" AND message_group = :messageGroup");
        }
        if (filter.createdAfter() != null) {
            sql.append(" AND created_at >= :createdAfter");
        }
        if (filter.createdBefore() != null) {
            sql.append(" AND created_at <= :createdBefore");
        }

        sql.append(" ORDER BY created_at DESC");
        sql.append(" LIMIT :size OFFSET :offset");

        int offset = filter.page() * filter.size();
        String finalSql = sql.toString();

        return jdbi.withHandle(handle -> {
            var query = handle.createQuery(finalSql)
                .registerRowMapper(new DispatchJobReadRowMapper());

            if (filter.clientIds() != null && !filter.clientIds().isEmpty()) {
                query.bindArray("clientIds", String.class, filter.clientIds().toArray(new String[0]));
            }
            if (filter.statuses() != null && !filter.statuses().isEmpty()) {
                query.bindArray("statuses", String.class, filter.statuses().toArray(new String[0]));
            }
            if (filter.applications() != null && !filter.applications().isEmpty()) {
                query.bindArray("applications", String.class, filter.applications().toArray(new String[0]));
            }
            if (filter.subdomains() != null && !filter.subdomains().isEmpty()) {
                query.bindArray("subdomains", String.class, filter.subdomains().toArray(new String[0]));
            }
            if (filter.aggregates() != null && !filter.aggregates().isEmpty()) {
                query.bindArray("aggregates", String.class, filter.aggregates().toArray(new String[0]));
            }
            if (filter.codes() != null && !filter.codes().isEmpty()) {
                query.bindArray("codes", String.class, filter.codes().toArray(new String[0]));
            }
            if (filter.source() != null) {
                query.bind("source", filter.source());
            }
            if (filter.kind() != null) {
                query.bind("kind", filter.kind());
            }
            if (filter.subscriptionId() != null) {
                query.bind("subscriptionId", filter.subscriptionId());
            }
            if (filter.dispatchPoolId() != null) {
                query.bind("dispatchPoolId", filter.dispatchPoolId());
            }
            if (filter.messageGroup() != null) {
                query.bind("messageGroup", filter.messageGroup());
            }
            if (filter.createdAfter() != null) {
                query.bind("createdAfter", filter.createdAfter());
            }
            if (filter.createdBefore() != null) {
                query.bind("createdBefore", filter.createdBefore());
            }

            query.bind("size", filter.size());
            query.bind("offset", offset);

            return query.mapTo(DispatchJobRead.class).list();
        });
    }

    @Override
    public List<DispatchJobRead> listAll() {
        return jdbi.withExtension(DispatchJobReadDao.class, DispatchJobReadDao::listAll);
    }

    @Override
    public long count() {
        return jdbi.withExtension(DispatchJobReadDao.class, DispatchJobReadDao::count);
    }

    @Override
    public long countWithFilter(DispatchJobReadFilter filter) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM dispatch_jobs_read WHERE 1=1");

        if (filter.clientIds() != null && !filter.clientIds().isEmpty()) {
            sql.append(" AND client_id = ANY(:clientIds)");
        }
        if (filter.statuses() != null && !filter.statuses().isEmpty()) {
            sql.append(" AND status = ANY(:statuses)");
        }
        if (filter.applications() != null && !filter.applications().isEmpty()) {
            sql.append(" AND application = ANY(:applications)");
        }
        if (filter.subdomains() != null && !filter.subdomains().isEmpty()) {
            sql.append(" AND subdomain = ANY(:subdomains)");
        }
        if (filter.aggregates() != null && !filter.aggregates().isEmpty()) {
            sql.append(" AND aggregate = ANY(:aggregates)");
        }
        if (filter.codes() != null && !filter.codes().isEmpty()) {
            sql.append(" AND code = ANY(:codes)");
        }
        if (filter.source() != null) {
            sql.append(" AND source = :source");
        }
        if (filter.kind() != null) {
            sql.append(" AND kind = :kind");
        }
        if (filter.subscriptionId() != null) {
            sql.append(" AND subscription_id = :subscriptionId");
        }
        if (filter.dispatchPoolId() != null) {
            sql.append(" AND dispatch_pool_id = :dispatchPoolId");
        }
        if (filter.messageGroup() != null) {
            sql.append(" AND message_group = :messageGroup");
        }
        if (filter.createdAfter() != null) {
            sql.append(" AND created_at >= :createdAfter");
        }
        if (filter.createdBefore() != null) {
            sql.append(" AND created_at <= :createdBefore");
        }

        String finalSql = sql.toString();
        return jdbi.withHandle(handle -> {
            var query = handle.createQuery(finalSql);

            if (filter.clientIds() != null && !filter.clientIds().isEmpty()) {
                query.bindArray("clientIds", String.class, filter.clientIds().toArray(new String[0]));
            }
            if (filter.statuses() != null && !filter.statuses().isEmpty()) {
                query.bindArray("statuses", String.class, filter.statuses().toArray(new String[0]));
            }
            if (filter.applications() != null && !filter.applications().isEmpty()) {
                query.bindArray("applications", String.class, filter.applications().toArray(new String[0]));
            }
            if (filter.subdomains() != null && !filter.subdomains().isEmpty()) {
                query.bindArray("subdomains", String.class, filter.subdomains().toArray(new String[0]));
            }
            if (filter.aggregates() != null && !filter.aggregates().isEmpty()) {
                query.bindArray("aggregates", String.class, filter.aggregates().toArray(new String[0]));
            }
            if (filter.codes() != null && !filter.codes().isEmpty()) {
                query.bindArray("codes", String.class, filter.codes().toArray(new String[0]));
            }
            if (filter.source() != null) {
                query.bind("source", filter.source());
            }
            if (filter.kind() != null) {
                query.bind("kind", filter.kind());
            }
            if (filter.subscriptionId() != null) {
                query.bind("subscriptionId", filter.subscriptionId());
            }
            if (filter.dispatchPoolId() != null) {
                query.bind("dispatchPoolId", filter.dispatchPoolId());
            }
            if (filter.messageGroup() != null) {
                query.bind("messageGroup", filter.messageGroup());
            }
            if (filter.createdAfter() != null) {
                query.bind("createdAfter", filter.createdAfter());
            }
            if (filter.createdBefore() != null) {
                query.bind("createdBefore", filter.createdBefore());
            }

            return query.mapTo(Long.class).one();
        });
    }

    @Override
    public FilterOptions getFilterOptions(FilterOptionsRequest request) {
        // Build dynamic filter options based on current filter selections
        StringBuilder baseSql = new StringBuilder(" FROM dispatch_jobs_read WHERE 1=1");

        if (request.clientIds() != null && !request.clientIds().isEmpty()) {
            baseSql.append(" AND client_id = ANY(:clientIds)");
        }
        if (request.applications() != null && !request.applications().isEmpty()) {
            baseSql.append(" AND application = ANY(:applications)");
        }
        if (request.subdomains() != null && !request.subdomains().isEmpty()) {
            baseSql.append(" AND subdomain = ANY(:subdomains)");
        }
        if (request.aggregates() != null && !request.aggregates().isEmpty()) {
            baseSql.append(" AND aggregate = ANY(:aggregates)");
        }

        String baseCondition = baseSql.toString();

        return jdbi.withHandle(handle -> {
            // Get clients
            List<String> clients = executeDistinctQuery(handle, "client_id", baseCondition, request);

            // Get applications
            List<String> applications = executeDistinctQuery(handle, "application", baseCondition, request);

            // Get subdomains
            List<String> subdomains = executeDistinctQuery(handle, "subdomain", baseCondition, request);

            // Get aggregates
            List<String> aggregates = executeDistinctQuery(handle, "aggregate", baseCondition, request);

            // Get codes
            List<String> codes = executeDistinctQuery(handle, "code", baseCondition, request);

            // Get statuses
            List<String> statuses = executeDistinctQuery(handle, "status", baseCondition, request);

            return new FilterOptions(clients, applications, subdomains, aggregates, codes, statuses);
        });
    }

    private List<String> executeDistinctQuery(org.jdbi.v3.core.Handle handle, String column,
                                               String baseCondition, FilterOptionsRequest request) {
        String sql = "SELECT DISTINCT " + column + baseCondition +
            " AND " + column + " IS NOT NULL ORDER BY " + column;

        var query = handle.createQuery(sql);

        if (request.clientIds() != null && !request.clientIds().isEmpty()) {
            query.bindArray("clientIds", String.class, request.clientIds().toArray(new String[0]));
        }
        if (request.applications() != null && !request.applications().isEmpty()) {
            query.bindArray("applications", String.class, request.applications().toArray(new String[0]));
        }
        if (request.subdomains() != null && !request.subdomains().isEmpty()) {
            query.bindArray("subdomains", String.class, request.subdomains().toArray(new String[0]));
        }
        if (request.aggregates() != null && !request.aggregates().isEmpty()) {
            query.bindArray("aggregates", String.class, request.aggregates().toArray(new String[0]));
        }

        return query.mapTo(String.class).list();
    }

    @Override
    public void persist(DispatchJobRead job) {
        jdbi.useExtension(DispatchJobReadDao.class, dao -> dao.insert(job));
    }

    @Override
    public void update(DispatchJobRead job) {
        job.updatedAt = Instant.now();
        jdbi.useExtension(DispatchJobReadDao.class, dao -> dao.update(job));
    }

    @Override
    public void delete(DispatchJobRead job) {
        jdbi.useExtension(DispatchJobReadDao.class, dao -> dao.deleteById(job.id));
    }

    @Override
    public boolean deleteById(String id) {
        return jdbi.withExtension(DispatchJobReadDao.class, dao -> dao.deleteById(id) > 0);
    }
}
