package tech.flowcatalyst.dispatchpool;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of DispatchPoolRepository using JDBI.
 */
@ApplicationScoped
@Typed(DispatchPoolRepository.class)
class PostgresDispatchPoolRepository implements DispatchPoolRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public DispatchPool findById(String id) {
        return jdbi.withExtension(DispatchPoolDao.class, dao -> dao.findById(id).orElse(null));
    }

    @Override
    public Optional<DispatchPool> findByIdOptional(String id) {
        return jdbi.withExtension(DispatchPoolDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<DispatchPool> findByCodeAndClientId(String code, String clientId) {
        return jdbi.withExtension(DispatchPoolDao.class, dao -> dao.findByCodeAndClientId(code, clientId));
    }

    @Override
    public List<DispatchPool> findByClientId(String clientId) {
        return jdbi.withExtension(DispatchPoolDao.class, dao -> dao.findByClientId(clientId));
    }

    @Override
    public List<DispatchPool> findAnchorLevel() {
        return jdbi.withExtension(DispatchPoolDao.class, DispatchPoolDao::findAnchorLevel);
    }

    @Override
    public List<DispatchPool> findByStatus(DispatchPoolStatus status) {
        return jdbi.withExtension(DispatchPoolDao.class, dao -> dao.findByStatus(status.name()));
    }

    @Override
    public List<DispatchPool> findActive() {
        return jdbi.withExtension(DispatchPoolDao.class, DispatchPoolDao::findActive);
    }

    @Override
    public List<DispatchPool> findAllNonArchived() {
        return jdbi.withExtension(DispatchPoolDao.class, DispatchPoolDao::findAllNonArchived);
    }

    @Override
    public List<DispatchPool> findWithFilters(String clientId, DispatchPoolStatus status, boolean includeArchived) {
        StringBuilder sql = new StringBuilder("SELECT * FROM dispatch_pools WHERE 1=1");

        if (clientId != null) {
            sql.append(" AND client_id = :clientId");
        }
        if (status != null) {
            sql.append(" AND status = :status");
        }
        if (!includeArchived) {
            sql.append(" AND status != 'ARCHIVED'");
        }
        sql.append(" ORDER BY code");

        String finalSql = sql.toString();
        return jdbi.withHandle(handle -> {
            var query = handle.createQuery(finalSql)
                .registerRowMapper(new DispatchPoolRowMapper());

            if (clientId != null) {
                query.bind("clientId", clientId);
            }
            if (status != null) {
                query.bind("status", status.name());
            }

            return query.mapTo(DispatchPool.class).list();
        });
    }

    @Override
    public List<DispatchPool> listAll() {
        return jdbi.withExtension(DispatchPoolDao.class, DispatchPoolDao::listAll);
    }

    @Override
    public long count() {
        return jdbi.withExtension(DispatchPoolDao.class, DispatchPoolDao::count);
    }

    @Override
    public boolean existsByCodeAndClientId(String code, String clientId) {
        return jdbi.withExtension(DispatchPoolDao.class, dao -> dao.existsByCodeAndClientId(code, clientId));
    }

    @Override
    public void persist(DispatchPool pool) {
        jdbi.useExtension(DispatchPoolDao.class, dao -> dao.insert(pool));
    }

    @Override
    public void update(DispatchPool pool) {
        DispatchPool updated = pool.toBuilder()
            .updatedAt(Instant.now())
            .build();
        jdbi.useExtension(DispatchPoolDao.class, dao -> dao.update(updated));
    }

    @Override
    public void delete(DispatchPool pool) {
        jdbi.useExtension(DispatchPoolDao.class, dao -> dao.deleteById(pool.id()));
    }

    @Override
    public boolean deleteById(String id) {
        return jdbi.withExtension(DispatchPoolDao.class, dao -> dao.deleteById(id) > 0);
    }
}
