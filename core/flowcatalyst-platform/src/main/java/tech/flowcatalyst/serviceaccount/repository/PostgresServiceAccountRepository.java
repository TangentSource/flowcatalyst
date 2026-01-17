package tech.flowcatalyst.serviceaccount.repository;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;
import tech.flowcatalyst.serviceaccount.entity.ServiceAccount;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of ServiceAccountRepository using JDBI.
 */
@ApplicationScoped
@Typed(ServiceAccountRepository.class)
class PostgresServiceAccountRepository implements ServiceAccountRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public ServiceAccount findById(String id) {
        return findByIdOptional(id).orElse(null);
    }

    @Override
    public Optional<ServiceAccount> findByIdOptional(String id) {
        return jdbi.withExtension(ServiceAccountDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<ServiceAccount> findByCode(String code) {
        return jdbi.withExtension(ServiceAccountDao.class, dao -> dao.findByCode(code));
    }

    @Override
    public Optional<ServiceAccount> findByApplicationId(String applicationId) {
        return jdbi.withExtension(ServiceAccountDao.class, dao -> dao.findByApplicationId(applicationId));
    }

    @Override
    public List<ServiceAccount> findByClientId(String clientId) {
        return jdbi.withExtension(ServiceAccountDao.class, dao -> dao.findByClientId(clientId));
    }

    @Override
    public List<ServiceAccount> findActive() {
        return jdbi.withExtension(ServiceAccountDao.class, ServiceAccountDao::findActive);
    }

    @Override
    public List<ServiceAccount> findWithFilter(ServiceAccountFilter filter) {
        return jdbi.withHandle(handle -> {
            StringBuilder sql = new StringBuilder("SELECT * FROM service_accounts WHERE 1=1");

            if (filter.clientId() != null) {
                sql.append(" AND :clientId = ANY(client_ids)");
            }
            if (filter.active() != null) {
                sql.append(" AND active = :active");
            }
            if (filter.applicationId() != null) {
                sql.append(" AND application_id = :applicationId");
            }
            sql.append(" ORDER BY name");

            var query = handle.createQuery(sql.toString())
                .registerRowMapper(new ServiceAccountRowMapper());

            if (filter.clientId() != null) {
                query.bind("clientId", filter.clientId());
            }
            if (filter.active() != null) {
                query.bind("active", filter.active());
            }
            if (filter.applicationId() != null) {
                query.bind("applicationId", filter.applicationId());
            }

            return query.mapTo(ServiceAccount.class).list();
        });
    }

    @Override
    public List<ServiceAccount> listAll() {
        return jdbi.withExtension(ServiceAccountDao.class, ServiceAccountDao::listAll);
    }

    @Override
    public long count() {
        return jdbi.withExtension(ServiceAccountDao.class, ServiceAccountDao::count);
    }

    @Override
    public long countWithFilter(ServiceAccountFilter filter) {
        return jdbi.withHandle(handle -> {
            StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM service_accounts WHERE 1=1");

            if (filter.clientId() != null) {
                sql.append(" AND :clientId = ANY(client_ids)");
            }
            if (filter.active() != null) {
                sql.append(" AND active = :active");
            }
            if (filter.applicationId() != null) {
                sql.append(" AND application_id = :applicationId");
            }

            var query = handle.createQuery(sql.toString());

            if (filter.clientId() != null) {
                query.bind("clientId", filter.clientId());
            }
            if (filter.active() != null) {
                query.bind("active", filter.active());
            }
            if (filter.applicationId() != null) {
                query.bind("applicationId", filter.applicationId());
            }

            return query.mapTo(Long.class).one();
        });
    }

    @Override
    public void persist(ServiceAccount serviceAccount) {
        String[] clientIdsArray = toStringArray(serviceAccount.clientIds);
        var wc = serviceAccount.webhookCredentials;
        String rolesJson = JsonHelper.toJsonArray(serviceAccount.roles);

        jdbi.useExtension(ServiceAccountDao.class, dao ->
            dao.insert(serviceAccount, clientIdsArray,
                wc != null && wc.authType != null ? wc.authType.name() : null,
                wc != null ? wc.authTokenRef : null,
                wc != null ? wc.signingSecretRef : null,
                wc != null && wc.signingAlgorithm != null ? wc.signingAlgorithm.name() : null,
                wc != null ? wc.createdAt : null,
                wc != null ? wc.regeneratedAt : null,
                rolesJson));
    }

    @Override
    public void update(ServiceAccount serviceAccount) {
        serviceAccount.updatedAt = Instant.now();
        String[] clientIdsArray = toStringArray(serviceAccount.clientIds);
        var wc = serviceAccount.webhookCredentials;
        String rolesJson = JsonHelper.toJsonArray(serviceAccount.roles);

        jdbi.useExtension(ServiceAccountDao.class, dao ->
            dao.update(serviceAccount, clientIdsArray,
                wc != null && wc.authType != null ? wc.authType.name() : null,
                wc != null ? wc.authTokenRef : null,
                wc != null ? wc.signingSecretRef : null,
                wc != null && wc.signingAlgorithm != null ? wc.signingAlgorithm.name() : null,
                wc != null ? wc.createdAt : null,
                wc != null ? wc.regeneratedAt : null,
                rolesJson));
    }

    @Override
    public void delete(ServiceAccount serviceAccount) {
        jdbi.useExtension(ServiceAccountDao.class, dao -> dao.deleteById(serviceAccount.id));
    }

    @Override
    public boolean deleteById(String id) {
        return jdbi.withExtension(ServiceAccountDao.class, dao -> dao.deleteById(id) > 0);
    }

    private String[] toStringArray(List<String> list) {
        if (list == null || list.isEmpty()) {
            return new String[0];
        }
        return list.toArray(new String[0]);
    }
}
