package tech.flowcatalyst.platform.application;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of ApplicationClientConfigRepository using JDBI.
 */
@ApplicationScoped
@Typed(ApplicationClientConfigRepository.class)
class PostgresApplicationClientConfigRepository implements ApplicationClientConfigRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Optional<ApplicationClientConfig> findByIdOptional(String id) {
        return jdbi.withExtension(ApplicationClientConfigDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<ApplicationClientConfig> findByApplicationAndClient(String applicationId, String clientId) {
        return jdbi.withExtension(ApplicationClientConfigDao.class, dao ->
            dao.findByApplicationAndClient(applicationId, clientId));
    }

    @Override
    public List<ApplicationClientConfig> findByApplication(String applicationId) {
        return jdbi.withExtension(ApplicationClientConfigDao.class, dao -> dao.findByApplication(applicationId));
    }

    @Override
    public List<ApplicationClientConfig> findByClient(String clientId) {
        return jdbi.withExtension(ApplicationClientConfigDao.class, dao -> dao.findByClient(clientId));
    }

    @Override
    public List<ApplicationClientConfig> findEnabledByClient(String clientId) {
        return jdbi.withExtension(ApplicationClientConfigDao.class, dao -> dao.findEnabledByClient(clientId));
    }

    @Override
    public boolean isApplicationEnabledForClient(String applicationId, String clientId) {
        return jdbi.withExtension(ApplicationClientConfigDao.class, dao ->
            dao.isApplicationEnabledForClient(applicationId, clientId));
    }

    @Override
    public long countByApplication(String applicationId) {
        return jdbi.withExtension(ApplicationClientConfigDao.class, dao -> dao.countByApplication(applicationId));
    }

    @Override
    public void persist(ApplicationClientConfig config) {
        String configJson = JsonHelper.toJson(config.configJson);
        jdbi.useExtension(ApplicationClientConfigDao.class, dao -> dao.insert(config, configJson));
    }

    @Override
    public void update(ApplicationClientConfig config) {
        config.updatedAt = Instant.now();
        String configJson = JsonHelper.toJson(config.configJson);
        jdbi.useExtension(ApplicationClientConfigDao.class, dao -> dao.update(config, configJson));
    }

    @Override
    public void delete(ApplicationClientConfig config) {
        jdbi.useExtension(ApplicationClientConfigDao.class, dao -> dao.deleteById(config.id));
    }

    @Override
    public void deleteByApplicationAndClient(String applicationId, String clientId) {
        jdbi.useExtension(ApplicationClientConfigDao.class, dao ->
            dao.deleteByApplicationAndClient(applicationId, clientId));
    }
}
