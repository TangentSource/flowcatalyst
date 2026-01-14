package tech.flowcatalyst.platform.client;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of ClientAuthConfigRepository using JDBI.
 */
@ApplicationScoped
@Typed(ClientAuthConfigRepository.class)
class PostgresClientAuthConfigRepository implements ClientAuthConfigRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Optional<ClientAuthConfig> findByIdOptional(String id) {
        return jdbi.withExtension(ClientAuthConfigDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<ClientAuthConfig> findByEmailDomain(String emailDomain) {
        return jdbi.withExtension(ClientAuthConfigDao.class, dao -> dao.findByEmailDomain(emailDomain));
    }

    @Override
    public List<ClientAuthConfig> findByClientId(String clientId) {
        return jdbi.withExtension(ClientAuthConfigDao.class, dao -> dao.findByClientId(clientId));
    }

    @Override
    public List<ClientAuthConfig> findByConfigType(AuthConfigType configType) {
        return jdbi.withExtension(ClientAuthConfigDao.class, dao ->
            dao.findByConfigType(configType != null ? configType.name() : "CLIENT"));
    }

    @Override
    public List<ClientAuthConfig> listAll() {
        return jdbi.withExtension(ClientAuthConfigDao.class, ClientAuthConfigDao::listAll);
    }

    @Override
    public boolean existsByEmailDomain(String emailDomain) {
        return jdbi.withExtension(ClientAuthConfigDao.class, dao -> dao.existsByEmailDomain(emailDomain));
    }

    @Override
    public void persist(ClientAuthConfig config) {
        String configType = config.configType != null ? config.configType.name() : "CLIENT";
        String authProvider = config.authProvider != null ? config.authProvider.name() : "INTERNAL";
        jdbi.useExtension(ClientAuthConfigDao.class, dao ->
            dao.insert(config, configType, authProvider,
                toStringArray(config.additionalClientIds),
                toStringArray(config.grantedClientIds)));
    }

    @Override
    public void update(ClientAuthConfig config) {
        config.updatedAt = Instant.now();
        String configType = config.configType != null ? config.configType.name() : "CLIENT";
        String authProvider = config.authProvider != null ? config.authProvider.name() : "INTERNAL";
        jdbi.useExtension(ClientAuthConfigDao.class, dao ->
            dao.update(config, configType, authProvider,
                toStringArray(config.additionalClientIds),
                toStringArray(config.grantedClientIds)));
    }

    @Override
    public void delete(ClientAuthConfig config) {
        jdbi.useExtension(ClientAuthConfigDao.class, dao -> dao.deleteById(config.id));
    }

    private String[] toStringArray(List<String> list) {
        if (list == null || list.isEmpty()) {
            return new String[0];
        }
        return list.toArray(new String[0]);
    }
}
