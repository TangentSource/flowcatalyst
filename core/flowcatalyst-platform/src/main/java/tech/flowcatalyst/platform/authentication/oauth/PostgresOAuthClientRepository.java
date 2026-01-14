package tech.flowcatalyst.platform.authentication.oauth;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of OAuthClientRepository using JDBI.
 */
@ApplicationScoped
@Typed(OAuthClientRepository.class)
class PostgresOAuthClientRepository implements OAuthClientRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Optional<OAuthClient> findByIdOptional(String id) {
        return jdbi.withExtension(OAuthClientDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<OAuthClient> findByClientId(String clientId) {
        return jdbi.withExtension(OAuthClientDao.class, dao -> dao.findByClientId(clientId));
    }

    @Override
    public Optional<OAuthClient> findByClientIdIncludingInactive(String clientId) {
        return jdbi.withExtension(OAuthClientDao.class, dao -> dao.findByClientIdIncludingInactive(clientId));
    }

    @Override
    public List<OAuthClient> findByApplicationIdAndActive(String applicationId, boolean active) {
        return jdbi.withExtension(OAuthClientDao.class, dao -> dao.findByApplicationIdAndActive(applicationId, active));
    }

    @Override
    public List<OAuthClient> findByApplicationId(String applicationId) {
        return jdbi.withExtension(OAuthClientDao.class, dao -> dao.findByApplicationId(applicationId));
    }

    @Override
    public List<OAuthClient> findByActive(boolean active) {
        return jdbi.withExtension(OAuthClientDao.class, dao -> dao.findByActive(active));
    }

    @Override
    public List<OAuthClient> listAll() {
        return jdbi.withExtension(OAuthClientDao.class, OAuthClientDao::listAll);
    }

    @Override
    public boolean isOriginAllowedByAnyClient(String origin) {
        return jdbi.withExtension(OAuthClientDao.class, dao -> dao.isOriginAllowedByAnyClient(origin));
    }

    @Override
    public void persist(OAuthClient client) {
        String clientType = client.clientType != null ? client.clientType.name() : "PUBLIC";
        jdbi.useExtension(OAuthClientDao.class, dao ->
            dao.insert(client, clientType,
                toStringArray(client.redirectUris),
                toStringArray(client.allowedOrigins),
                toStringArray(client.grantTypes),
                toStringArray(client.applicationIds)));
    }

    @Override
    public void update(OAuthClient client) {
        client.updatedAt = Instant.now();
        String clientType = client.clientType != null ? client.clientType.name() : "PUBLIC";
        jdbi.useExtension(OAuthClientDao.class, dao ->
            dao.update(client, clientType,
                toStringArray(client.redirectUris),
                toStringArray(client.allowedOrigins),
                toStringArray(client.grantTypes),
                toStringArray(client.applicationIds)));
    }

    @Override
    public void delete(OAuthClient client) {
        jdbi.useExtension(OAuthClientDao.class, dao -> dao.deleteById(client.id));
    }

    @Override
    public long deleteByServiceAccountPrincipalId(String principalId) {
        return jdbi.withExtension(OAuthClientDao.class, dao -> dao.deleteByServiceAccountPrincipalId(principalId));
    }

    private String[] toStringArray(List<String> list) {
        if (list == null || list.isEmpty()) {
            return new String[0];
        }
        return list.toArray(new String[0]);
    }
}
