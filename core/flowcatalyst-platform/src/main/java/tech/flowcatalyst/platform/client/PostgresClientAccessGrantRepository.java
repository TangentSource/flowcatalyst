package tech.flowcatalyst.platform.client;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of ClientAccessGrantRepository using JDBI.
 */
@ApplicationScoped
@Typed(ClientAccessGrantRepository.class)
class PostgresClientAccessGrantRepository implements ClientAccessGrantRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public List<ClientAccessGrant> findByPrincipalId(String principalId) {
        return jdbi.withExtension(ClientAccessGrantDao.class, dao -> dao.findByPrincipalId(principalId));
    }

    @Override
    public List<ClientAccessGrant> findByClientId(String clientId) {
        return jdbi.withExtension(ClientAccessGrantDao.class, dao -> dao.findByClientId(clientId));
    }

    @Override
    public Optional<ClientAccessGrant> findByPrincipalIdAndClientId(String principalId, String clientId) {
        return jdbi.withExtension(ClientAccessGrantDao.class, dao ->
            dao.findByPrincipalIdAndClientId(principalId, clientId));
    }

    @Override
    public boolean existsByPrincipalIdAndClientId(String principalId, String clientId) {
        return jdbi.withExtension(ClientAccessGrantDao.class, dao ->
            dao.existsByPrincipalIdAndClientId(principalId, clientId));
    }

    @Override
    public void persist(ClientAccessGrant grant) {
        jdbi.useExtension(ClientAccessGrantDao.class, dao -> dao.insert(grant));
    }

    @Override
    public void delete(ClientAccessGrant grant) {
        jdbi.useExtension(ClientAccessGrantDao.class, dao -> dao.deleteById(grant.id));
    }

    @Override
    public void deleteByPrincipalId(String principalId) {
        jdbi.useExtension(ClientAccessGrantDao.class, dao -> dao.deleteByPrincipalId(principalId));
    }

    @Override
    public long deleteByPrincipalIdAndClientId(String principalId, String clientId) {
        return jdbi.withExtension(ClientAccessGrantDao.class, dao ->
            (long) dao.deleteByPrincipalIdAndClientId(principalId, clientId));
    }
}
