package tech.flowcatalyst.platform.principal;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of PrincipalRepository using JDBI.
 */
@ApplicationScoped
@Typed(PrincipalRepository.class)
class PostgresPrincipalRepository implements PrincipalRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Principal findById(String id) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.findById(id).orElse(null));
    }

    @Override
    public Optional<Principal> findByIdOptional(String id) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<Principal> findByEmail(String email) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.findByEmail(email));
    }

    @Override
    public Optional<Principal> findByServiceAccountCode(String code) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.findByServiceAccountCode(code));
    }

    @Override
    public List<Principal> findByType(PrincipalType type) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.findByType(type.name()));
    }

    @Override
    public List<Principal> findByClientId(String clientId) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.findByClientId(clientId));
    }

    @Override
    public List<Principal> findByIds(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        return jdbi.withHandle(handle ->
            handle.createQuery("SELECT * FROM principals WHERE id = ANY(:ids)")
                .bindArray("ids", String.class, ids.toArray(new String[0]))
                .registerRowMapper(new PrincipalRowMapper())
                .mapTo(Principal.class)
                .list()
        );
    }

    @Override
    public List<Principal> findUsersByClientId(String clientId) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.findUsersByClientId(clientId));
    }

    @Override
    public List<Principal> findActiveUsersByClientId(String clientId) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.findActiveUsersByClientId(clientId));
    }

    @Override
    public List<Principal> findByClientIdAndTypeAndActive(String clientId, PrincipalType type, Boolean active) {
        return jdbi.withExtension(PrincipalDao.class, dao ->
            dao.findByClientIdAndTypeAndActive(clientId, type.name(), active));
    }

    @Override
    public List<Principal> findByClientIdAndType(String clientId, PrincipalType type) {
        return jdbi.withExtension(PrincipalDao.class, dao ->
            dao.findByClientIdAndType(clientId, type.name()));
    }

    @Override
    public List<Principal> findByClientIdAndActive(String clientId, Boolean active) {
        return jdbi.withExtension(PrincipalDao.class, dao ->
            dao.findByClientIdAndActive(clientId, active));
    }

    @Override
    public List<Principal> findByActive(Boolean active) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.findByActive(active));
    }

    @Override
    public List<Principal> listAll() {
        return jdbi.withExtension(PrincipalDao.class, PrincipalDao::listAll);
    }

    @Override
    public Optional<Principal> findByServiceAccountClientId(String clientId) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.findByServiceAccountClientId(clientId));
    }

    @Override
    public long countByEmailDomain(String domain) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.countByEmailDomain(domain));
    }

    @Override
    public void persist(Principal principal) {
        jdbi.useExtension(PrincipalDao.class, dao -> dao.insert(
            principal.id,
            principal.type != null ? principal.type.name() : null,
            principal.scope != null ? principal.scope.name() : null,
            principal.clientId,
            principal.applicationId,
            principal.name,
            principal.active,
            JsonHelper.toJson(principal.userIdentity),
            JsonHelper.toJson(principal.serviceAccount),
            JsonHelper.toJsonArray(principal.roles),
            principal.createdAt,
            principal.updatedAt
        ));
    }

    @Override
    public void update(Principal principal) {
        principal.updatedAt = Instant.now();
        jdbi.useExtension(PrincipalDao.class, dao -> dao.update(
            principal.id,
            principal.type != null ? principal.type.name() : null,
            principal.scope != null ? principal.scope.name() : null,
            principal.clientId,
            principal.applicationId,
            principal.name,
            principal.active,
            JsonHelper.toJson(principal.userIdentity),
            JsonHelper.toJson(principal.serviceAccount),
            JsonHelper.toJsonArray(principal.roles),
            principal.updatedAt
        ));
    }

    @Override
    public boolean deleteById(String id) {
        return jdbi.withExtension(PrincipalDao.class, dao -> dao.deleteById(id) > 0);
    }
}
