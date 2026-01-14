package tech.flowcatalyst.platform.authorization;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of AuthPermissionRepository using JDBI.
 */
@ApplicationScoped
@Typed(AuthPermissionRepository.class)
class PostgresAuthPermissionRepository implements AuthPermissionRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Optional<AuthPermission> findByName(String name) {
        return jdbi.withExtension(AuthPermissionDao.class, dao -> dao.findByName(name));
    }

    @Override
    public List<AuthPermission> findByApplicationId(String applicationId) {
        return jdbi.withExtension(AuthPermissionDao.class, dao -> dao.findByApplicationId(applicationId));
    }

    @Override
    public List<AuthPermission> listAll() {
        return jdbi.withExtension(AuthPermissionDao.class, AuthPermissionDao::listAll);
    }

    @Override
    public boolean existsByName(String name) {
        return jdbi.withExtension(AuthPermissionDao.class, dao -> dao.existsByName(name));
    }

    @Override
    public void persist(AuthPermission permission) {
        String source = permission.source != null ? permission.source.name() : "SDK";
        jdbi.useExtension(AuthPermissionDao.class, dao -> dao.insert(permission, source));
    }

    @Override
    public void update(AuthPermission permission) {
        String source = permission.source != null ? permission.source.name() : "SDK";
        jdbi.useExtension(AuthPermissionDao.class, dao -> dao.update(permission, source));
    }

    @Override
    public void delete(AuthPermission permission) {
        jdbi.useExtension(AuthPermissionDao.class, dao -> dao.deleteById(permission.id));
    }

    @Override
    public long deleteByApplicationId(String applicationId) {
        return jdbi.withExtension(AuthPermissionDao.class, dao -> dao.deleteByApplicationId(applicationId));
    }
}
