package tech.flowcatalyst.platform.authorization;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of AuthRoleRepository using JDBI.
 */
@ApplicationScoped
@Typed(AuthRoleRepository.class)
class PostgresAuthRoleRepository implements AuthRoleRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Optional<AuthRole> findByName(String name) {
        return jdbi.withExtension(AuthRoleDao.class, dao -> dao.findByName(name));
    }

    @Override
    public List<AuthRole> findByApplicationCode(String applicationCode) {
        return jdbi.withExtension(AuthRoleDao.class, dao -> dao.findByApplicationCode(applicationCode));
    }

    @Override
    public List<AuthRole> findBySource(AuthRole.RoleSource source) {
        return jdbi.withExtension(AuthRoleDao.class, dao -> dao.findBySource(source.name()));
    }

    @Override
    public List<AuthRole> listAll() {
        return jdbi.withExtension(AuthRoleDao.class, AuthRoleDao::listAll);
    }

    @Override
    public boolean existsByName(String name) {
        return jdbi.withExtension(AuthRoleDao.class, dao -> dao.existsByName(name));
    }

    @Override
    public void persist(AuthRole role) {
        String[] permissionsArray = toStringArray(role.permissions);
        String source = role.source != null ? role.source.name() : "DATABASE";
        jdbi.useExtension(AuthRoleDao.class, dao -> dao.insert(role, permissionsArray, source));
    }

    @Override
    public void update(AuthRole role) {
        role.updatedAt = Instant.now();
        String[] permissionsArray = toStringArray(role.permissions);
        String source = role.source != null ? role.source.name() : "DATABASE";
        jdbi.useExtension(AuthRoleDao.class, dao -> dao.update(role, permissionsArray, source));
    }

    @Override
    public void delete(AuthRole role) {
        jdbi.useExtension(AuthRoleDao.class, dao -> dao.deleteById(role.id));
    }

    private String[] toStringArray(java.util.Set<String> set) {
        if (set == null || set.isEmpty()) {
            return new String[0];
        }
        return set.toArray(new String[0]);
    }
}
