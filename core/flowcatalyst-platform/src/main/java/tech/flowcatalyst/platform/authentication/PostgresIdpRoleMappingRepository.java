package tech.flowcatalyst.platform.authentication;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.util.Optional;

/**
 * PostgreSQL implementation of IdpRoleMappingRepository using JDBI.
 */
@ApplicationScoped
@Typed(IdpRoleMappingRepository.class)
class PostgresIdpRoleMappingRepository implements IdpRoleMappingRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Optional<IdpRoleMapping> findByIdpRoleName(String idpRoleName) {
        return jdbi.withExtension(IdpRoleMappingDao.class, dao -> dao.findByIdpRoleName(idpRoleName));
    }

    @Override
    public void persist(IdpRoleMapping mapping) {
        jdbi.useExtension(IdpRoleMappingDao.class, dao -> dao.insert(mapping));
    }

    @Override
    public void delete(IdpRoleMapping mapping) {
        jdbi.useExtension(IdpRoleMappingDao.class, dao -> dao.deleteById(mapping.id));
    }
}
