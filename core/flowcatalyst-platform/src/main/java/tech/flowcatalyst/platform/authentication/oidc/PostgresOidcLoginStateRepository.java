package tech.flowcatalyst.platform.authentication.oidc;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.util.Optional;

/**
 * PostgreSQL implementation of OidcLoginStateRepository using JDBI.
 */
@ApplicationScoped
@Typed(OidcLoginStateRepository.class)
class PostgresOidcLoginStateRepository implements OidcLoginStateRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Optional<OidcLoginState> findValidState(String state) {
        return jdbi.withExtension(OidcLoginStateDao.class, dao -> dao.findValidState(state));
    }

    @Override
    public void persist(OidcLoginState state) {
        jdbi.useExtension(OidcLoginStateDao.class, dao -> dao.insert(state));
    }

    @Override
    public void deleteByState(String state) {
        jdbi.useExtension(OidcLoginStateDao.class, dao -> dao.deleteByState(state));
    }
}
