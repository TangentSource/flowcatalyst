package tech.flowcatalyst.platform.authentication.oauth;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.util.Optional;

/**
 * PostgreSQL implementation of AuthorizationCodeRepository using JDBI.
 */
@ApplicationScoped
@Typed(AuthorizationCodeRepository.class)
class PostgresAuthorizationCodeRepository implements AuthorizationCodeRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Optional<AuthorizationCode> findValidCode(String code) {
        return jdbi.withExtension(AuthorizationCodeDao.class, dao -> dao.findValidCode(code));
    }

    @Override
    public void persist(AuthorizationCode code) {
        jdbi.useExtension(AuthorizationCodeDao.class, dao -> dao.insert(code));
    }

    @Override
    public void markAsUsed(String code) {
        jdbi.useExtension(AuthorizationCodeDao.class, dao -> dao.markAsUsed(code));
    }
}
