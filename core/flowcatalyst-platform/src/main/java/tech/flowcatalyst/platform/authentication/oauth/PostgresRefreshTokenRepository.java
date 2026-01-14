package tech.flowcatalyst.platform.authentication.oauth;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.time.Instant;
import java.util.Optional;

/**
 * PostgreSQL implementation of RefreshTokenRepository using JDBI.
 */
@ApplicationScoped
@Typed(RefreshTokenRepository.class)
class PostgresRefreshTokenRepository implements RefreshTokenRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Optional<RefreshToken> findValidToken(String tokenHash) {
        return jdbi.withExtension(RefreshTokenDao.class, dao -> dao.findValidToken(tokenHash));
    }

    @Override
    public Optional<RefreshToken> findByTokenHash(String tokenHash) {
        return jdbi.withExtension(RefreshTokenDao.class, dao -> dao.findByTokenHash(tokenHash));
    }

    @Override
    public void persist(RefreshToken token) {
        jdbi.useExtension(RefreshTokenDao.class, dao -> dao.insert(token));
    }

    @Override
    public void update(RefreshToken token) {
        jdbi.useExtension(RefreshTokenDao.class, dao -> dao.update(token));
    }

    @Override
    public void revokeToken(String tokenHash, String replacedBy) {
        jdbi.useExtension(RefreshTokenDao.class, dao ->
            dao.revokeToken(tokenHash, replacedBy, Instant.now()));
    }

    @Override
    public void revokeTokenFamily(String tokenFamily) {
        jdbi.useExtension(RefreshTokenDao.class, dao ->
            dao.revokeTokenFamily(tokenFamily, Instant.now()));
    }
}
