package tech.flowcatalyst.platform.authentication.oauth;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.time.Instant;
import java.util.Optional;

/**
 * JDBI DAO interface for RefreshToken entity.
 */
@RegisterRowMapper(RefreshTokenRowMapper.class)
public interface RefreshTokenDao {

    @SqlQuery("""
        SELECT * FROM refresh_tokens
        WHERE token_hash = :tokenHash AND revoked = false AND expires_at > NOW()
        """)
    Optional<RefreshToken> findValidToken(@Bind("tokenHash") String tokenHash);

    @SqlQuery("SELECT * FROM refresh_tokens WHERE token_hash = :tokenHash")
    Optional<RefreshToken> findByTokenHash(@Bind("tokenHash") String tokenHash);

    @SqlUpdate("""
        INSERT INTO refresh_tokens (token_hash, principal_id, client_id, context_client_id,
            scope, token_family, revoked, revoked_at, replaced_by, expires_at, created_at)
        VALUES (:tokenHash, :principalId, :clientId, :contextClientId,
            :scope, :tokenFamily, :revoked, :revokedAt, :replacedBy, :expiresAt, :createdAt)
        """)
    void insert(@BindBean RefreshToken token);

    @SqlUpdate("""
        UPDATE refresh_tokens SET principal_id = :principalId, client_id = :clientId,
            context_client_id = :contextClientId, scope = :scope, token_family = :tokenFamily,
            revoked = :revoked, revoked_at = :revokedAt, replaced_by = :replacedBy, expires_at = :expiresAt
        WHERE token_hash = :tokenHash
        """)
    void update(@BindBean RefreshToken token);

    @SqlUpdate("""
        UPDATE refresh_tokens SET revoked = true, revoked_at = :revokedAt, replaced_by = :replacedBy
        WHERE token_hash = :tokenHash
        """)
    int revokeToken(@Bind("tokenHash") String tokenHash,
                    @Bind("replacedBy") String replacedBy,
                    @Bind("revokedAt") Instant revokedAt);

    @SqlUpdate("UPDATE refresh_tokens SET revoked = true, revoked_at = :revokedAt WHERE token_family = :tokenFamily")
    int revokeTokenFamily(@Bind("tokenFamily") String tokenFamily, @Bind("revokedAt") Instant revokedAt);
}
