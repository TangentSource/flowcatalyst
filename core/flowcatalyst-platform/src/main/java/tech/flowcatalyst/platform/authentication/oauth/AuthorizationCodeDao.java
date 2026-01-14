package tech.flowcatalyst.platform.authentication.oauth;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Optional;

/**
 * JDBI DAO interface for AuthorizationCode entity.
 */
@RegisterRowMapper(AuthorizationCodeRowMapper.class)
public interface AuthorizationCodeDao {

    @SqlQuery("""
        SELECT * FROM authorization_codes
        WHERE code = :code AND used = false AND expires_at > NOW()
        """)
    Optional<AuthorizationCode> findValidCode(@Bind("code") String code);

    @SqlUpdate("""
        INSERT INTO authorization_codes (id, code, client_id, principal_id, redirect_uri,
            scope, code_challenge, code_challenge_method, nonce, state, context_client_id,
            used, expires_at, created_at)
        VALUES (:id, :code, :clientId, :principalId, :redirectUri,
            :scope, :codeChallenge, :codeChallengeMethod, :nonce, :state, :contextClientId,
            :used, :expiresAt, :createdAt)
        """)
    void insert(@BindFields AuthorizationCode authCode);

    @SqlUpdate("UPDATE authorization_codes SET used = true WHERE code = :code")
    int markAsUsed(@Bind("code") String code);
}
