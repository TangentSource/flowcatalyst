package tech.flowcatalyst.platform.authentication.oidc;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Optional;

/**
 * JDBI DAO interface for OidcLoginState entity.
 */
@RegisterRowMapper(OidcLoginStateRowMapper.class)
public interface OidcLoginStateDao {

    @SqlQuery("SELECT * FROM oidc_login_states WHERE state = :state AND expires_at > NOW()")
    Optional<OidcLoginState> findValidState(@Bind("state") String state);

    @SqlUpdate("""
        INSERT INTO oidc_login_states (state, email_domain, auth_config_id, nonce, code_verifier,
            return_url, oauth_client_id, oauth_redirect_uri, oauth_scope, oauth_state,
            oauth_nonce, oauth_code_challenge, oauth_code_challenge_method, expires_at, created_at)
        VALUES (:state, :emailDomain, :authConfigId, :nonce, :codeVerifier,
            :returnUrl, :oauthClientId, :oauthRedirectUri, :oauthScope, :oauthState,
            :oauthNonce, :oauthCodeChallenge, :oauthCodeChallengeMethod, :expiresAt, :createdAt)
        """)
    void insert(@BindBean OidcLoginState loginState);

    @SqlUpdate("DELETE FROM oidc_login_states WHERE state = :state")
    int deleteByState(@Bind("state") String state);
}
