package tech.flowcatalyst.platform.authentication.oidc;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for OidcLoginState entity.
 */
public class OidcLoginStateRowMapper implements RowMapper<OidcLoginState> {

    @Override
    public OidcLoginState map(ResultSet rs, StatementContext ctx) throws SQLException {
        OidcLoginState state = new OidcLoginState();
        state.state = rs.getString("state");
        state.emailDomain = rs.getString("email_domain");
        state.authConfigId = rs.getString("auth_config_id");
        state.nonce = rs.getString("nonce");
        state.codeVerifier = rs.getString("code_verifier");
        state.returnUrl = rs.getString("return_url");
        state.oauthClientId = rs.getString("oauth_client_id");
        state.oauthRedirectUri = rs.getString("oauth_redirect_uri");
        state.oauthScope = rs.getString("oauth_scope");
        state.oauthState = rs.getString("oauth_state");
        state.oauthNonce = rs.getString("oauth_nonce");
        state.oauthCodeChallenge = rs.getString("oauth_code_challenge");
        state.oauthCodeChallengeMethod = rs.getString("oauth_code_challenge_method");

        Timestamp createdAt = rs.getTimestamp("created_at");
        state.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp expiresAt = rs.getTimestamp("expires_at");
        state.expiresAt = expiresAt != null ? expiresAt.toInstant() : null;

        return state;
    }
}
