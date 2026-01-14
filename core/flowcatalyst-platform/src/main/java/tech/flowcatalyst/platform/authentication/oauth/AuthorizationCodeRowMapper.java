package tech.flowcatalyst.platform.authentication.oauth;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for AuthorizationCode entity.
 */
public class AuthorizationCodeRowMapper implements RowMapper<AuthorizationCode> {

    @Override
    public AuthorizationCode map(ResultSet rs, StatementContext ctx) throws SQLException {
        AuthorizationCode code = new AuthorizationCode();
        code.id = rs.getString("id");
        code.code = rs.getString("code");
        code.clientId = rs.getString("client_id");
        code.principalId = rs.getString("principal_id");
        code.redirectUri = rs.getString("redirect_uri");
        code.scope = rs.getString("scope");
        code.codeChallenge = rs.getString("code_challenge");
        code.codeChallengeMethod = rs.getString("code_challenge_method");
        code.nonce = rs.getString("nonce");
        code.state = rs.getString("state");
        code.contextClientId = rs.getString("context_client_id");
        code.used = rs.getBoolean("used");

        Timestamp createdAt = rs.getTimestamp("created_at");
        code.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp expiresAt = rs.getTimestamp("expires_at");
        code.expiresAt = expiresAt != null ? expiresAt.toInstant() : null;

        return code;
    }
}
