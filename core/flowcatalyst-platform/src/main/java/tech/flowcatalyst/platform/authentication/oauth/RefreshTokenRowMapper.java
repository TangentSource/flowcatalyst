package tech.flowcatalyst.platform.authentication.oauth;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for RefreshToken entity.
 */
public class RefreshTokenRowMapper implements RowMapper<RefreshToken> {

    @Override
    public RefreshToken map(ResultSet rs, StatementContext ctx) throws SQLException {
        RefreshToken token = new RefreshToken();
        token.tokenHash = rs.getString("token_hash");
        token.principalId = rs.getString("principal_id");
        token.clientId = rs.getString("client_id");
        token.contextClientId = rs.getString("context_client_id");
        token.scope = rs.getString("scope");
        token.tokenFamily = rs.getString("token_family");
        token.revoked = rs.getBoolean("revoked");
        token.replacedBy = rs.getString("replaced_by");

        Timestamp createdAt = rs.getTimestamp("created_at");
        token.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp expiresAt = rs.getTimestamp("expires_at");
        token.expiresAt = expiresAt != null ? expiresAt.toInstant() : null;

        Timestamp revokedAt = rs.getTimestamp("revoked_at");
        token.revokedAt = revokedAt != null ? revokedAt.toInstant() : null;

        return token;
    }
}
