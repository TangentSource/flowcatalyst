package tech.flowcatalyst.platform.client;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for ClientAccessGrant entity.
 */
public class ClientAccessGrantRowMapper implements RowMapper<ClientAccessGrant> {

    @Override
    public ClientAccessGrant map(ResultSet rs, StatementContext ctx) throws SQLException {
        ClientAccessGrant grant = new ClientAccessGrant();
        grant.id = rs.getString("id");
        grant.principalId = rs.getString("principal_id");
        grant.clientId = rs.getString("client_id");

        Timestamp grantedAt = rs.getTimestamp("granted_at");
        grant.grantedAt = grantedAt != null ? grantedAt.toInstant() : null;

        Timestamp expiresAt = rs.getTimestamp("expires_at");
        grant.expiresAt = expiresAt != null ? expiresAt.toInstant() : null;

        return grant;
    }
}
