package tech.flowcatalyst.platform.authorization;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for AuthPermission entity.
 */
public class AuthPermissionRowMapper implements RowMapper<AuthPermission> {

    @Override
    public AuthPermission map(ResultSet rs, StatementContext ctx) throws SQLException {
        AuthPermission permission = new AuthPermission();
        permission.id = rs.getString("id");
        permission.applicationId = rs.getString("application_id");
        permission.name = rs.getString("name");
        permission.displayName = rs.getString("display_name");
        permission.description = rs.getString("description");

        String sourceStr = rs.getString("source");
        permission.source = sourceStr != null ? AuthPermission.PermissionSource.valueOf(sourceStr) : AuthPermission.PermissionSource.SDK;

        Timestamp createdAt = rs.getTimestamp("created_at");
        permission.createdAt = createdAt != null ? createdAt.toInstant() : null;

        return permission;
    }
}
