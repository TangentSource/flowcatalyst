package tech.flowcatalyst.platform.authorization;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;

/**
 * JDBI row mapper for AuthRole entity.
 */
public class AuthRoleRowMapper implements RowMapper<AuthRole> {

    @Override
    public AuthRole map(ResultSet rs, StatementContext ctx) throws SQLException {
        AuthRole role = new AuthRole();
        role.id = rs.getString("id");
        role.applicationId = rs.getString("application_id");
        role.applicationCode = rs.getString("application_code");
        role.name = rs.getString("name");
        role.displayName = rs.getString("display_name");
        role.description = rs.getString("description");
        role.clientManaged = rs.getBoolean("client_managed");

        // Map permissions TEXT[] array to Set<String>
        Array permissionsArray = rs.getArray("permissions");
        if (permissionsArray != null) {
            String[] arr = (String[]) permissionsArray.getArray();
            role.permissions = arr != null ? new HashSet<>(Arrays.asList(arr)) : new HashSet<>();
        } else {
            role.permissions = new HashSet<>();
        }

        String sourceStr = rs.getString("source");
        role.source = sourceStr != null ? AuthRole.RoleSource.valueOf(sourceStr) : AuthRole.RoleSource.DATABASE;

        Timestamp createdAt = rs.getTimestamp("created_at");
        role.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp updatedAt = rs.getTimestamp("updated_at");
        role.updatedAt = updatedAt != null ? updatedAt.toInstant() : null;

        return role;
    }
}
