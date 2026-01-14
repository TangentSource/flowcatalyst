package tech.flowcatalyst.platform.authentication;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for IdpRoleMapping entity.
 */
public class IdpRoleMappingRowMapper implements RowMapper<IdpRoleMapping> {

    @Override
    public IdpRoleMapping map(ResultSet rs, StatementContext ctx) throws SQLException {
        IdpRoleMapping mapping = new IdpRoleMapping();
        mapping.id = rs.getString("id");
        mapping.idpRoleName = rs.getString("idp_role_name");
        mapping.internalRoleName = rs.getString("internal_role_name");

        Timestamp createdAt = rs.getTimestamp("created_at");
        mapping.createdAt = createdAt != null ? createdAt.toInstant() : null;

        return mapping;
    }
}
