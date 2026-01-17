package tech.flowcatalyst.platform.principal;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.platform.authentication.IdpType;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for Principal entity.
 * Maps flat columns for user identity and JSONB columns for serviceAccount and roles.
 */
public class PrincipalRowMapper implements RowMapper<Principal> {

    @Override
    public Principal map(ResultSet rs, StatementContext ctx) throws SQLException {
        Principal p = new Principal();
        p.id = rs.getString("id");

        String typeStr = rs.getString("type");
        p.type = typeStr != null ? PrincipalType.valueOf(typeStr) : null;

        String scopeStr = rs.getString("scope");
        p.scope = scopeStr != null ? UserScope.valueOf(scopeStr) : null;

        p.clientId = rs.getString("client_id");
        p.applicationId = rs.getString("application_id");
        p.name = rs.getString("name");
        p.active = rs.getBoolean("active");

        // Build UserIdentity from flat columns
        String email = rs.getString("email");
        if (email != null) {
            UserIdentity ui = new UserIdentity();
            ui.email = email;
            ui.emailDomain = rs.getString("email_domain");
            String idpTypeStr = rs.getString("idp_type");
            ui.idpType = idpTypeStr != null ? IdpType.valueOf(idpTypeStr) : null;
            ui.externalIdpId = rs.getString("external_idp_id");
            ui.passwordHash = rs.getString("password_hash");
            Timestamp lastLoginAt = rs.getTimestamp("last_login_at");
            ui.lastLoginAt = lastLoginAt != null ? lastLoginAt.toInstant() : null;
            p.userIdentity = ui;
        }

        // Parse JSONB columns
        String serviceAccountJson = rs.getString("service_account");
        if (serviceAccountJson != null && !serviceAccountJson.isEmpty()) {
            p.serviceAccount = JsonHelper.fromJson(serviceAccountJson, ServiceAccount.class);
        }

        String rolesJson = rs.getString("roles");
        p.roles = JsonHelper.fromJsonList(rolesJson, Principal.RoleAssignment.class);

        Timestamp createdAt = rs.getTimestamp("created_at");
        p.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp updatedAt = rs.getTimestamp("updated_at");
        p.updatedAt = updatedAt != null ? updatedAt.toInstant() : null;

        return p;
    }
}
