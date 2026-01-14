package tech.flowcatalyst.serviceaccount.repository;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.platform.principal.Principal;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;
import tech.flowcatalyst.serviceaccount.entity.ServiceAccount;
import tech.flowcatalyst.serviceaccount.entity.WebhookCredentials;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * JDBI row mapper for ServiceAccount entity.
 */
public class ServiceAccountRowMapper implements RowMapper<ServiceAccount> {

    @Override
    public ServiceAccount map(ResultSet rs, StatementContext ctx) throws SQLException {
        ServiceAccount sa = new ServiceAccount();
        sa.id = rs.getString("id");
        sa.code = rs.getString("code");
        sa.name = rs.getString("name");
        sa.description = rs.getString("description");
        sa.applicationId = rs.getString("application_id");
        sa.active = rs.getBoolean("active");

        // Map client_ids TEXT[] array
        Array clientIdsArray = rs.getArray("client_ids");
        if (clientIdsArray != null) {
            String[] arr = (String[]) clientIdsArray.getArray();
            sa.clientIds = arr != null ? new ArrayList<>(Arrays.asList(arr)) : new ArrayList<>();
        } else {
            sa.clientIds = new ArrayList<>();
        }

        // Map JSONB columns
        String webhookCredentialsJson = rs.getString("webhook_credentials");
        sa.webhookCredentials = JsonHelper.fromJson(webhookCredentialsJson, WebhookCredentials.class);

        String rolesJson = rs.getString("roles");
        sa.roles = JsonHelper.fromJsonList(rolesJson, Principal.RoleAssignment.class);

        // Map timestamps
        Timestamp lastUsedAt = rs.getTimestamp("last_used_at");
        sa.lastUsedAt = lastUsedAt != null ? lastUsedAt.toInstant() : null;

        Timestamp createdAt = rs.getTimestamp("created_at");
        sa.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp updatedAt = rs.getTimestamp("updated_at");
        sa.updatedAt = updatedAt != null ? updatedAt.toInstant() : null;

        return sa;
    }
}
