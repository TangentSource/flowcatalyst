package tech.flowcatalyst.serviceaccount.repository;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.dispatchjob.model.SignatureAlgorithm;
import tech.flowcatalyst.platform.principal.Principal;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;
import tech.flowcatalyst.serviceaccount.entity.ServiceAccount;
import tech.flowcatalyst.serviceaccount.entity.WebhookAuthType;
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
 * Maps flat columns for webhook credentials and JSONB columns for roles.
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

        // Build WebhookCredentials from flat columns
        String whAuthType = rs.getString("wh_auth_type");
        String whAuthTokenRef = rs.getString("wh_auth_token_ref");
        String whSigningSecretRef = rs.getString("wh_signing_secret_ref");
        if (whAuthType != null || whAuthTokenRef != null || whSigningSecretRef != null) {
            WebhookCredentials wc = new WebhookCredentials();
            wc.authType = whAuthType != null ? WebhookAuthType.valueOf(whAuthType) : WebhookAuthType.BEARER_TOKEN;
            wc.authTokenRef = whAuthTokenRef;
            wc.signingSecretRef = whSigningSecretRef;
            String whSigningAlgorithm = rs.getString("wh_signing_algorithm");
            wc.signingAlgorithm = whSigningAlgorithm != null ? SignatureAlgorithm.valueOf(whSigningAlgorithm) : SignatureAlgorithm.HMAC_SHA256;
            Timestamp whCreatedAt = rs.getTimestamp("wh_credentials_created_at");
            wc.createdAt = whCreatedAt != null ? whCreatedAt.toInstant() : null;
            Timestamp whRegeneratedAt = rs.getTimestamp("wh_credentials_regenerated_at");
            wc.regeneratedAt = whRegeneratedAt != null ? whRegeneratedAt.toInstant() : null;
            sa.webhookCredentials = wc;
        }

        // Map JSONB roles column
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
