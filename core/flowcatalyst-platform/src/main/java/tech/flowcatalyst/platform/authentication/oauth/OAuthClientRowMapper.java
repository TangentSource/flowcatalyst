package tech.flowcatalyst.platform.authentication.oauth;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * JDBI row mapper for OAuthClient entity.
 */
public class OAuthClientRowMapper implements RowMapper<OAuthClient> {

    @Override
    public OAuthClient map(ResultSet rs, StatementContext ctx) throws SQLException {
        OAuthClient client = new OAuthClient();
        client.id = rs.getString("id");
        client.clientId = rs.getString("client_id");
        client.clientName = rs.getString("client_name");
        client.clientSecretRef = rs.getString("client_secret_ref");
        client.defaultScopes = rs.getString("default_scopes");
        client.pkceRequired = rs.getBoolean("pkce_required");
        client.serviceAccountPrincipalId = rs.getString("service_account_principal_id");
        client.active = rs.getBoolean("active");

        String clientTypeStr = rs.getString("client_type");
        client.clientType = clientTypeStr != null ? OAuthClient.ClientType.valueOf(clientTypeStr) : OAuthClient.ClientType.PUBLIC;

        // Map TEXT[] arrays
        client.redirectUris = mapStringArray(rs.getArray("redirect_uris"));
        client.allowedOrigins = mapStringArray(rs.getArray("allowed_origins"));
        client.grantTypes = mapStringArray(rs.getArray("grant_types"));
        client.applicationIds = mapStringArray(rs.getArray("application_ids"));

        Timestamp createdAt = rs.getTimestamp("created_at");
        client.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp updatedAt = rs.getTimestamp("updated_at");
        client.updatedAt = updatedAt != null ? updatedAt.toInstant() : null;

        return client;
    }

    private java.util.List<String> mapStringArray(Array sqlArray) throws SQLException {
        if (sqlArray != null) {
            String[] arr = (String[]) sqlArray.getArray();
            return arr != null ? new ArrayList<>(Arrays.asList(arr)) : new ArrayList<>();
        }
        return new ArrayList<>();
    }
}
