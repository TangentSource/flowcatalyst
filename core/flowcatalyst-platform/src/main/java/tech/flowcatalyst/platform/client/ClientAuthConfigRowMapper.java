package tech.flowcatalyst.platform.client;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.platform.authentication.AuthProvider;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * JDBI row mapper for ClientAuthConfig entity.
 */
public class ClientAuthConfigRowMapper implements RowMapper<ClientAuthConfig> {

    @Override
    public ClientAuthConfig map(ResultSet rs, StatementContext ctx) throws SQLException {
        ClientAuthConfig config = new ClientAuthConfig();
        config.id = rs.getString("id");
        config.emailDomain = rs.getString("email_domain");
        config.clientId = rs.getString("client_id");
        config.primaryClientId = rs.getString("primary_client_id");
        config.oidcIssuerUrl = rs.getString("oidc_issuer_url");
        config.oidcClientId = rs.getString("oidc_client_id");
        config.oidcClientSecretRef = rs.getString("oidc_client_secret_ref");
        config.oidcMultiTenant = rs.getBoolean("oidc_multi_tenant");
        config.oidcIssuerPattern = rs.getString("oidc_issuer_pattern");

        String configTypeStr = rs.getString("config_type");
        config.configType = configTypeStr != null ? AuthConfigType.valueOf(configTypeStr) : AuthConfigType.CLIENT;

        String authProviderStr = rs.getString("auth_provider");
        config.authProvider = authProviderStr != null ? AuthProvider.valueOf(authProviderStr) : AuthProvider.INTERNAL;

        // Map TEXT[] arrays
        config.additionalClientIds = mapStringArray(rs.getArray("additional_client_ids"));
        config.grantedClientIds = mapStringArray(rs.getArray("granted_client_ids"));

        Timestamp createdAt = rs.getTimestamp("created_at");
        config.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp updatedAt = rs.getTimestamp("updated_at");
        config.updatedAt = updatedAt != null ? updatedAt.toInstant() : null;

        return config;
    }

    private java.util.List<String> mapStringArray(Array sqlArray) throws SQLException {
        if (sqlArray != null) {
            String[] arr = (String[]) sqlArray.getArray();
            return arr != null ? new ArrayList<>(Arrays.asList(arr)) : new ArrayList<>();
        }
        return new ArrayList<>();
    }
}
