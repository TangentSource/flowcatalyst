package tech.flowcatalyst.platform.application;

import com.fasterxml.jackson.core.type.TypeReference;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * JDBI row mapper for ApplicationClientConfig entity.
 */
public class ApplicationClientConfigRowMapper implements RowMapper<ApplicationClientConfig> {

    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
        new TypeReference<>() {};

    @Override
    public ApplicationClientConfig map(ResultSet rs, StatementContext ctx) throws SQLException {
        ApplicationClientConfig config = new ApplicationClientConfig();
        config.id = rs.getString("id");
        config.applicationId = rs.getString("application_id");
        config.clientId = rs.getString("client_id");
        config.enabled = rs.getBoolean("enabled");
        config.baseUrlOverride = rs.getString("base_url_override");
        config.websiteOverride = rs.getString("website_override");

        // Map JSONB to Map<String, Object>
        String configJson = rs.getString("config_json");
        if (configJson != null && !configJson.isEmpty()) {
            config.configJson = JsonHelper.fromJson(configJson, MAP_TYPE_REF);
        } else {
            config.configJson = new HashMap<>();
        }

        Timestamp createdAt = rs.getTimestamp("created_at");
        config.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp updatedAt = rs.getTimestamp("updated_at");
        config.updatedAt = updatedAt != null ? updatedAt.toInstant() : null;

        return config;
    }
}
