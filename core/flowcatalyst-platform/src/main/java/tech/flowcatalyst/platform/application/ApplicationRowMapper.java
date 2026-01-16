package tech.flowcatalyst.platform.application;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for Application entity.
 */
public class ApplicationRowMapper implements RowMapper<Application> {

    @Override
    public Application map(ResultSet rs, StatementContext ctx) throws SQLException {
        Application app = new Application();
        app.id = rs.getString("id");
        app.code = rs.getString("code");
        app.name = rs.getString("name");
        app.description = rs.getString("description");

        String typeStr = rs.getString("type");
        app.type = typeStr != null ? Application.ApplicationType.valueOf(typeStr) : Application.ApplicationType.APPLICATION;

        app.iconUrl = rs.getString("icon_url");
        app.website = rs.getString("website");
        app.logo = rs.getString("logo");
        app.logoMimeType = rs.getString("logo_mime_type");
        app.defaultBaseUrl = rs.getString("default_base_url");
        app.serviceAccountId = rs.getString("service_account_id");
        app.active = rs.getBoolean("active");

        Timestamp createdAt = rs.getTimestamp("created_at");
        app.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp updatedAt = rs.getTimestamp("updated_at");
        app.updatedAt = updatedAt != null ? updatedAt.toInstant() : null;

        return app;
    }
}
