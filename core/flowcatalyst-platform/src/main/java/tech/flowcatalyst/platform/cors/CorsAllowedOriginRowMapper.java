package tech.flowcatalyst.platform.cors;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for CorsAllowedOrigin entity.
 */
public class CorsAllowedOriginRowMapper implements RowMapper<CorsAllowedOrigin> {

    @Override
    public CorsAllowedOrigin map(ResultSet rs, StatementContext ctx) throws SQLException {
        CorsAllowedOrigin entry = new CorsAllowedOrigin();
        entry.id = rs.getString("id");
        entry.origin = rs.getString("origin");
        entry.description = rs.getString("description");
        entry.createdBy = rs.getString("created_by");

        Timestamp createdAt = rs.getTimestamp("created_at");
        entry.createdAt = createdAt != null ? createdAt.toInstant() : null;

        return entry;
    }
}
