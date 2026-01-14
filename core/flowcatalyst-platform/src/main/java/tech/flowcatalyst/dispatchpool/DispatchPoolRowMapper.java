package tech.flowcatalyst.dispatchpool;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for DispatchPool entity.
 */
public class DispatchPoolRowMapper implements RowMapper<DispatchPool> {

    @Override
    public DispatchPool map(ResultSet rs, StatementContext ctx) throws SQLException {
        String statusStr = rs.getString("status");
        DispatchPoolStatus status = statusStr != null ? DispatchPoolStatus.valueOf(statusStr) : DispatchPoolStatus.ACTIVE;

        Timestamp createdAt = rs.getTimestamp("created_at");
        Timestamp updatedAt = rs.getTimestamp("updated_at");

        String code = rs.getString("code");
        String name = getStringOrNull(rs, "name");
        String description = getStringOrNull(rs, "description");
        String clientIdentifier = getStringOrNull(rs, "client_identifier");

        return DispatchPool.builder()
            .id(rs.getString("id"))
            .code(code)
            .name(name != null ? name : code)  // fallback to code if name is null
            .description(description)
            .rateLimit(rs.getInt("rate_limit"))
            .concurrency(rs.getInt("concurrency"))
            .clientId(rs.getString("client_id"))
            .clientIdentifier(clientIdentifier)
            .status(status)
            .createdAt(createdAt != null ? createdAt.toInstant() : null)
            .updatedAt(updatedAt != null ? updatedAt.toInstant() : null)
            .build();
    }

    private String getStringOrNull(ResultSet rs, String column) throws SQLException {
        try {
            return rs.getString(column);
        } catch (SQLException e) {
            // Column may not exist in older schemas
            return null;
        }
    }
}
