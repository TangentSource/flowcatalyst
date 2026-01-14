package tech.flowcatalyst.eventtype;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * JDBI row mapper for EventType entity.
 */
public class EventTypeRowMapper implements RowMapper<EventType> {

    @Override
    public EventType map(ResultSet rs, StatementContext ctx) throws SQLException {
        String statusStr = rs.getString("status");
        EventTypeStatus status = statusStr != null ? EventTypeStatus.valueOf(statusStr) : EventTypeStatus.CURRENT;

        String specVersionsJson = rs.getString("spec_versions");
        var specVersions = JsonHelper.fromJsonList(specVersionsJson, SpecVersion.class);

        Timestamp createdAt = rs.getTimestamp("created_at");
        Timestamp updatedAt = rs.getTimestamp("updated_at");

        return EventType.builder()
            .id(rs.getString("id"))
            .code(rs.getString("code"))
            .name(rs.getString("name"))
            .description(rs.getString("description"))
            .specVersions(specVersions != null ? specVersions : new ArrayList<>())
            .status(status)
            .createdAt(createdAt != null ? createdAt.toInstant() : null)
            .updatedAt(updatedAt != null ? updatedAt.toInstant() : null)
            .build();
    }
}
