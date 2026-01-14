package tech.flowcatalyst.event;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * JDBI row mapper for Event entity.
 */
public class EventRowMapper implements RowMapper<Event> {

    @Override
    public Event map(ResultSet rs, StatementContext ctx) throws SQLException {
        Event event = new Event();

        event.id = rs.getString("id");
        event.type = rs.getString("type");
        event.source = rs.getString("source");
        event.subject = rs.getString("subject");

        Timestamp time = rs.getTimestamp("time");
        event.time = time != null ? time.toInstant() : null;

        // Data is stored as JSONB but we read it as a string
        event.data = rs.getString("data");

        event.correlationId = rs.getString("correlation_id");
        event.causationId = rs.getString("causation_id");
        event.deduplicationId = rs.getString("deduplication_id");
        event.messageGroup = rs.getString("message_group");
        event.clientId = rs.getString("client_id");

        // Parse JSONB context_data array
        String contextDataJson = rs.getString("context_data");
        event.contextData = contextDataJson != null
            ? JsonHelper.fromJsonList(contextDataJson, ContextData.class)
            : new ArrayList<>();

        // Note: specVersion is not in the database schema for events
        // It should be set based on type lookup if needed

        return event;
    }
}
