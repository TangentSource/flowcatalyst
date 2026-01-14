package tech.flowcatalyst.event.read;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * JDBI row mapper for EventRead entity.
 */
public class EventReadRowMapper implements RowMapper<EventRead> {

    @Override
    public EventRead map(ResultSet rs, StatementContext ctx) throws SQLException {
        EventRead event = new EventRead();
        event.id = rs.getString("id");
        event.eventId = rs.getString("event_id");
        event.specVersion = rs.getString("spec_version");
        event.type = rs.getString("type");
        event.application = rs.getString("application");
        event.subdomain = rs.getString("subdomain");
        event.aggregate = rs.getString("aggregate");
        event.source = rs.getString("source");
        event.subject = rs.getString("subject");

        Timestamp time = rs.getTimestamp("time");
        event.time = time != null ? time.toInstant() : null;

        event.data = rs.getString("data");
        event.messageGroup = rs.getString("message_group");
        event.correlationId = rs.getString("correlation_id");
        event.causationId = rs.getString("causation_id");
        event.deduplicationId = rs.getString("deduplication_id");
        event.clientId = rs.getString("client_id");

        Timestamp projectedAt = rs.getTimestamp("projected_at");
        event.projectedAt = projectedAt != null ? projectedAt.toInstant() : null;

        // Parse JSONB context_data
        String contextDataJson = rs.getString("context_data");
        if (contextDataJson != null) {
            event.contextData = JsonHelper.fromJsonList(contextDataJson, EventRead.ContextDataRead.class);
        }

        return event;
    }
}
