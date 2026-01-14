package tech.flowcatalyst.platform.client;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for Client entity.
 */
public class ClientRowMapper implements RowMapper<Client> {

    @Override
    public Client map(ResultSet rs, StatementContext ctx) throws SQLException {
        Client client = new Client();
        client.id = rs.getString("id");
        client.name = rs.getString("name");
        client.identifier = rs.getString("identifier");

        String statusStr = rs.getString("status");
        client.status = statusStr != null ? ClientStatus.valueOf(statusStr) : ClientStatus.ACTIVE;

        client.statusReason = rs.getString("status_reason");

        Timestamp statusChangedAt = rs.getTimestamp("status_changed_at");
        client.statusChangedAt = statusChangedAt != null ? statusChangedAt.toInstant() : null;

        // Parse JSONB notes array
        String notesJson = rs.getString("notes");
        client.notes = JsonHelper.fromJsonList(notesJson, ClientNote.class);

        Timestamp createdAt = rs.getTimestamp("created_at");
        client.createdAt = createdAt != null ? createdAt.toInstant() : null;

        Timestamp updatedAt = rs.getTimestamp("updated_at");
        client.updatedAt = updatedAt != null ? updatedAt.toInstant() : null;

        return client;
    }
}
