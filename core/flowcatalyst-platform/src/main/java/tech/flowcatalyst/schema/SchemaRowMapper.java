package tech.flowcatalyst.schema;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.eventtype.SchemaType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for Schema entity.
 */
public class SchemaRowMapper implements RowMapper<Schema> {

    @Override
    public Schema map(ResultSet rs, StatementContext ctx) throws SQLException {
        String schemaTypeStr = rs.getString("schema_type");
        SchemaType schemaType = schemaTypeStr != null ? SchemaType.valueOf(schemaTypeStr) : SchemaType.JSON_SCHEMA;

        Timestamp createdAt = rs.getTimestamp("created_at");
        Timestamp updatedAt = rs.getTimestamp("updated_at");

        return Schema.builder()
            .id(rs.getString("id"))
            .name(rs.getString("name"))
            .description(rs.getString("description"))
            .mimeType(rs.getString("mime_type"))
            .schemaType(schemaType)
            .content(rs.getString("content"))
            .eventTypeId(rs.getString("event_type_id"))
            .version(rs.getString("version"))
            .createdAt(createdAt != null ? createdAt.toInstant() : null)
            .updatedAt(updatedAt != null ? updatedAt.toInstant() : null)
            .build();
    }
}
