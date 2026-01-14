package tech.flowcatalyst.platform.audit;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for AuditLog entity.
 */
public class AuditLogRowMapper implements RowMapper<AuditLog> {

    @Override
    public AuditLog map(ResultSet rs, StatementContext ctx) throws SQLException {
        AuditLog log = new AuditLog();
        log.id = rs.getString("id");
        log.entityType = rs.getString("entity_type");
        log.entityId = rs.getString("entity_id");
        log.operation = rs.getString("operation");
        log.operationJson = rs.getString("operation_json");
        log.principalId = rs.getString("principal_id");

        Timestamp performedAt = rs.getTimestamp("performed_at");
        log.performedAt = performedAt != null ? performedAt.toInstant() : null;

        return log;
    }
}
